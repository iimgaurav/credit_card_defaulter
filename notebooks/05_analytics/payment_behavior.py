# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics — Payment Behavior
# MAGIC **KPIs:** payment ratio, late payment %, min-due-only %, consecutive late months, delinquency score
# MAGIC **Sources:** `gold.fact_statement` + `gold.dim_card` + `gold.dim_customer`
# MAGIC **Output:** `gold.analytics_payment_behavior` (full refresh)

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F, Window
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "analytics_payment_behavior", run_id)

# COMMAND ----------

task_log = logger.start_task("compute_behavior")

stmt = spark.read.table(GOLD_FACT_STATEMENT)
dim_card = spark.read.table(GOLD_DIM_CARD).filter("is_current = true").select(
    "card_sk", "card_id", "customer_sk", "credit_limit"
)
dim_customer = spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true").select(
    "customer_sk", "customer_id", "full_name", "credit_score"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Per-statement payment classification

# COMMAND ----------

# Payment method classification per statement
stmt_classified = (
    stmt
    .withColumn("payment_ratio", F.coalesce(F.col("payment_ratio"), F.lit(0.0)))
    .withColumn("payment_class",
        F.when(F.col("total_payments") >= F.col("closing_balance"),  F.lit("FULL"))
         .when(
             (F.col("total_payments") > F.col("minimum_due")) &
             (F.col("total_payments") < F.col("closing_balance")),   F.lit("PARTIAL"))
         .when(
             (F.col("total_payments") > 0) &
             (F.col("total_payments") <= F.col("minimum_due")),       F.lit("MIN_ONLY"))
         .when(F.col("total_payments") == 0,                          F.lit("NO_PAYMENT"))
         .otherwise(                                                   F.lit("OTHER"))
    )
    # Late: due flag was set (closing > minimum) AND no full payment made
    .withColumn("is_late",
        F.col("payment_due_flag") & (F.col("payment_class") != "FULL")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consecutive late months (window per card, ordered by statement_date_sk)

# COMMAND ----------

w_card_time = Window.partitionBy("card_sk").orderBy("statement_date_sk")
w_card_agg  = Window.partitionBy("card_sk")

# Streak: assign group number where streak resets on non-late
streak_df = (
    stmt_classified
    .withColumn("late_int", F.col("is_late").cast("int"))
    # Group by non-late breaks to count consecutive lates
    .withColumn("grp",
        F.sum((F.col("is_late") == False).cast("int")).over(w_card_time)
    )
)

consecutive_late = (
    streak_df
    .groupBy("card_sk", "grp")
    .agg(F.sum("late_int").alias("streak_len"), F.max("statement_date_sk").alias("streak_end_sk"))
    .groupBy("card_sk")
    .agg(F.max("streak_len").alias("max_consecutive_late_months"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aggregate per card

# COMMAND ----------

card_payment = (
    stmt_classified
    .groupBy("card_sk")
    .agg(
        F.count("statement_id").alias("total_statements"),
        F.round(F.avg("payment_ratio"), 4).alias("avg_payment_ratio"),
        F.sum(F.col("is_late").cast("int")).alias("late_payment_count"),
        F.round(F.avg(F.col("is_late").cast("int")) * 100, 2).alias("late_payment_pct"),
        F.sum((F.col("payment_class") == "MIN_ONLY").cast("int")).alias("min_only_count"),
        F.round(F.avg((F.col("payment_class") == "MIN_ONLY").cast("int")) * 100, 2).alias("min_only_pct"),
        F.sum((F.col("payment_class") == "FULL").cast("int")).alias("full_payment_count"),
        F.round(F.avg((F.col("payment_class") == "FULL").cast("int")) * 100, 2).alias("full_payment_pct"),
        F.sum((F.col("payment_class") == "NO_PAYMENT").cast("int")).alias("no_payment_count"),
        F.round(F.sum("interest_charged"), 2).alias("total_interest_paid"),
        F.max("statement_date_sk").alias("latest_statement_sk"),
    )
    .join(consecutive_late, on="card_sk", how="left")
    .withColumn("max_consecutive_late_months", F.coalesce("max_consecutive_late_months", F.lit(0)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delinquency score (0-100, higher = worse)

# COMMAND ----------

# Weighted score: late_pct*0.4 + min_only_pct*0.2 + no_payment*0.3 + consecutive*0.1
payment_behavior = (
    card_payment
    .join(dim_card,     on="card_sk",     how="left")
    .join(dim_customer, on="customer_sk", how="left")
    .withColumn("delinquency_score",
        F.round(
            F.least(F.lit(100.0),
                (F.col("late_payment_pct")             * 0.40) +
                (F.col("min_only_pct")                 * 0.20) +
                (F.least(F.col("no_payment_count") * 10.0, F.lit(30.0))) +  # cap at 30
                (F.least(F.col("max_consecutive_late_months") * 5.0, F.lit(10.0)))  # cap at 10
            ), 1
        )
    )
    # Behavior segment
    .withColumn("payment_segment",
        F.when(F.col("delinquency_score") >= 60,  F.lit("HIGH_RISK"))
         .when(F.col("delinquency_score") >= 30,  F.lit("MODERATE_RISK"))
         .when(F.col("late_payment_pct") == 0,    F.lit("EXCELLENT"))
         .otherwise(                               F.lit("LOW_RISK"))
    )
    .withColumn("_created_at", F.current_timestamp())
    .select(
        "card_id", "customer_id", "full_name",
        "total_statements", "avg_payment_ratio",
        "late_payment_count", "late_payment_pct",
        "min_only_count", "min_only_pct",
        "full_payment_count", "full_payment_pct",
        "no_payment_count", "max_consecutive_late_months",
        "total_interest_paid", "delinquency_score", "payment_segment",
        "credit_score", "_created_at",
    )
)

(
    payment_behavior.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.analytics_payment_behavior")
)

row_count = payment_behavior.count()
logger.complete_task("compute_behavior", task_log, row_count=row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

pb = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_payment_behavior")
print("=" * 60)
print("PAYMENT BEHAVIOR — SEGMENT SUMMARY")
print("=" * 60)

pb.groupBy("payment_segment").agg(
    F.count("card_id").alias("cards"),
    F.round(F.avg("late_payment_pct"), 1).alias("avg_late_pct"),
    F.round(F.avg("delinquency_score"), 1).alias("avg_delinquency_score"),
    F.round(F.avg("full_payment_pct"), 1).alias("avg_full_pay_pct"),
).orderBy(F.col("avg_delinquency_score").desc()).show(truncate=False)

print(f"Total cards: {pb.count():,} | Run ID: {run_id}")
