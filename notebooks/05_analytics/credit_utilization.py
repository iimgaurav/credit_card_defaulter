# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics — Credit Utilization
# MAGIC **KPIs:** utilization ratio per card, utilization bucket, over-limit flag, portfolio-level summary
# MAGIC **Sources:** `gold.fact_statement` + `gold.dim_card` + `gold.dim_customer`
# MAGIC **Output:** `gold.analytics_credit_utilization` (full refresh)

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F, Window
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "analytics_credit_utilization", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Latest statement per card (most recent closing balance vs credit limit)

# COMMAND ----------

task_log = logger.start_task("compute_utilization")

stmt = spark.read.table(GOLD_FACT_STATEMENT)
dim_card = spark.read.table(GOLD_DIM_CARD).filter("is_current = true").select(
    "card_sk", "card_id", "customer_sk", "credit_limit", "card_type", "card_network", "current_status"
)
dim_customer = spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true").select(
    "customer_sk", "customer_id", "full_name", "credit_score", "city", "state_code"
)

# Latest statement per card
w_latest = Window.partitionBy("card_sk").orderBy(F.col("statement_date_sk").desc())

latest_stmt = (
    stmt
    .withColumn("_rn", F.row_number().over(w_latest))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select("card_sk", "statement_date_sk", "closing_balance", "minimum_due",
            "payment_due_flag", "utilization_ratio", "payment_ratio")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute utilization metrics

# COMMAND ----------

utilization = (
    dim_card
    .join(latest_stmt, on="card_sk", how="left")
    .join(dim_customer, on="customer_sk", how="left")
    # Utilization = closing_balance / credit_limit
    .withColumn("utilization_ratio",
        F.when(F.col("credit_limit") > 0,
            F.round(F.coalesce(F.col("closing_balance"), F.lit(0)) / F.col("credit_limit"), 4)
        ).otherwise(F.lit(None))
    )
    # Over-limit flag
    .withColumn("is_over_limit",
        F.col("closing_balance") > F.col("credit_limit")
    )
    # Utilization bucket (industry standard thresholds)
    .withColumn("utilization_bucket",
        F.when(F.col("utilization_ratio").isNull(),           F.lit("NO_STATEMENT"))
         .when(F.col("is_over_limit"),                        F.lit("OVER_LIMIT"))     # >100%
         .when(F.col("utilization_ratio") >= 0.90,            F.lit("CRITICAL"))       # 90-100%
         .when(F.col("utilization_ratio") >= 0.70,            F.lit("HIGH"))           # 70-90%
         .when(F.col("utilization_ratio") >= 0.30,            F.lit("MODERATE"))       # 30-70%
         .when(F.col("utilization_ratio") > 0,                F.lit("LOW"))            # 0-30%
         .otherwise(                                          F.lit("ZERO"))
    )
    # Risk signal: high utilization + low credit score
    .withColumn("utilization_risk_flag",
        (F.col("utilization_ratio") >= 0.70) & (F.coalesce(F.col("credit_score"), F.lit(850)) < 650)
    )
    .withColumn("_created_at", F.current_timestamp())
    .select(
        "card_id", "customer_id", "full_name",
        "card_type", "card_network", "current_status",
        "credit_limit", "closing_balance",
        "utilization_ratio", "utilization_bucket",
        "is_over_limit", "utilization_risk_flag",
        "credit_score", "city", "state_code",
        "statement_date_sk", "_created_at",
    )
)

(
    utilization.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.analytics_credit_utilization")
)

row_count = utilization.count()
logger.complete_task("compute_utilization", task_log, row_count=row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Portfolio Summary

# COMMAND ----------

summary = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_credit_utilization")
total_cards = summary.count()

print("=" * 60)
print("CREDIT UTILIZATION — PORTFOLIO SUMMARY")
print("=" * 60)

summary.groupBy("utilization_bucket").agg(
    F.count("card_id").alias("cards"),
    F.round(F.avg("utilization_ratio") * 100, 1).alias("avg_util_pct"),
    F.round(F.sum("closing_balance") / 1e6, 2).alias("total_balance_M"),
).orderBy("utilization_bucket").show(truncate=False)

over_limit = summary.filter("is_over_limit = true").count()
risk_flagged = summary.filter("utilization_risk_flag = true").count()
avg_util = summary.filter("utilization_ratio is not null").agg(F.avg("utilization_ratio")).collect()[0][0]

print(f"Total cards      : {total_cards:,}")
print(f"Over-limit cards : {over_limit:,} ({round(over_limit/total_cards*100,1)}%)")
print(f"Risk-flagged     : {risk_flagged:,} ({round(risk_flagged/total_cards*100,1)}%)")
print(f"Portfolio avg util: {round(avg_util*100, 1) if avg_util else 0}%")
print(f"Run ID: {run_id}")
