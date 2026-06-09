# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics — Default Risk Scoring
# MAGIC **KPIs:** weighted risk score (0-100), risk probability tier, feature contributions
# MAGIC **Sources:** all Gold analytics tables + Gold dims/facts
# MAGIC **Output:** `gold.analytics_risk_scores` (full refresh)
# MAGIC
# MAGIC **Scoring model (rules-based, weighted):**
# MAGIC | Feature | Weight | Direction |
# MAGIC |---|---|---|
# MAGIC | Credit score bucket | 25% | Low score = high risk |
# MAGIC | Utilization ratio | 20% | High util = high risk |
# MAGIC | Payment delinquency score | 20% | High delinquency = high risk |
# MAGIC | Prior defaults | 20% | Has defaults = high risk |
# MAGIC | Consecutive late months | 10% | More = higher risk |
# MAGIC | Income level | 5% | Low income = higher risk |

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "analytics_risk_scoring", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load feature tables

# COMMAND ----------

task_log = logger.start_task("compute_risk_scores")

dim_customer = spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true").select(
    "customer_sk", "customer_id", "full_name", "credit_score", "annual_income",
    "employment_status", "city", "state_code"
)

util = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_credit_utilization").select(
    "customer_id", "utilization_ratio", "is_over_limit", "utilization_bucket"
)

pay = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_payment_behavior").select(
    "customer_id", "delinquency_score", "late_payment_pct",
    "max_consecutive_late_months", "no_payment_count", "payment_segment"
)

# Default history: aggregate per customer
fact_def = spark.read.table(GOLD_FACT_DEFAULT_ANALYSIS)
dim_cust_sk = dim_customer.select("customer_sk", "customer_id")
default_hist = (
    fact_def.join(dim_cust_sk, on="customer_sk", how="left")
    .groupBy("customer_id")
    .agg(
        F.count("default_id").alias("total_defaults"),
        F.max("days_past_due").alias("max_dpd"),
        F.sum("outstanding_amount").alias("total_outstanding"),
        F.sum(F.col("is_repeat_default").cast("int")).alias("repeat_default_count"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Join all features per customer

# COMMAND ----------

# Aggregate util per customer (worst card utilization)
util_cust = util.groupBy("customer_id").agg(
    F.max("utilization_ratio").alias("max_util_ratio"),
    F.sum(F.col("is_over_limit").cast("int")).alias("over_limit_cards"),
)

# Aggregate payment per customer (worst delinquency)
pay_cust = pay.groupBy("customer_id").agg(
    F.max("delinquency_score").alias("max_delinquency_score"),
    F.max("late_payment_pct").alias("max_late_pct"),
    F.max("max_consecutive_late_months").alias("max_consec_late"),
    F.sum("no_payment_count").alias("total_no_payment",),
)

features = (
    dim_customer
    .join(util_cust,    on="customer_id", how="left")
    .join(pay_cust,     on="customer_id", how="left")
    .join(default_hist, on="customer_id", how="left")
    # Null-fill
    .fillna({
        "max_util_ratio": 0.0, "over_limit_cards": 0,
        "max_delinquency_score": 0.0, "max_late_pct": 0.0,
        "max_consec_late": 0, "total_no_payment": 0,
        "total_defaults": 0, "max_dpd": 0, "total_outstanding": 0.0,
    })
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compute component scores (each 0-100)

# COMMAND ----------

scored = (
    features
    # --- Credit score component (25%) ---
    # 300 = 100 risk, 850 = 0 risk
    .withColumn("score_credit",
        F.when(F.col("credit_score").isNull(), F.lit(50.0))
         .otherwise(F.round((850 - F.col("credit_score")) / 5.5, 1))
    )
    # --- Utilization component (20%) ---
    # 0%=0, 30%=15, 70%=50, 90%=75, >100%=100
    .withColumn("score_utilization",
        F.when(F.col("max_util_ratio") > 1.0,                      F.lit(100.0))
         .when(F.col("max_util_ratio") >= 0.90,                    F.round(F.col("max_util_ratio") * 83.3, 1))
         .otherwise(                                                F.round(F.col("max_util_ratio") * 70.0, 1))
    )
    # --- Delinquency component (20%) ---
    .withColumn("score_delinquency", F.col("max_delinquency_score"))
    # --- Default history component (20%) ---
    # 0 defaults=0, 1=40, 2=70, 3+=100
    .withColumn("score_defaults",
        F.when(F.col("total_defaults") == 0, F.lit(0.0))
         .when(F.col("total_defaults") == 1, F.lit(40.0))
         .when(F.col("total_defaults") == 2, F.lit(70.0))
         .otherwise(                         F.lit(100.0))
    )
    # --- Consecutive late component (10%) ---
    .withColumn("score_consec_late",
        F.least(F.col("max_consec_late") * 16.7, F.lit(100.0))
    )
    # --- Income component (5%) ---
    # Annual income <100K=50, <200K=25, >=200K=0
    .withColumn("score_income",
        F.when(F.col("annual_income").isNull(),        F.lit(25.0))
         .when(F.col("annual_income") < 100000,        F.lit(50.0))
         .when(F.col("annual_income") < 200000,        F.lit(25.0))
         .otherwise(                                   F.lit(0.0))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Weighted composite risk score

# COMMAND ----------

risk_scores = (
    scored
    .withColumn("risk_score",
        F.round(
            (F.col("score_credit")      * 0.25) +
            (F.col("score_utilization") * 0.20) +
            (F.col("score_delinquency") * 0.20) +
            (F.col("score_defaults")    * 0.20) +
            (F.col("score_consec_late") * 0.10) +
            (F.col("score_income")      * 0.05),
            1
        )
    )
    # Risk probability tier
    .withColumn("risk_tier",
        F.when(F.col("risk_score") >= 75, F.lit("VERY_HIGH"))
         .when(F.col("risk_score") >= 55, F.lit("HIGH"))
         .when(F.col("risk_score") >= 35, F.lit("MEDIUM"))
         .when(F.col("risk_score") >= 15, F.lit("LOW"))
         .otherwise(                      F.lit("VERY_LOW"))
    )
    # Primary risk driver (highest contributing component)
    .withColumn("primary_risk_driver",
        F.when(
            (F.col("score_defaults") * 0.20) >= F.greatest(
                F.col("score_credit") * 0.25, F.col("score_utilization") * 0.20,
                F.col("score_delinquency") * 0.20
            ), F.lit("DEFAULT_HISTORY")
        ).when(
            (F.col("score_credit") * 0.25) >= F.greatest(
                F.col("score_utilization") * 0.20, F.col("score_delinquency") * 0.20
            ), F.lit("CREDIT_SCORE")
        ).when(
            (F.col("score_delinquency") * 0.20) >= (F.col("score_utilization") * 0.20),
            F.lit("PAYMENT_DELINQUENCY")
        ).otherwise(F.lit("CREDIT_UTILIZATION"))
    )
    .withColumn("_created_at", F.current_timestamp())
    .select(
        "customer_id", "full_name", "credit_score", "annual_income",
        "max_util_ratio", "max_delinquency_score", "total_defaults", "max_dpd",
        "max_consec_late",
        "score_credit", "score_utilization", "score_delinquency",
        "score_defaults", "score_consec_late", "score_income",
        "risk_score", "risk_tier", "primary_risk_driver",
        "city", "state_code", "_created_at",
    )
)

(
    risk_scores.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.analytics_risk_scores")
)

row_count = risk_scores.count()
logger.complete_task("compute_risk_scores", task_log, row_count=row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

rs = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_risk_scores")
print("=" * 60)
print("DEFAULT RISK SCORING — TIER DISTRIBUTION")
print("=" * 60)

rs.groupBy("risk_tier").agg(
    F.count("customer_id").alias("customers"),
    F.round(F.avg("risk_score"), 1).alias("avg_risk_score"),
    F.round(F.avg("credit_score"), 0).alias("avg_credit_score"),
    F.round(F.avg("total_defaults"), 2).alias("avg_defaults"),
).orderBy(F.col("avg_risk_score").desc()).show(truncate=False)

rs.groupBy("primary_risk_driver").count().orderBy(F.col("count").desc()).show(truncate=False)
print(f"Total customers scored: {rs.count():,} | Run ID: {run_id}")
