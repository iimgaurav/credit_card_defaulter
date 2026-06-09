# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics — Customer Segmentation
# MAGIC **Model:** RFM (Recency, Frequency, Monetary) × Risk Tier
# MAGIC **Output:** `gold.analytics_customer_segments`
# MAGIC
# MAGIC **RFM Scoring (1-5 each):**
# MAGIC - R: Days since last transaction (lower = better)
# MAGIC - F: Transaction count (higher = better)
# MAGIC - M: Total spend (higher = better)
# MAGIC
# MAGIC **Segments:** Champions, Loyal, At-Risk, Hibernating, Top Spenders, Default-Prone

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F, Window
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "analytics_customer_segments", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build RFM base from fact_transaction

# COMMAND ----------

task_log = logger.start_task("compute_segments")

fact_txn = spark.read.table(GOLD_FACT_TRANSACTION).filter("transaction_type = 'PURCHASE'")
dim_customer = spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true").select(
    "customer_sk", "customer_id", "full_name", "credit_score",
    "annual_income", "city", "state_code", "loyalty_tier" if "loyalty_tier" in [f.name for f in spark.read.table(GOLD_DIM_CUSTOMER).schema.fields] else F.lit(None).alias("loyalty_tier")
)
dim_date = spark.read.table(GOLD_DIM_DATE).select("date_sk", "full_date")
risk_scores = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_risk_scores").select(
    "customer_id", "risk_score", "risk_tier", "total_defaults"
)

# Snapshot date = max transaction date in dataset
snapshot_date = fact_txn.agg(F.max(F.to_date("transaction_datetime"))).collect()[0][0]
snapshot_date_lit = F.to_date(F.lit(str(snapshot_date)))

rfm_raw = (
    fact_txn
    .groupBy("customer_sk")
    .agg(
        F.datediff(snapshot_date_lit, F.max(F.to_date("transaction_datetime"))).alias("recency_days"),
        F.count("transaction_id").alias("frequency"),
        F.round(F.sum("amount"), 2).alias("monetary"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. RFM quintile scoring (1-5 using percentile buckets)

# COMMAND ----------

# Compute percentile thresholds
def percentile_thresholds(df, col):
    return df.approxQuantile(col, [0.2, 0.4, 0.6, 0.8], 0.01)

r_thresholds = percentile_thresholds(rfm_raw, "recency_days")
f_thresholds = percentile_thresholds(rfm_raw, "frequency")
m_thresholds = percentile_thresholds(rfm_raw, "monetary")

# R: lower recency = better (score 5)
r_score = (
    F.when(F.col("recency_days") <= r_thresholds[0], F.lit(5))
     .when(F.col("recency_days") <= r_thresholds[1], F.lit(4))
     .when(F.col("recency_days") <= r_thresholds[2], F.lit(3))
     .when(F.col("recency_days") <= r_thresholds[3], F.lit(2))
     .otherwise(F.lit(1))
)
# F: higher frequency = better (score 5)
f_score = (
    F.when(F.col("frequency") >= f_thresholds[3], F.lit(5))
     .when(F.col("frequency") >= f_thresholds[2], F.lit(4))
     .when(F.col("frequency") >= f_thresholds[1], F.lit(3))
     .when(F.col("frequency") >= f_thresholds[0], F.lit(2))
     .otherwise(F.lit(1))
)
# M: higher monetary = better (score 5)
m_score = (
    F.when(F.col("monetary") >= m_thresholds[3], F.lit(5))
     .when(F.col("monetary") >= m_thresholds[2], F.lit(4))
     .when(F.col("monetary") >= m_thresholds[1], F.lit(3))
     .when(F.col("monetary") >= m_thresholds[0], F.lit(2))
     .otherwise(F.lit(1))
)

rfm_scored = (
    rfm_raw
    .withColumn("r_score", r_score)
    .withColumn("f_score", f_score)
    .withColumn("m_score", m_score)
    .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    .withColumn("rfm_label", F.concat(
        F.col("r_score").cast("string"),
        F.col("f_score").cast("string"),
        F.col("m_score").cast("string")
    ))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. RFM Segment assignment

# COMMAND ----------

rfm_segments = (
    rfm_scored
    .withColumn("rfm_segment",
        # Champions: high R, F, M
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
               F.lit("CHAMPIONS"))
        # Loyal customers
        .when((F.col("f_score") >= 4) & (F.col("m_score") >= 3),
               F.lit("LOYAL"))
        # Potential loyalists
        .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3),
               F.lit("POTENTIAL_LOYALIST"))
        # At risk: high F&M historically but gone quiet
        .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3),
               F.lit("AT_RISK"))
        # Hibernating: low recency + low frequency
        .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2),
               F.lit("HIBERNATING"))
        # New customers: high recency, low frequency
        .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2),
               F.lit("NEW_CUSTOMER"))
        # Lost: very low everything
        .when(F.col("rfm_score") <= 5,
               F.lit("LOST"))
        .otherwise(F.lit("AVERAGE"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Join with risk scores → final segmentation

# COMMAND ----------

dim_cust_sk = spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true").select(
    "customer_sk", "customer_id", "full_name", "credit_score",
    "annual_income", "city", "state_code"
)

segments = (
    rfm_segments
    .join(dim_cust_sk,  on="customer_sk", how="left")
    .join(risk_scores,  on="customer_id", how="left")
    .fillna({"risk_score": 0.0, "risk_tier": "UNKNOWN", "total_defaults": 0})
    # Combined segment: RFM + risk overlay
    .withColumn("combined_segment",
        F.when(F.col("rfm_segment").isin("CHAMPIONS", "LOYAL") & (F.col("risk_tier").isin("VERY_HIGH", "HIGH")),
               F.lit("HIGH_VALUE_HIGH_RISK"))
         .when(F.col("rfm_segment").isin("CHAMPIONS", "LOYAL") & ~F.col("risk_tier").isin("VERY_HIGH", "HIGH")),
               F.lit("HIGH_VALUE_LOW_RISK"))
         .when(F.col("rfm_segment") == "AT_RISK",  F.lit("AT_RISK"))
         .when(F.col("risk_tier").isin("VERY_HIGH", "HIGH"), F.lit("DEFAULT_PRONE"))
         .when(F.col("rfm_segment") == "HIBERNATING", F.lit("DORMANT"))
         .otherwise(F.col("rfm_segment"))
    )
    # Top spender flag: top 10% by monetary
    .withColumn("is_top_spender",
        F.col("monetary") >= F.expr(f"percentile_approx(monetary, 0.90) OVER ()")
    )
    .withColumn("_created_at", F.current_timestamp())
    .select(
        "customer_id", "full_name",
        "recency_days", "frequency", "monetary",
        "r_score", "f_score", "m_score", "rfm_score", "rfm_label",
        "rfm_segment", "risk_score", "risk_tier",
        "combined_segment",
        "total_defaults", "credit_score", "annual_income",
        "city", "state_code", "_created_at",
    )
)

# Top spender: window expression workaround (approxQuantile)
monetary_p90 = segments.approxQuantile("monetary", [0.90], 0.01)[0]
segments = segments.withColumn("is_top_spender", F.col("monetary") >= monetary_p90)

(
    segments.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.analytics_customer_segments")
)

row_count = segments.count()
logger.complete_task("compute_segments", task_log, row_count=row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

seg = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_customer_segments")
print("=" * 70)
print("CUSTOMER SEGMENTATION — COMBINED SEGMENT SUMMARY")
print("=" * 70)

seg.groupBy("combined_segment").agg(
    F.count("customer_id").alias("customers"),
    F.round(F.avg("monetary"), 0).alias("avg_spend"),
    F.round(F.avg("risk_score"), 1).alias("avg_risk_score"),
    F.round(F.avg("frequency"), 1).alias("avg_txn_count"),
).orderBy(F.col("customers").desc()).show(truncate=False)

top_10 = seg.orderBy(F.col("monetary").desc()).select(
    "customer_id", "full_name", "monetary", "rfm_segment", "risk_tier"
).limit(10)
print("\nTop 10 Spenders:")
top_10.show(truncate=False)
print(f"Total customers: {seg.count():,} | Top spenders (top 10%): {seg.filter('is_top_spender').count():,} | Run ID: {run_id}")
