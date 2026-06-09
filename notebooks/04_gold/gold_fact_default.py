# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_default_analysis
# MAGIC **Sources:** `silver.default_clean` + `silver.recovery_clean`
# MAGIC **Target:** `gold.fact_default_analysis`
# MAGIC **Grain:** 1 row per default event
# MAGIC **FKs:** customer_sk, card_sk, default_date_sk
# MAGIC **Recovery:** rolled up from recovery_clean (total_amount, count, final status)

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "gold_fact_default", run_id)

# COMMAND ----------

task_log = logger.start_task("build_fact_default")

defaults   = spark.read.table(SILVER_DEFAULT_CLEAN)
recoveries = spark.read.table(SILVER_RECOVERY_CLEAN)

dim_customer = F.broadcast(
    spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true")
    .select(F.col("customer_sk").alias("dim_customer_sk"), "customer_id")
)
dim_card = F.broadcast(
    spark.read.table(GOLD_DIM_CARD).filter("is_current = true")
    .select(F.col("card_sk").alias("dim_card_sk"), "card_id")
)
dim_date = F.broadcast(spark.read.table(GOLD_DIM_DATE).select("date_sk", "full_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Rollup recovery per default_id

# COMMAND ----------

# Priority order for final status: FULL > PARTIAL > PENDING > UNKNOWN
status_priority = (
    F.when(F.col("recovery_status") == "FULL",    F.lit(3))
     .when(F.col("recovery_status") == "PARTIAL", F.lit(2))
     .when(F.col("recovery_status") == "PENDING", F.lit(1))
     .otherwise(F.lit(0))
)

recovery_agg = (
    recoveries
    .withColumn("status_rank", status_priority)
    .groupBy("default_id")
    .agg(
        F.round(F.sum("recovery_amount"), 2).alias("recovery_amount"),
        F.count("recovery_id").alias("recovery_count"),
        F.max("status_rank").alias("max_status_rank"),
        F.max("recovery_date").alias("last_recovery_date"),
    )
    .withColumn("recovery_status",
        F.when(F.col("max_status_rank") == 3, F.lit("FULL"))
         .when(F.col("max_status_rank") == 2, F.lit("PARTIAL"))
         .when(F.col("max_status_rank") == 1, F.lit("PENDING"))
         .otherwise(F.lit("NO_RECOVERY"))
    )
    .drop("max_status_rank")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Join dims + recovery

# COMMAND ----------

fact_raw = (
    defaults
    .join(recovery_agg, on="default_id", how="left")
    .join(dim_customer,  on="customer_id", how="left")
    .join(dim_card,      on="card_id",     how="left")
    .join(
        dim_date.withColumnRenamed("date_sk", "default_date_sk")
                .withColumnRenamed("full_date", "_default_full_date"),
        defaults["default_date"] == F.col("_default_full_date"),
        how="left"
    )
    .withColumn("customer_sk",    F.coalesce(F.col("dim_customer_sk"), F.lit(-1)))
    .withColumn("card_sk",        F.coalesce(F.col("dim_card_sk"),     F.lit(-1)))
    .withColumn("default_date_sk", F.coalesce(
        F.col("default_date_sk"),
        F.date_format("default_date", "yyyyMMdd").cast("int")
    ))
    # Null-safe recovery fields
    .withColumn("recovery_amount",  F.coalesce(F.col("recovery_amount"),  F.lit(0.0)))
    .withColumn("recovery_count",   F.coalesce(F.col("recovery_count"),   F.lit(0)))
    .withColumn("recovery_status",  F.coalesce(F.col("recovery_status"),  F.lit("NO_RECOVERY")))
    # Recovery rate
    .withColumn("recovery_rate_pct",
        F.round(F.col("recovery_amount") / F.nullif(F.col("outstanding_amount"), F.lit(0)) * 100, 2)
    )
    .withColumn("default_sk",   F.monotonically_increasing_id())
    .withColumn("_created_at",  F.current_timestamp())
    .select(
        "default_sk", "default_id",
        "customer_sk", "card_sk", "default_date_sk",
        "days_past_due", "outstanding_amount", "collection_stage",
        "is_repeat_default", "dormancy_period_days",
        "default_sequence", "dpd_trend",
        "recovery_amount", "recovery_count", "recovery_status",
        "recovery_rate_pct", "last_recovery_date",
        "_created_at",
    )
    .dropDuplicates(["default_id"])
)

(
    upsert_table(spark, fact_raw, GOLD_FACT_DEFAULT_ANALYSIS, ["default_id"])
)

cnt = spark.read.table(GOLD_FACT_DEFAULT_ANALYSIS).count()
logger.complete_task("build_fact_default", task_log, row_count=cnt)
print(f"fact_default_analysis: {cnt:,} rows | Run ID: {run_id}")
