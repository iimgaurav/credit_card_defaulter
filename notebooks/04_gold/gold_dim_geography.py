# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_geography
# MAGIC **Sources:** `bronze.ref_country` + `bronze.ref_state` + `bronze.ref_currency`
# MAGIC **Target:** `gold.dim_geography`
# MAGIC **Type:** Type 1 (full refresh)
# MAGIC **Grain:** 1 row per state+country combination (also 1 row per country-only when no state)

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "gold_dim_geography", run_id)

# COMMAND ----------

task_log = logger.start_task("build_dim_geography")

country  = spark.read.table(BRONZE_REF_COUNTRY).drop("currency_code")
state    = spark.read.table(BRONZE_REF_STATE).withColumnRenamed("region", "state_region")
currency = spark.read.table(BRONZE_REF_CURRENCY).select("country_code", "currency_code")

# State-level rows: one row per state+country
state_rows = (
    state
    .join(country.select("country_code", "country_name", "region"), on="country_code", how="left")
    .join(currency, on="country_code", how="left")
    .select(
        F.col("country_code"),
        F.col("country_name"),
        F.col("region").alias("country_region"),
        F.col("state_code"),
        F.col("state_name"),
        F.coalesce(F.col("state_region"), F.col("region")).alias("region"),
        F.col("currency_code"),
    )
)

# Country-level fallback rows (for transactions with country but no state match)
country_rows = (
    country
    .join(currency, on="country_code", how="left")
    .withColumn("state_code", F.lit("UNKNOWN"))
    .withColumn("state_name", F.lit("Unknown"))
    .withColumn("region",     F.col("region"))
    .select(
        F.col("country_code"),
        F.col("country_name"),
        F.col("region").alias("country_region"),
        F.col("state_code"),
        F.col("state_name"),
        F.col("region"),
        F.col("currency_code"),
    )
)

# Unknown sentinel row
unknown_row = spark.createDataFrame([{
    "country_code": "UNKNOWN", "country_name": "Unknown",
    "country_region": "Unknown", "state_code": "UNKNOWN",
    "state_name": "Unknown", "region": "Unknown", "currency_code": "UNKNOWN",
}])

dim_geography = (
    state_rows.unionByName(country_rows).unionByName(unknown_row)
    .dropDuplicates(["country_code", "state_code"])
    .withColumn("geo_sk", F.monotonically_increasing_id())
    .withColumn("_created_at", F.current_timestamp())
    .select(
        "geo_sk", "country_code", "country_name", "country_region",
        "state_code", "state_name", "region", "currency_code",
        "_created_at",
    )
)

(
    upsert_table(spark, dim_geography, GOLD_DIM_GEOGRAPHY, ["country_code", "state_code"])
)

cnt = spark.read.table(GOLD_DIM_GEOGRAPHY).count()
logger.complete_task("build_dim_geography", task_log, row_count=cnt)
print(f"dim_geography: {cnt:,} rows | Run ID: {run_id}")
