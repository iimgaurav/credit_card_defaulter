# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_customer (SCD Type 2)
# MAGIC **Source:** `silver.customer_clean`
# MAGIC **Target:** `gold.dim_customer`
# MAGIC **Type:** SCD Type 2 — tracks changes to credit_score, income, employment_status, marital_status, address
# MAGIC **SK:** `customer_sk` — new surrogate per version

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "gold_dim_customer", run_id)

SCD2_EXPIRY  = "9999-12-31"
TODAY        = F.current_date()

# Columns that trigger a new SCD2 version when changed
SCD2_COLS = [
    "credit_score", "annual_income", "employment_status",
    "marital_status", "city", "state_code", "country_code",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source + resolve geo_sk

# COMMAND ----------

task_log = logger.start_task("read_source")

silver = spark.read.table(SILVER_CUSTOMER_CLEAN)
geo    = spark.read.table(GOLD_DIM_GEOGRAPHY).select("geo_sk", "country_code", "state_code")

# Join latest geo_sk (country+state match, fallback country-only)
geo_country = geo.filter(F.col("state_code") == "UNKNOWN").select(
    F.col("geo_sk").alias("geo_sk_country"), "country_code"
)

source = (
    silver
    .join(geo.filter(F.col("state_code") != "UNKNOWN"), on=["country_code", "state_code"], how="left")
    .join(geo_country, on="country_code", how="left")
    .withColumn("geo_sk", F.coalesce(F.col("geo_sk"), F.col("geo_sk_country")))
    .drop("geo_sk_country")
    .withColumn("age", F.floor(F.datediff(TODAY, F.col("date_of_birth")) / 365.25).cast("int"))
    # SCD2 hash of tracked columns
    .withColumn("scd_hash", F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SCD2_COLS])))
)

logger.complete_task("read_source", task_log, row_count=source.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create table if not exists (first load = full insert)

# COMMAND ----------

task_log = logger.start_task("scd2_merge")

table_exists = spark.catalog.tableExists(GOLD_DIM_CUSTOMER)

if not table_exists:
    # First load: insert all as current records
    (
        source
        .withColumn("customer_sk",   F.monotonically_increasing_id())
        .withColumn("effective_date", TODAY)
        .withColumn("expiry_date",    F.lit(SCD2_EXPIRY).cast("date"))
        .withColumn("is_current",     F.lit(True))
        .withColumn("_created_at",    F.current_timestamp())
        .withColumn("_modified_at",   F.current_timestamp())
        .select(
            "customer_sk", "customer_id", "full_name", "date_of_birth", "age",
            "gender", "marital_status", "email", "phone_number",
            "employment_status", "annual_income", "credit_score",
            "city", "state_code", "country_code", "geo_sk",
            "effective_date", "expiry_date", "is_current", "scd_hash",
            "_created_at", "_modified_at",
        )
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(GOLD_DIM_CUSTOMER)
    )
else:
    # Incremental SCD2: detect changes vs current records
    dim = DeltaTable.forName(spark, GOLD_DIM_CUSTOMER)
    dim_current = spark.read.table(GOLD_DIM_CUSTOMER).filter(F.col("is_current") == True)

    # Records that changed (hash mismatch) or are new
    changed = (
        source.alias("src")
        .join(dim_current.alias("dim"), on="customer_id", how="left")
        .filter(
            F.col("dim.customer_id").isNull() |            # new customer
            (F.col("src.scd_hash") != F.col("dim.scd_hash"))  # changed
        )
        .select("src.*")
    )

    # Step 2a: expire old current rows for changed customers
    (
        dim.alias("dim")
        .merge(
            changed.select("customer_id", "scd_hash").alias("chg"),
            "dim.customer_id = chg.customer_id AND dim.is_current = true AND dim.scd_hash != chg.scd_hash"
        )
        .whenMatchedUpdate(set={
            "is_current":   F.lit(False),
            "expiry_date":  F.date_sub(TODAY, 1),
            "_modified_at": F.current_timestamp(),
        })
        .execute()
    )

    # Step 2b: insert new versions
    (
        changed
        .withColumn("customer_sk",   F.monotonically_increasing_id())
        .withColumn("effective_date", TODAY)
        .withColumn("expiry_date",    F.lit(SCD2_EXPIRY).cast("date"))
        .withColumn("is_current",     F.lit(True))
        .withColumn("_created_at",    F.current_timestamp())
        .withColumn("_modified_at",   F.current_timestamp())
        .select(
            "customer_sk", "customer_id", "full_name", "date_of_birth", "age",
            "gender", "marital_status", "email", "phone_number",
            "employment_status", "annual_income", "credit_score",
            "city", "state_code", "country_code", "geo_sk",
            "effective_date", "expiry_date", "is_current", "scd_hash",
            "_created_at", "_modified_at",
        )
        .write.format("delta").mode("append").saveAsTable(GOLD_DIM_CUSTOMER)
    )

cnt = spark.read.table(GOLD_DIM_CUSTOMER).count()
current_cnt = spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true").count()
logger.complete_task("scd2_merge", task_log, row_count=cnt)
print(f"dim_customer: {cnt:,} total rows | {current_cnt:,} current | Run ID: {run_id}")
