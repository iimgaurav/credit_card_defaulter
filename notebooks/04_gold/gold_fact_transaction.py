# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_transaction
# MAGIC **Source:** `silver.transaction_clean`
# MAGIC **Target:** `gold.fact_transaction`
# MAGIC **Grain:** 1 row per transaction
# MAGIC **FKs:** customer_sk, card_sk, date_sk (YYYYMMDD), geo_sk (merchant country)

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "gold_fact_transaction", run_id)

# COMMAND ----------

task_log = logger.start_task("build_fact_transaction")

txn = spark.read.table(SILVER_TRANSACTION_CLEAN)

# Dim lookups — broadcast small dims to avoid shuffle
dim_customer = F.broadcast(
    spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true")
    .select(F.col("customer_sk").alias("dim_customer_sk"), "customer_id")
)
dim_card = F.broadcast(
    spark.read.table(GOLD_DIM_CARD).filter("is_current = true")
    .select(F.col("card_sk").alias("dim_card_sk"), "card_id")
)
dim_date = F.broadcast(spark.read.table(GOLD_DIM_DATE).select("date_sk", "full_date"))
dim_geo  = F.broadcast(
    spark.read.table(GOLD_DIM_GEOGRAPHY).select(
        F.col("geo_sk").alias("dim_geo_sk"), "country_code"
    )
)

# For geo: use merchant_country; fall back to UNKNOWN geo_sk
geo_unknown_sk = (
    spark.read.table(GOLD_DIM_GEOGRAPHY)
    .filter((F.col("country_code") == "UNKNOWN") & (F.col("state_code") == "UNKNOWN"))
    .select("geo_sk")
    .collect()[0]["geo_sk"]
)

fact = (
    txn
    # date_sk from transaction date
    .withColumn("txn_date", F.to_date("transaction_datetime"))
    .withColumn("date_sk_raw", F.date_format("txn_date", "yyyyMMdd").cast("int"))
    # Join dims
    .join(dim_customer, on="customer_id", how="left")
    .join(dim_card,     on="card_id",     how="left")
    .join(dim_date,     F.col("date_sk_raw") == dim_date["date_sk"], how="left")
    .join(dim_geo,      txn["merchant_country"] == dim_geo["country_code"], how="left")
    # Coalesce FKs to -1 / unknown sentinel
    .withColumn("customer_sk", F.coalesce(F.col("dim_customer_sk"), F.lit(-1)))
    .withColumn("card_sk",     F.coalesce(F.col("dim_card_sk"),     F.lit(-1)))
    .withColumn("date_sk",     F.coalesce(F.col("date_sk"),         F.col("date_sk_raw")))
    .withColumn("geo_sk",      F.coalesce(F.col("dim_geo_sk"),      F.lit(geo_unknown_sk)))
    # Surrogate key
    .withColumn("txn_sk", F.monotonically_increasing_id())
    .withColumn("_created_at", F.current_timestamp())
    .select(
        "txn_sk", "transaction_id",
        "customer_sk", "card_sk", "date_sk", "geo_sk",
        "transaction_datetime", "amount", "currency_code",
        "merchant_name", "merchant_category_code", "merchant_category_desc",
        "transaction_type", "pos_entry_mode",
        "_created_at",
    )
    .dropDuplicates(["transaction_id"])
)

(
    upsert_table(spark, fact, GOLD_FACT_TRANSACTION, ["transaction_id"])
)

# Enable Liquid Clustering on first write (replaces static partitioning for large fact tables)
spark.sql(f"""
    ALTER TABLE {GOLD_FACT_TRANSACTION}
    CLUSTER BY (customer_sk, date_sk)
""")


cnt = spark.read.table(GOLD_FACT_TRANSACTION).count()
logger.complete_task("build_fact_transaction", task_log, row_count=cnt)
print(f"fact_transaction: {cnt:,} rows | Run ID: {run_id}")
