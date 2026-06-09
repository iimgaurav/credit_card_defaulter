# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Transaction Clean
# MAGIC **Source:** `bronze.txn_transactions` enriched via `silver.card_clean`
# MAGIC **Target:** `silver.transaction_clean`
# MAGIC **Transforms:** filter invalid, cast types, combine datetime, standardize, enrich MCC description, enrich customer_id

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

# MAGIC %run ../00_utilities/dq_framework

# COMMAND ----------

from pyspark.sql import functions as F, Window
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "silver_transactions", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze + Silver Card

# COMMAND ----------

task_log = logger.start_task("read_sources")

txn_raw = spark.read.table(BRONZE_TXN_TRANSACTIONS)
card_silver = spark.read.table(SILVER_CARD_CLEAN).select("card_id", "customer_id", "card_type", "card_network")

logger.complete_task("read_sources", task_log, row_count=txn_raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Filter & Cast

# COMMAND ----------

task_log = logger.start_task("filter_cast")

txn_filtered = (
    txn_raw
    .filter(
        F.col("transaction_id").isNotNull()
        & F.col("card_id").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") != 0)
    )
    # Parse & combine transaction_date + transaction_time into timestamp
    .withColumn("transaction_datetime",
        F.to_timestamp(
            F.concat_ws(" ", F.col("transaction_date"), F.col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    # Filter out future transactions
    .filter(F.col("transaction_datetime") <= F.current_timestamp())
    # Cast amount to absolute value decimal
    .withColumn("amount", F.round(F.abs(F.col("amount").cast("decimal(12,2)")), 2))
    # Standardize string fields
    .withColumn("merchant_name", F.initcap(F.trim(F.col("merchant_name"))))
    .withColumn("merchant_category_code", F.upper(F.trim(F.col("merchant_category"))))
    .withColumn("merchant_country", F.upper(F.trim(F.col("merchant_country"))))
    .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
)

logger.complete_task("filter_cast", task_log, row_count=txn_filtered.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Standardize transaction_type & pos_entry_mode

# COMMAND ----------

task_log = logger.start_task("standardize_codes")

TXN_TYPE_MAP = {
    "PUR": "PURCHASE", "PURCHASE": "PURCHASE",
    "WDR": "WITHDRAWAL", "WITHDRAWAL": "WITHDRAWAL",
    "REF": "REFUND", "REFUND": "REFUND",
}
POS_MAP = {
    "CHIP": "CHIP", "SWP": "SWIPE", "SWIPE": "SWIPE",
    "CTLS": "CONTACTLESS", "CONTACTLESS": "CONTACTLESS",
    "ONL": "ONLINE", "ONLINE": "ONLINE",
}

txn_type_expr = F.create_map([F.lit(k) for pair in TXN_TYPE_MAP.items() for k in pair])
pos_expr = F.create_map([F.lit(k) for pair in POS_MAP.items() for k in pair])

txn_std = (
    txn_filtered
    .withColumn("transaction_type", F.coalesce(txn_type_expr[F.upper(F.col("transaction_type"))], F.lit("OTHER")))
    .withColumn("pos_entry_mode", F.coalesce(pos_expr[F.upper(F.col("pos_entry_mode"))], F.lit("UNKNOWN")))
)

logger.complete_task("standardize_codes", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Enrich MCC description

# COMMAND ----------

task_log = logger.start_task("enrich_mcc")

# Representative MCC categories (top categories for credit card spending)
MCC_MAP = {
    "5411": "Grocery Stores", "5912": "Drug Stores & Pharmacies",
    "5812": "Eating Places & Restaurants", "5541": "Service Stations",
    "5999": "Retail Stores", "4111": "Transportation",
    "7011": "Hotels & Lodging", "4112": "Airlines",
    "5045": "Electronics", "5944": "Jewelry Stores",
    "5621": "Women's Clothing", "5661": "Shoe Stores",
    "7832": "Movie Theaters", "5942": "Book Stores",
    "4816": "Computer Network/Info Services",
}

mcc_expr = F.create_map([F.lit(k) for pair in MCC_MAP.items() for k in pair])

txn_mcc = txn_std.withColumn(
    "merchant_category_desc",
    F.coalesce(mcc_expr[F.col("merchant_category_code")], F.lit("Other"))
)

logger.complete_task("enrich_mcc", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Enrich customer_id from silver.card_clean

# COMMAND ----------

task_log = logger.start_task("enrich_customer")

txn_enriched = txn_mcc.join(card_silver, on="card_id", how="left")

logger.complete_task("enrich_customer", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Final Select + Surrogate Key

# COMMAND ----------

transaction_clean = (
    txn_enriched
    .withColumn("transaction_sk", F.monotonically_increasing_id())
    .withColumn("_silver_created_at", F.current_timestamp())
    .select(
        "transaction_sk", "transaction_id", "card_id", "customer_id",
        "transaction_datetime", "merchant_name", "merchant_category_code",
        "merchant_category_desc", "merchant_country", "amount", "currency_code",
        "transaction_type", "pos_entry_mode", "card_type", "card_network",
        "_silver_created_at",
    )
    # Dedup: keep one row per transaction_id
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Silver (MERGE on transaction_id)

# COMMAND ----------

task_log = logger.start_task("write_silver")
(
    upsert_table(spark, transaction_clean, SILVER_TRANSACTION_CLEAN, ["transaction_id"], partition_cols=["transaction_type"])
)
spark.sql(f"ALTER TABLE {SILVER_TRANSACTION_CLEAN} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
final_count = spark.read.table(SILVER_TRANSACTION_CLEAN).count()
logger.complete_task("write_silver", task_log, row_count=final_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: DQ Checks

# COMMAND ----------

df_silver = spark.read.table(SILVER_TRANSACTION_CLEAN)

dq_results = run_dq_suite(df_silver, "transaction_clean", [
    {"type": "null",      "column": "transaction_id",       "threshold": 0.0},
    {"type": "null",      "column": "card_id",              "threshold": 0.0},
    {"type": "null",      "column": "amount",               "threshold": 0.0},
    {"type": "null",      "column": "transaction_datetime", "threshold": 0.5},
    {"type": "duplicate", "pk_columns": ["transaction_id"]},
    {"type": "range",     "column": "amount", "min": 0.01, "max": 1000000},
    {"type": "domain",    "column": "transaction_type", "accepted_values": ["PURCHASE", "WITHDRAWAL", "REFUND", "OTHER"]},
    {"type": "domain",    "column": "pos_entry_mode",   "accepted_values": ["CHIP", "SWIPE", "CONTACTLESS", "ONLINE", "UNKNOWN"]},
], spark=spark, pipeline_name="silver_transactions", run_id=run_id)

print(f"DQ Score: {dq_results['dq_score']}%")
print(f"Final row count: {final_count:,}")
print(f"Run ID: {run_id}")
