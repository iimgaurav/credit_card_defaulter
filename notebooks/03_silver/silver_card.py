# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Card Clean
# MAGIC **Sources:** `bronze.card_details` + `bronze.card_status`
# MAGIC **Target:** `silver.card_clean`
# MAGIC **Transforms:** dedup cards, join latest status, validate limits/dates, standardize types

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
logger = PipelineLogger(spark, "silver_card", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze

# COMMAND ----------

task_log = logger.start_task("read_bronze")

card_raw = spark.read.table(BRONZE_CARD_DETAILS)
status_raw = spark.read.table(BRONZE_CARD_STATUS)

logger.complete_task("read_bronze", task_log, row_count=card_raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Dedup card_details — keep latest per card_id

# COMMAND ----------

task_log = logger.start_task("dedup_cards")

w_card = Window.partitionBy("card_id").orderBy(F.col("load_timestamp").desc())

card_dedup = (
    card_raw
    .filter(F.col("card_id").isNotNull() & F.col("customer_id").isNotNull())
    .withColumn("_rn", F.row_number().over(w_card))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

logger.complete_task("dedup_cards", task_log, row_count=card_dedup.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get latest status per card_id

# COMMAND ----------

task_log = logger.start_task("latest_status")

w_status = Window.partitionBy("card_id").orderBy(F.col("status_date").desc(), F.col("load_timestamp").desc())

status_latest = (
    status_raw
    .filter(F.col("card_id").isNotNull())
    .withColumn("status_date_parsed", F.to_date(F.col("status_date"), "yyyy-MM-dd"))
    .withColumn("_rn", F.row_number().over(w_status))
    .filter(F.col("_rn") == 1)
    .select(
        F.col("card_id"),
        F.col("status_code").alias("current_status"),
        F.col("reason_code").alias("status_reason"),
        F.col("status_date_parsed").alias("status_effective_date"),
    )
)

logger.complete_task("latest_status", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Join cards + status, validate & standardize

# COMMAND ----------

task_log = logger.start_task("join_validate")

CARD_TYPE_MAP = {"CC": "CREDIT", "DC": "DEBIT", "PP": "PREPAID",
                 "CREDIT": "CREDIT", "DEBIT": "DEBIT", "PREPAID": "PREPAID"}
NETWORK_MAP = {"VS": "VISA", "MC": "MASTERCARD", "AX": "AMEX",
               "VISA": "VISA", "MASTERCARD": "MASTERCARD", "AMEX": "AMEX"}

ct_expr = F.create_map([F.lit(k) for pair in CARD_TYPE_MAP.items() for k in pair])
nw_expr = F.create_map([F.lit(k) for pair in NETWORK_MAP.items() for k in pair])

card_joined = card_dedup.join(status_latest, on="card_id", how="left")

card_clean = (
    card_joined
    # Standardize
    .withColumn("card_type", F.coalesce(ct_expr[F.upper(F.col("card_type"))], F.col("card_type")))
    .withColumn("card_network", F.coalesce(nw_expr[F.upper(F.col("card_network"))], F.col("card_network")))
    # Parse dates
    .withColumn("issued_date", F.to_date(F.col("issued_date"), "yyyy-MM-dd"))
    .withColumn("expiry_date", F.to_date(F.col("expiry_date"), "yyyy-MM-dd"))
    # Validate: expiry must be future
    .withColumn("expiry_date", F.when(F.col("expiry_date") > F.current_date(), F.col("expiry_date")).otherwise(F.lit(None)))
    # Validate limits
    .withColumn("credit_limit", F.when(F.col("credit_limit") > 0, F.round(F.col("credit_limit"), 2)).otherwise(F.lit(None)))
    .withColumn("cash_limit", F.when(
        (F.col("cash_limit") > 0) & (F.col("cash_limit") <= F.col("credit_limit")),
        F.round(F.col("cash_limit"), 2)
    ).otherwise(F.round(F.col("credit_limit") * 0.3, 2)))
    # Validate interest rate 0-50%
    .withColumn("interest_rate", F.when(F.col("interest_rate").between(0, 50), F.round(F.col("interest_rate"), 2)).otherwise(F.lit(None)))
    # Default status if missing
    .withColumn("current_status", F.coalesce(F.col("current_status"), F.lit("UNKNOWN")))
    # Surrogate key + audit
    .withColumn("card_sk", F.monotonically_increasing_id())
    .withColumn("_silver_created_at", F.current_timestamp())
    .select(
        "card_sk", "card_id", "customer_id", "card_type", "card_network",
        "issued_date", "expiry_date", "credit_limit", "cash_limit",
        "interest_rate", "current_status", "status_reason", "status_effective_date",
        "_silver_created_at",
    )
)

logger.complete_task("join_validate", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to Silver (MERGE on card_id)

# COMMAND ----------

task_log = logger.start_task("write_silver")
(
    upsert_table(spark, card_clean, SILVER_CARD_CLEAN, ["card_id"])
)
spark.sql(f"ALTER TABLE {SILVER_CARD_CLEAN} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
final_count = spark.read.table(SILVER_CARD_CLEAN).count()
logger.complete_task("write_silver", task_log, row_count=final_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: DQ Checks

# COMMAND ----------

df_silver = spark.read.table(SILVER_CARD_CLEAN)

dq_results = run_dq_suite(df_silver, "card_clean", [
    {"type": "null",      "column": "card_id",      "threshold": 0.0},
    {"type": "null",      "column": "customer_id",  "threshold": 0.0},
    {"type": "null",      "column": "credit_limit", "threshold": 2.0},
    {"type": "duplicate", "pk_columns": ["card_id"]},
    {"type": "range",     "column": "interest_rate", "min": 0, "max": 50},
    {"type": "domain",    "column": "card_type", "accepted_values": ["CREDIT", "DEBIT", "PREPAID"]},
    {"type": "domain",    "column": "current_status", "accepted_values": ["ACTIVE", "BLOCKED", "CLOSED", "SUSPENDED", "UNKNOWN"]},
], spark=spark, pipeline_name="silver_card", run_id=run_id)

print(f"DQ Score: {dq_results['dq_score']}%")
print(f"Final row count: {final_count:,}")
print(f"Run ID: {run_id}")
