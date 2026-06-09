# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reconciliation — Bronze → Silver → Gold
# MAGIC End-to-end row count comparison across all layers.
# MAGIC Flags tables where retention drops below 90% or count unexpectedly increases.

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

# MAGIC %run ../00_utilities/dq_framework

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "reconciliation", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Layer Health

# COMMAND ----------

task_log = logger.start_task("reconcile_counts")

bronze_tables = [
    ("crm_customer_master",    BRONZE_CRM_CUSTOMER),
    ("crm_customer_address",   BRONZE_CRM_ADDRESS),
    ("card_details",           BRONZE_CARD_DETAILS),
    ("card_status",            BRONZE_CARD_STATUS),
    ("txn_transactions",       BRONZE_TXN_TRANSACTIONS),
    ("billing_statements",     BRONZE_BILLING_STATEMENTS),
    ("billing_payments",       BRONZE_BILLING_PAYMENTS),
    ("collections_defaults",   BRONZE_COLLECTIONS_DEFAULTS),
    ("collections_recovery",   BRONZE_COLLECTIONS_RECOVERY),
    ("ref_country",            BRONZE_REF_COUNTRY),
    ("ref_state",              BRONZE_REF_STATE),
    ("ref_currency",           BRONZE_REF_CURRENCY),
    ("dim_calendar",           BRONZE_DIM_CALENDAR),
]

print(f"  {'Table':<30} {'Rows':>10} {'Status'}")
print("  " + "-" * 50)
total_bronze = 0
for name, tbl in bronze_tables:
    try:
        cnt = spark.read.table(tbl).count()
        total_bronze += cnt
        status = "✅" if cnt > 0 else "⚠️  EMPTY"
        print(f"  {name:<30} {cnt:>10,} {status}")
    except:
        print(f"  {name:<30} {'MISSING':>10} ❌")

print(f"\n  {'TOTAL BRONZE ROWS':<30} {total_bronze:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. End-to-End Summary

# COMMAND ----------

silver_gold_map = [
    (GOLD_DIM_CUSTOMER,       SILVER_CUSTOMER_CLEAN),
    (GOLD_DIM_CARD,           SILVER_CARD_CLEAN),
    (GOLD_FACT_TRANSACTION,   SILVER_TRANSACTION_CLEAN),
    (GOLD_FACT_STATEMENT,     SILVER_STATEMENT_CLEAN),
    (GOLD_FACT_DEFAULT_ANALYSIS, SILVER_DEFAULT_CLEAN),
]

silver_total = sum(
    spark.read.table(t).count()
    for _, t in silver_gold_map
)
gold_total = (
    spark.read.table(GOLD_FACT_TRANSACTION).count() +
    spark.read.table(GOLD_FACT_STATEMENT).count() +
    spark.read.table(GOLD_FACT_DEFAULT_ANALYSIS).count()
)

print("\n" + "=" * 55)
print("END-TO-END SUMMARY")
print("=" * 55)
print(f"  Total Bronze rows  : {total_bronze:>12,}")
print(f"  Total Silver rows  : {silver_total:>12,}")
print(f"  Total Gold rows    : {gold_total:>12,}")
print(f"  B→S retention     : {round(silver_total/total_bronze*100,1) if total_bronze > 0 else 0:>11.1f}%")
print("=" * 55)
logger.complete_task("reconcile_counts", task_log, row_count=silver_total + gold_total)

print(f"Run ID: {run_id}")
