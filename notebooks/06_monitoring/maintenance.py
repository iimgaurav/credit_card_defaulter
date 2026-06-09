# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance — OPTIMIZE, ZORDER, VACUUM, Liquid Clustering
# MAGIC Run weekly (Sunday 01:00 UTC) via monitoring_job.
# MAGIC - OPTIMIZE + ZORDER on Silver and Gold tables
# MAGIC - VACUUM (retain 7 days) on all Delta tables
# MAGIC - Enables Liquid Clustering on fact_transaction

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

import uuid
run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "maintenance", run_id)

# COMMAND ----------

from datetime import datetime

# Only run full maintenance on Sundays when called from daily monitoring job
if datetime.now().weekday() != 6:
    print("Not Sunday — skipping OPTIMIZE, VACUUM, Liquid Clustering.")
    print("Maintenance runs weekly on Sundays. Run ID:", run_id)
    dbutils.notebook.exit("Skipped — not Sunday")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. OPTIMIZE + ZORDER

# COMMAND ----------

OPTIMIZE_CONFIG = [
    # (table,                      zorder_cols)
    (SILVER_TRANSACTION_CLEAN,    "customer_id, card_id"),
    (SILVER_STATEMENT_CLEAN,      "card_id"),
    (SILVER_DEFAULT_CLEAN,        "customer_id, card_id"),
    (SILVER_CUSTOMER_CLEAN,       "customer_id"),
    (SILVER_CARD_CLEAN,           "card_id, customer_id"),
    (GOLD_FACT_TRANSACTION,       "customer_sk, date_sk"),
    (GOLD_FACT_STATEMENT,         "customer_sk, card_sk"),
    (GOLD_FACT_DEFAULT_ANALYSIS,  "customer_sk, default_date_sk"),
    (GOLD_DIM_CUSTOMER,           "customer_id"),
    (GOLD_DIM_CARD,               "card_id"),
]

task_log = logger.start_task("optimize_zorder")
errors = []
for table, zcols in OPTIMIZE_CONFIG:
    try:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({zcols})")
        print(f"✅ OPTIMIZE {table.split('.')[-1]} ZORDER BY ({zcols})")
    except Exception as e:
        errors.append((table, str(e)))
        print(f"⚠️  {table.split('.')[-1]}: {e}")

if errors:
    logger.fail_task("optimize_zorder", task_log, Exception(f"{len(errors)} tables failed"))
else:
    logger.complete_task("optimize_zorder", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. VACUUM (retain 7 days)

# COMMAND ----------

ALL_TABLES = [
    BRONZE_CRM_CUSTOMER, BRONZE_CRM_ADDRESS, BRONZE_CARD_DETAILS, BRONZE_CARD_STATUS,
    BRONZE_TXN_TRANSACTIONS, BRONZE_BILLING_STATEMENTS, BRONZE_BILLING_PAYMENTS,
    BRONZE_COLLECTIONS_DEFAULTS, BRONZE_COLLECTIONS_RECOVERY,
    BRONZE_REF_COUNTRY, BRONZE_REF_STATE, BRONZE_REF_CURRENCY, BRONZE_DIM_CALENDAR,
    SILVER_CUSTOMER_CLEAN, SILVER_CARD_CLEAN, SILVER_TRANSACTION_CLEAN,
    SILVER_STATEMENT_CLEAN, SILVER_PAYMENT_CLEAN,
    SILVER_DEFAULT_CLEAN, SILVER_RECOVERY_CLEAN, SILVER_CUSTOMER_360,
    GOLD_DIM_DATE, GOLD_DIM_GEOGRAPHY, GOLD_DIM_CUSTOMER, GOLD_DIM_CARD,
    GOLD_FACT_TRANSACTION, GOLD_FACT_STATEMENT, GOLD_FACT_DEFAULT_ANALYSIS,
]

task_log = logger.start_task("vacuum")
vacuum_errors = []
for table in ALL_TABLES:
    try:
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS")   # 7 days
        print(f"✅ VACUUM {table.split('.')[-1]}")
    except Exception as e:
        vacuum_errors.append((table, str(e)))
        print(f"⚠️  {table.split('.')[-1]}: {e}")

if vacuum_errors:
    logger.fail_task("vacuum", task_log, Exception(f"{len(vacuum_errors)} tables failed"))
else:
    logger.complete_task("vacuum", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Liquid Clustering — enable on fact_transaction if not already set

# COMMAND ----------

task_log = logger.start_task("liquid_clustering")
try:
    # Check if clustering is already set
    props = spark.sql(f"DESCRIBE DETAIL {GOLD_FACT_TRANSACTION}").select("clusteringColumns").collect()[0][0]
    if not props:
        spark.sql(f"""
            ALTER TABLE {GOLD_FACT_TRANSACTION}
            CLUSTER BY (customer_sk, date_sk)
        """)
        print(f"✅ Liquid Clustering enabled on fact_transaction (customer_sk, date_sk)")
    else:
        print(f"ℹ️  Liquid Clustering already set: {props}")
    logger.complete_task("liquid_clustering", task_log)
except Exception as e:
    logger.fail_task("liquid_clustering", task_log, e)
    print(f"⚠️  Liquid clustering: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. _rescued_data Monitoring — Alert on Schema Evolution Issues

# COMMAND ----------

task_log = logger.start_task("rescued_data_check")
try:
    BRONZE_TABLES_WITH_RESCUED = [
        ("crm_customer_master", BRONZE_CRM_CUSTOMER),
        ("crm_customer_address", BRONZE_CRM_ADDRESS),
        ("card_details", BRONZE_CARD_DETAILS),
        ("card_status", BRONZE_CARD_STATUS),
        ("txn_transactions", BRONZE_TXN_TRANSACTIONS),
        ("billing_statements", BRONZE_BILLING_STATEMENTS),
        ("billing_payments", BRONZE_BILLING_PAYMENTS),
    ]
    total_rescued = 0
    for name, table in BRONZE_TABLES_WITH_RESCUED:
        try:
            cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {table} WHERE _rescued_data IS NOT NULL").collect()[0][0]
            if cnt > 0:
                print(f"⚠️  {name}: {cnt:,} rows with _rescued_data")
                total_rescued += cnt
            else:
                print(f"✅ {name}: no rescued data")
        except Exception:
            print(f"ℹ️  {name}: no _rescued_data column")
    if total_rescued > 0:
        print(f"\n⚠️  TOTAL: {total_rescued:,} rescued rows across all bronze tables — investigate schema changes")
    else:
        print("\n✅ No _rescued_data found in any bronze table")
    logger.complete_task("rescued_data_check", task_log)
except Exception as e:
    logger.fail_task("rescued_data_check", task_log, e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Retention Cleanup — Purge Old Data Per Policy

# COMMAND ----------

task_log = logger.start_task("retention_cleanup")
try:
    # Bronze: purge > 90 days
    bronze_tables = [
        BRONZE_CRM_CUSTOMER, BRONZE_CRM_ADDRESS, BRONZE_CARD_DETAILS, BRONZE_CARD_STATUS,
        BRONZE_TXN_TRANSACTIONS, BRONZE_BILLING_STATEMENTS, BRONZE_BILLING_PAYMENTS,
        BRONZE_COLLECTIONS_DEFAULTS, BRONZE_COLLECTIONS_RECOVERY,
        BRONZE_REF_COUNTRY, BRONZE_REF_STATE, BRONZE_REF_CURRENCY, BRONZE_DIM_CALENDAR,
    ]
    for tbl in bronze_tables:
        try:
            spark.sql(f"DELETE FROM {tbl} WHERE ingestion_date < CURRENT_DATE() - INTERVAL '90' DAYS")
        except Exception:
            pass
    # Pipeline logs: purge > 90 days
    spark.sql(f"DELETE FROM {BRONZE_PIPELINE_LOGS} WHERE logged_at < CURRENT_TIMESTAMP() - INTERVAL '90' DAYS")
    # DQ scores: purge > 365 days
    spark.sql(f"DELETE FROM {SILVER_DQ_SCORES} WHERE recorded_at < CURRENT_TIMESTAMP() - INTERVAL '365' DAYS")
    print("✅ Retention cleanup complete")
    logger.complete_task("retention_cleanup", task_log)
except Exception as e:
    logger.fail_task("retention_cleanup", task_log, e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*55}")
print(f"MAINTENANCE COMPLETE")
print(f"  OPTIMIZE errors : {len(errors)}")
print(f"  VACUUM errors   : {len(vacuum_errors)}")
print(f"  Run ID          : {run_id}")
print(f"{'='*55}")
