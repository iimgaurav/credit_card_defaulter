# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Drift Monitor
# MAGIC Audits all bronze tables for schema drift daily.
# MAGIC Persists drift history to control.schema_drift_log.
# MAGIC Alerts on new drift events.

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

# MAGIC %run ../00_utilities/schema_registry

# COMMAND ----------

# MAGIC %run ../00_utilities/dq_framework

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "schema_drift_monitor", run_id)

DRIFT_LOG_TABLE = "credit_card_dev.control.schema_drift_log"
METADATA_AND_SYSTEM = [
    "ingestion_date", "ingestion_batch_id", "source_file",
    "load_timestamp", "_created_at", "_created_by",
    "_rescued_data",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan All Bronze Tables

# COMMAND ----------

task_log = logger.start_task("scan_drift")

bronze_table_schemas = [
    ("crm_customer_master", BRONZE_CRM_CUSTOMER, CRM_CUSTOMER_SCHEMA),
    ("crm_customer_address", BRONZE_CRM_ADDRESS, CRM_ADDRESS_SCHEMA),
    ("card_details", BRONZE_CARD_DETAILS, CARD_DETAILS_SCHEMA),
    ("card_status", BRONZE_CARD_STATUS, CARD_STATUS_SCHEMA),
    ("txn_transactions", BRONZE_TXN_TRANSACTIONS, TXN_TRANSACTIONS_SCHEMA),
    ("billing_statements", BRONZE_BILLING_STATEMENTS, BILLING_STATEMENT_SCHEMA),
    ("billing_payments", BRONZE_BILLING_PAYMENTS, BILLING_PAYMENT_SCHEMA),
    ("collections_defaults", BRONZE_COLLECTIONS_DEFAULTS, COLLECTIONS_DEFAULT_SCHEMA),
    ("collections_recovery", BRONZE_COLLECTIONS_RECOVERY, COLLECTIONS_RECOVERY_SCHEMA),
    ("ref_country", BRONZE_REF_COUNTRY, REF_COUNTRY_SCHEMA),
    ("ref_state", BRONZE_REF_STATE, REF_STATE_SCHEMA),
    ("ref_currency", BRONZE_REF_CURRENCY, REF_CURRENCY_SCHEMA),
    ("dim_calendar", BRONZE_DIM_CALENDAR, CALENDAR_SCHEMA),
]

drift_records = []
new_drifts = 0

for name, full_path, expected_schema in bronze_table_schemas:
    try:
        df = spark.read.table(full_path)
        drift = check_schema_drift(df, expected_schema, name, METADATA_AND_SYSTEM)
        rescue = check_rescued_data(spark, full_path)

        has_drift = not drift["passed"] or not rescue["passed"]

        record = {
            "run_id": run_id,
            "table_name": name,
            "table_path": full_path,
            "has_drift": has_drift,
            "missing_columns": ", ".join(drift["missing_columns"]) or None,
            "unexpected_columns": ", ".join(drift["unexpected_columns"]) or None,
            "type_mismatches": str(drift["type_mismatches"]) if drift["type_mismatches"] else None,
            "rescued_row_count": rescue["rescued_row_count"],
            "rescued_columns": ", ".join(rescue["distinct_unexpected_columns"]) or None,
            "total_rows": rescue["total_row_count"],
            "detected_at": F.current_timestamp(),
        }
        drift_records.append(record)

        if has_drift:
            new_drifts += 1
            print(f"⚠️  DRIFT: {name} — rescued={rescue['rescued_row_count']} rows, "
                  f"unexpected={drift['unexpected_columns']}")
        else:
            print(f"✅ OK: {name}")

    except Exception as e:
        print(f"❌ ERROR: {name} — {str(e)[:80]}")
        drift_records.append({
            "run_id": run_id,
            "table_name": name,
            "table_path": full_path,
            "has_drift": True,
            "error": str(e)[:200],
            "detected_at": F.current_timestamp(),
        })

logger.complete_task("scan_drift", task_log, tables_scanned=len(bronze_table_schemas), drifts_found=new_drifts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Drift Log

# COMMAND ----------

if drift_records:
    log_df = spark.createDataFrame(drift_records)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DRIFT_LOG_TABLE} (
            run_id STRING,
            table_name STRING,
            table_path STRING,
            has_drift BOOLEAN,
            missing_columns STRING,
            unexpected_columns STRING,
            type_mismatches STRING,
            rescued_row_count BIGINT,
            rescued_columns STRING,
            total_rows BIGINT,
            error STRING,
            detected_at TIMESTAMP
        )
        USING DELTA
        LOCATION '/Volumes/{CATALOG}/{RAW_SCHEMA}/monitoring/schema_drift_log'
    """)
    log_df.write.mode("append").saveAsTable(DRIFT_LOG_TABLE)
    print(f"Logged {len(drift_records)} records to {DRIFT_LOG_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

print(f"\n{'='*60}")
print("SCHEMA DRIFT MONITOR SUMMARY")
print(f"{'='*60}")
print(f"Tables scanned:  {len(bronze_table_schemas)}")
print(f"Drifts found:    {new_drifts}")
print(f"Run ID:          {run_id}")

if new_drifts > 0:
    print(f"\n⚠️  ACTION REQUIRED: {new_drifts} table(s) have schema drift.")
    print("   Review the drift_log table for details:")
    print(f"   SELECT * FROM {DRIFT_LOG_TABLE} WHERE run_id = '{run_id}'")
    print("   Update schema_registry.py if source changes are intentional.")
    dbutils.notebook.exit(f"DRIFT_FOUND:{new_drifts}")
else:
    print("\n✅ No schema drift detected — all tables match registry.")
    dbutils.notebook.exit("NO_DRIFT")
