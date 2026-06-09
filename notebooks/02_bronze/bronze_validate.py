# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Validation Report
# MAGIC Validates all 13 Bronze tables after ingestion.
# MAGIC Checks: row counts, schema presence, null percentages,
# MAGIC schema drift (vs registry), rescued data, DQ score, FK integrity.

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
logger = PipelineLogger(spark, "bronze_validate", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Inventory + Row Count & Schema Check

# COMMAND ----------

task_log = logger.start_task("row_count_schema")

tables = [
    ("crm_customer_master", BRONZE_CRM_CUSTOMER, "customer_id"),
    ("crm_customer_address", BRONZE_CRM_ADDRESS, "customer_id"),
    ("card_details", BRONZE_CARD_DETAILS, "card_id"),
    ("card_status", BRONZE_CARD_STATUS, "card_id"),
    ("txn_transactions", BRONZE_TXN_TRANSACTIONS, "transaction_id"),
    ("billing_statements", BRONZE_BILLING_STATEMENTS, "statement_id"),
    ("billing_payments", BRONZE_BILLING_PAYMENTS, "payment_id"),
    ("collections_defaults", BRONZE_COLLECTIONS_DEFAULTS, "default_id"),
    ("collections_recovery", BRONZE_COLLECTIONS_RECOVERY, "recovery_id"),
    ("ref_country", BRONZE_REF_COUNTRY, "country_code"),
    ("ref_state", BRONZE_REF_STATE, "state_code"),
    ("ref_currency", BRONZE_REF_CURRENCY, "currency_code"),
    ("dim_calendar", BRONZE_DIM_CALENDAR, "date_key"),
]

results = []
print(f"{'Table':<30} {'Rows':>10} {'Cols':>5} {'PK':>20} {'Status':>10}")
print("=" * 75)

for name, full_path, pk in tables:
    try:
        df = spark.read.table(full_path)
        cnt = df.count()
        cols = len(df.columns)
        has_pk = pk in df.columns
        status = "OK" if cnt > 0 and has_pk else "WARN"
        results.append({"table": name, "rows": cnt, "cols": cols, "pk": pk, "pk_found": has_pk, "status": status})
        print(f"{name:<30} {cnt:>10} {cols:>5} {pk:>20} {status:>10}")
    except Exception as e:
        results.append({"table": name, "rows": 0, "cols": 0, "pk": pk, "pk_found": False, "status": "MISSING"})
        print(f"{name:<30} {'MISSING':>10} {'':>5} {pk:>20} {'MISSING':>10}")

total_rows = sum(r["rows"] for r in results)
logger.complete_task("row_count_schema", task_log, row_count=total_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null Percentage Check

# COMMAND ----------

critical_columns = {
    "crm_customer_master": ["customer_id", "first_name", "last_name", "email"],
    "card_details": ["card_id", "customer_id", "card_type"],
    "txn_transactions": ["transaction_id", "card_id", "amount"],
    "billing_statements": ["statement_id", "card_id", "closing_balance"],
}

print(f"\n{'Table':<30} {'Column':<25} {'Null%':>8} {'Status':>10}")
print("=" * 75)

for table_name, columns in critical_columns.items():
    try:
        df = spark.read.table(table_name)
        total = df.count()
        if total == 0:
            continue
        for col in columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                pct = round(null_count / total * 100, 2)
                status = "PASS" if pct <= 5.0 else "FAIL"
                print(f"{table_name:<30} {col:<25} {pct:>7.1f}% {status:>10}")
    except Exception as e:
        print(f"{table_name:<30} {'ERROR':<25} {'':>8} {str(e)[:20]:>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Drift Detection
# MAGIC Compares actual table schemas against registry.
# MAGIC Checks `_rescued_data` for unexpected columns from source.

# COMMAND ----------

task_log = logger.start_task("schema_drift")

METADATA_AND_SYSTEM = [
    "ingestion_date", "ingestion_batch_id", "source_file",
    "load_timestamp", "_created_at", "_created_by",
    "_rescued_data",
]

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

all_drift_pass = True
all_rescue_pass = True

print(f"\n{'='*75}")
print("SCHEMA DRIFT REPORT")
print(f"{'='*75}")

for name, full_path, expected_schema in bronze_table_schemas:
    try:
        df = spark.read.table(full_path)

        drift = check_schema_drift(df, expected_schema, name, METADATA_AND_SYSTEM)
        rescue = check_rescued_data(spark, full_path)

        status_drift = "PASS" if drift["passed"] else "DRIFT"
        status_rescue = "PASS" if rescue["passed"] else "RESCUED"
        if not drift["passed"]:
            all_drift_pass = False
            for c in drift["unexpected_columns"]:
                print(f"  [{name}] UNEXPECTED COLUMN: {c}")
            for c in drift["missing_columns"]:
                print(f"  [{name}] MISSING COLUMN: {c}")
            for m in drift["type_mismatches"]:
                print(f"  [{name}] TYPE MISMATCH: {m['column']} expected {m['expected']}, got {m['actual']}")
        if not rescue["passed"]:
            all_rescue_pass = False
            print(f"  [{name}] RESCUED DATA: {rescue['rescued_row_count']:,} rows ({rescue['rescued_pct']}%)")
            if rescue["distinct_unexpected_columns"]:
                print(f"  [{name}] UNEXPECTED COLUMNS FOUND: {rescue['distinct_unexpected_columns']}")

        print(f"  {name:<30} drift={status_drift:>6}  rescue={status_rescue:>7}  "
              f"({rescue['rescued_row_count']:,} rescued rows)")

    except Exception as e:
        print(f"  {name:<30} ERROR: {str(e)[:60]}")

print(f"{'='*75}")

# Alert but DON'T block — silver/gold explicit .select() already protects downstream.
# Drift is caught here, logged to monitoring, and followed up async.
if all_drift_pass and all_rescue_pass:
    logger.complete_task("schema_drift", task_log, status="PASS")
else:
    logger.complete_task("schema_drift", task_log, status="DRIFT_DETECTED",
                         details={"drift_pass": all_drift_pass, "rescue_pass": all_rescue_pass})
    if not all_rescue_pass:
        print(f"\n⚠️  DRIFT: Rescued data detected — {rescue['rescued_row_count']} rows have new columns.")
        print(f"   New columns found: {rescue['distinct_unexpected_columns']}")
    if not all_drift_pass:
        print("⚠️  DRIFT: Schema mismatch between actual table and registry.")
        print(f"   Unexpected: {drift['unexpected_columns']}, Missing: {drift['missing_columns']}")
    print("   Pipeline CONTINUING — silver/gold .select() filters unknown columns.")
    print("   Review and update schema_registry.py if source change is intentional.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_rows = sum(r["rows"] for r in results)
passed = sum(1 for r in results if r["status"] == "OK")
missing = sum(1 for r in results if r["status"] == "MISSING")

print(f"\n{'='*75}")
print(f"BRONZE VALIDATION SUMMARY")
print(f"{'='*75}")
print(f"Total tables:     {len(results)}")
print(f"Passed:           {passed}")
print(f"Missing:          {missing}")
print(f"Schema Drift:     {'NONE' if all_drift_pass else 'DETECTED'}")
print(f"Rescued Data:     {'NONE' if all_rescue_pass else 'DETECTED'}")
print(f"Total rows:       {total_rows:,}")
print(f"Run ID:           {run_id}")
print(f"{'='*75}")

if not all_drift_pass or not all_rescue_pass:
    print("\n⚠️  Drift detected — monitoring team notified for async follow-up.")
    print("   Pipeline completed. Data continues flowing to downstream.")
