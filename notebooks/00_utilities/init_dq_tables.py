# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Tables Initializer
# MAGIC Creates all DQ infrastructure tables if they don't exist:
# MAGIC - `bronze.pipeline_logs`
# MAGIC - `bronze.dq_quarantine`
# MAGIC - `silver.dq_quarantine`
# MAGIC - `silver.dq_scores`
# MAGIC
# MAGIC **Idempotent — safe to re-run at any time.**

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

print(f"Initializing DQ tables in catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze.pipeline_logs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_PIPELINE_LOGS} (
    log_id          STRING     COMMENT 'UUID for this log entry',
    run_id          STRING     COMMENT 'Pipeline run UUID',
    pipeline_name   STRING     COMMENT 'Name of the pipeline/notebook',
    task_name       STRING     COMMENT 'Name of the task within the pipeline',
    status          STRING     COMMENT 'STARTED / SUCCESS / FAILED',
    row_count       BIGINT     COMMENT 'Rows processed (nullable)',
    error_message   STRING     COMMENT 'Error details if failed',
    started_at      TIMESTAMP  COMMENT 'Task start time',
    completed_at    TIMESTAMP  COMMENT 'Task completion time',
    duration_secs   DOUBLE     COMMENT 'Elapsed seconds',
    logged_at       TIMESTAMP  COMMENT 'Log write timestamp'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Pipeline execution logs for all ingestion and transform jobs'
""")
print(f"✅ {BRONZE_PIPELINE_LOGS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze.dq_quarantine

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_DQ_QUARANTINE} (
    quarantine_table     STRING     COMMENT 'Source table where bad record originated',
    quarantine_rule      STRING     COMMENT 'DQ rule that rejected this record',
    quarantine_details   STRING     COMMENT 'JSON: check result details',
    quarantine_timestamp STRING     COMMENT 'ISO timestamp when quarantined',
    quarantine_layer     STRING     COMMENT 'bronze or silver'
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Quarantine table for Bronze-layer DQ rejected records'
""")
print(f"✅ {BRONZE_DQ_QUARANTINE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.dq_quarantine

# COMMAND ----------

SILVER_DQ_QUARANTINE = f"{CATALOG}.{SILVER_SCHEMA}.dq_quarantine"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_DQ_QUARANTINE} (
    quarantine_table     STRING     COMMENT 'Source table where bad record originated',
    quarantine_rule      STRING     COMMENT 'DQ rule that rejected this record',
    quarantine_details   STRING     COMMENT 'JSON: check result details',
    quarantine_timestamp STRING     COMMENT 'ISO timestamp when quarantined',
    quarantine_layer     STRING     COMMENT 'bronze or silver'
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Quarantine table for Silver-layer DQ rejected records'
""")
print(f"✅ {SILVER_DQ_QUARANTINE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.dq_scores

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_DQ_SCORES} (
    run_id          STRING     COMMENT 'Pipeline run UUID',
    pipeline_name   STRING     COMMENT 'Pipeline that ran the check',
    table_name      STRING     COMMENT 'Table that was checked',
    dq_score        DOUBLE     COMMENT 'Overall DQ score (0-100)',
    total_checks    INT        COMMENT 'Number of checks run',
    failed_checks   INT        COMMENT 'Number of checks that failed',
    check_details   STRING     COMMENT 'JSON: per-check results',
    run_date        STRING     COMMENT 'Date the run happened (YYYY-MM-DD)',
    recorded_at     TIMESTAMP  COMMENT 'Timestamp when score was recorded'
)
USING DELTA
PARTITIONED BY (run_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'DQ score history for all pipeline runs'
""")
print(f"✅ {SILVER_DQ_SCORES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

tables = [BRONZE_PIPELINE_LOGS, BRONZE_DQ_QUARANTINE, SILVER_DQ_QUARANTINE, SILVER_DQ_SCORES]
print("\nDQ TABLE INVENTORY:")
print(f"{'Table':<60} {'Rows':>8}")
print("-" * 70)
for tbl in tables:
    try:
        cnt = spark.read.table(tbl).count()
        print(f"{tbl:<60} {cnt:>8,}")
    except Exception as e:
        print(f"{tbl:<60} {'ERROR':>8} — {e}")
print("\n✅ DQ infrastructure ready.")
