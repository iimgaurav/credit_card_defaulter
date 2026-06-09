# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Alert Definitions — DQ & Pipeline Monitoring
# MAGIC
# MAGIC Creates two Databricks SQL Alerts via the REST API:
# MAGIC 1. **DQ Score Below Threshold** — fires when any table's DQ score drops below 90%
# MAGIC 2. **Pipeline Failure** — fires when any pipeline task fails in the last 2 hours
# MAGIC
# MAGIC Also creates the alert query views used by both alerts.
# MAGIC
# MAGIC **Run once to register alerts. Re-run to update thresholds.**

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

import requests
import json
import uuid

run_id = str(uuid.uuid4())[:8]
logger = PipelineLogger(spark, "sql_alerts", run_id)

# ── Databricks REST API config ──────────────────────────────────────────────
# These are read from Databricks secrets in production.
# For dev: set via environment or widget.
dbutils.widgets.text("workspace_host", HOST,   "Workspace Host")
dbutils.widgets.text("warehouse_id",   WAREHOUSE_ID, "SQL Warehouse ID")

WORKSPACE_HOST = dbutils.widgets.get("workspace_host").rstrip("/")
WAREHOUSE     = dbutils.widgets.get("warehouse_id")
TOKEN         = dbutils.secrets.get(scope="credit-card", key="databricks-token") \
                if any(s.name == "credit-card" for s in dbutils.secrets.listScopes()) \
                else ""

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

DQ_THRESHOLD    = 90.0   # Alert if DQ score below this
NOTIFICATION_EMAIL = "on-call@bank.com"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create alert queries in SQL Warehouse

# COMMAND ----------

# ── Alert Query 1: DQ Score Below Threshold ─────────────────────────────────

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SILVER_SCHEMA}.alert_dq_below_threshold AS
SELECT
    table_name,
    ROUND(dq_score, 2)  AS dq_score,
    failed_checks,
    total_checks,
    run_date,
    recorded_at,
    CASE
        WHEN dq_score < 80  THEN 'CRITICAL'
        WHEN dq_score < 90  THEN 'WARNING'
        ELSE 'OK'
    END AS severity
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY recorded_at DESC) AS rn
    FROM {SILVER_DQ_SCORES}
) latest
WHERE rn = 1
  AND dq_score < {DQ_THRESHOLD}
ORDER BY dq_score ASC
""")

print(f"✅ View created: {CATALOG}.{SILVER_SCHEMA}.alert_dq_below_threshold")

# ── Alert Query 2: Pipeline Failure Last 2 Hours ────────────────────────────

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{BRONZE_SCHEMA}.alert_pipeline_failures AS
SELECT
    pipeline_name,
    task_name,
    status,
    error_message,
    started_at,
    completed_at,
    duration_secs,
    logged_at
FROM {BRONZE_PIPELINE_LOGS}
WHERE status = 'FAILED'
  AND logged_at >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
ORDER BY logged_at DESC
""")

print(f"✅ View created: {CATALOG}.{BRONZE_SCHEMA}.alert_pipeline_failures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register Databricks SQL Alerts via REST API

# COMMAND ----------

def create_alert(name: str, query_sql: str, condition_op: str,
                 condition_val: str, rearm_secs: int = 3600) -> dict:
    """
    Create a Databricks SQL Alert.
    Returns the created alert JSON or prints the error.
    """
    if not TOKEN:
        print(f"⚠️  No token configured — skipping API call for: {name}")
        print(f"   SQL: {query_sql[:120]}...")
        return {}

    # Step 1: Create a query
    q_resp = requests.post(
        f"{WORKSPACE_HOST}/api/2.0/sql/queries",
        headers=HEADERS,
        json={
            "name": name + " [query]",
            "query": query_sql,
            "data_source_id": WAREHOUSE,
        },
    )
    if q_resp.status_code != 200:
        print(f"❌ Query creation failed: {q_resp.text}")
        return {}
    query_id = q_resp.json()["id"]

    # Step 2: Create alert on that query
    a_resp = requests.post(
        f"{WORKSPACE_HOST}/api/2.0/sql/alerts",
        headers=HEADERS,
        json={
            "name": name,
            "query_id": query_id,
            "options": {
                "column":    "dq_score" if "dq" in name.lower() else "failure_count",
                "op":        condition_op,
                "value":     condition_val,
                "muted":     False,
                "rearm":     rearm_secs,
            },
        },
    )
    if a_resp.status_code != 200:
        print(f"❌ Alert creation failed: {a_resp.text}")
        return {}

    alert = a_resp.json()
    print(f"✅ Alert created: {name} (id={alert.get('id')})")
    return alert


# Alert 1: DQ score below 90
dq_alert = create_alert(
    name         = f"[{CATALOG}] DQ Score Below {int(DQ_THRESHOLD)}%",
    query_sql    = f"""
        SELECT table_name, dq_score, severity, run_date
        FROM {CATALOG}.{SILVER_SCHEMA}.alert_dq_below_threshold
    """,
    condition_op = ">",
    condition_val= "0",       # fires if any row exists (score < threshold)
    rearm_secs   = 3600,      # re-arm after 1 hour
)

# Alert 2: Pipeline failure
fail_alert = create_alert(
    name         = f"[{CATALOG}] Pipeline Task Failed",
    query_sql    = f"""
        SELECT COUNT(*) AS failure_count
        FROM {CATALOG}.{BRONZE_SCHEMA}.alert_pipeline_failures
    """,
    condition_op = ">",
    condition_val= "0",       # fires if any failure in last 2 hours
    rearm_secs   = 1800,      # re-arm after 30 minutes
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify alert views

# COMMAND ----------

task_log = logger.start_task("create_alerts")

print("\n=== DQ ALERTS (current) ===")
dq_df = spark.sql(f"SELECT * FROM {CATALOG}.{SILVER_SCHEMA}.alert_dq_below_threshold")
if dq_df.count() == 0:
    print("✅ All table DQ scores are above threshold — no alerts.")
else:
    dq_df.show(truncate=False)

print("\n=== PIPELINE FAILURES (last 2h) ===")
fail_df = spark.sql(f"SELECT * FROM {CATALOG}.{BRONZE_SCHEMA}.alert_pipeline_failures")
if fail_df.count() == 0:
    print("✅ No pipeline failures in the last 2 hours.")
else:
    fail_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Thresholds Reference
# MAGIC
# MAGIC | Alert | Condition | Re-arm | Severity |
# MAGIC |---|---|---|---|
# MAGIC | DQ Score Below Threshold | `dq_score < 90%` | 1 hour | WARNING at <90%, CRITICAL at <80% |
# MAGIC | Pipeline Task Failed | `failure_count > 0` in last 2h | 30 minutes | CRITICAL |
# MAGIC
# MAGIC To change thresholds, update the `DQ_THRESHOLD` widget and re-run this notebook.

# COMMAND ----------

logger.complete_task("create_alerts", task_log)
