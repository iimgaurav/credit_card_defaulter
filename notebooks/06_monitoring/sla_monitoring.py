# Databricks notebook source
# MAGIC %md
# MAGIC # SLA Monitoring
# MAGIC Checks pipeline completion against defined SLA windows.
# MAGIC Detects breaches, near-misses, and logs SLA status to `bronze.sla_log`.
# MAGIC
# MAGIC **SLA Definitions:**
# MAGIC | Pipeline | SLA Window | Frequency |
# MAGIC |---|---|---|
# MAGIC | Bronze Ingestion | Must complete by 03:00 UTC | Daily |
# MAGIC | Silver Transform | Must complete by 04:00 UTC | Daily |
# MAGIC | Gold Build | Must complete by 05:00 UTC | Daily |
# MAGIC | Full Pipeline | Must complete by 05:30 UTC | Daily |
# MAGIC | DQ Monitoring | Must complete by 07:00 UTC | Daily |

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone
import uuid

run_id = str(uuid.uuid4())
now_utc = datetime.now(timezone.utc)
logger = PipelineLogger(spark, "sla_monitoring", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ensure SLA log table exists

# COMMAND ----------

task_log = logger.start_task("check_sla_compliance")

# COMMAND ----------

SLA_LOG_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.sla_log"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SLA_LOG_TABLE} (
    sla_check_id    STRING     COMMENT 'UUID for this SLA check',
    check_date      DATE       COMMENT 'Date being checked',
    pipeline_name   STRING     COMMENT 'Pipeline / job name',
    sla_window_utc  STRING     COMMENT 'SLA deadline (HH:MM UTC)',
    completed_at    TIMESTAMP  COMMENT 'Actual completion time (NULL if not completed)',
    duration_secs   DOUBLE     COMMENT 'Total pipeline duration in seconds',
    sla_status      STRING     COMMENT 'ON_TIME / BREACHED / NOT_RUN / AT_RISK',
    breach_mins     DOUBLE     COMMENT 'Minutes past SLA (negative = early)',
    checked_at      TIMESTAMP  COMMENT 'When this SLA check ran'
)
USING DELTA
PARTITIONED BY (check_date)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define SLA windows

# COMMAND ----------

# SLA: pipeline_name → required completion hour:minute (UTC)
SLA_DEFINITIONS = {
    "bronze_crm_customer_ingest":   "03:00",
    "bronze_card_details_ingest":   "03:00",
    "bronze_txn_transactions_ingest": "03:00",
    "silver_crm_customer":          "04:00",
    "silver_card":                  "04:00",
    "silver_transactions":          "04:30",
    "silver_enrichment":            "04:45",
    "silver_dq_validation":         "04:50",
    "gold_dim_customer":            "05:00",
    "gold_dim_card":                "05:00",
    "gold_fact_transaction":        "05:15",
    "gold_fact_statement":          "05:15",
    "gold_fact_default":            "05:15",
    "gold_validate":                "05:30",
    "silver_billing":               "04:30",
    "silver_collections":           "04:30",
    "gold_dim_date":                "05:00",
    "gold_dim_geography":           "05:00",
    "gold_orchestrator":            "05:30",
    "silver_orchestrator":          "04:55",
    "dq_monitoring":                "07:00",
    "reconciliation":               "07:00",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check each pipeline against its SLA

# COMMAND ----------

check_date = now_utc.date()

# Get latest completed run per pipeline for today
logs = spark.read.table(BRONZE_PIPELINE_LOGS)

today_runs = (
    logs
    .filter(
        (F.to_date(F.col("logged_at")) == F.lit(str(check_date))) &
        (F.col("status").isin("SUCCESS", "FAILED"))
    )
    .groupBy("pipeline_name")
    .agg(
        F.max(F.when(F.col("status") == "SUCCESS", F.col("completed_at"))).alias("last_success"),
        F.min("started_at").alias("first_start"),
        F.sum(F.coalesce("duration_secs", F.lit(0))).alias("total_duration_secs"),
        F.sum((F.col("status") == "FAILED").cast("int")).alias("failures"),
    )
    .toPandas()
)

sla_rows = []
print(f"{'Pipeline':<45} {'SLA':>6} {'Completed':>10} {'Status':>12} {'Δ mins':>8}")
print("=" * 85)

for pipeline, sla_time in SLA_DEFINITIONS.items():
    sla_hour, sla_min = map(int, sla_time.split(":"))
    sla_deadline = now_utc.replace(hour=sla_hour, minute=sla_min, second=0, microsecond=0)

    row = today_runs[today_runs["pipeline_name"] == pipeline]

    if row.empty or row.iloc[0]["last_success"] is None:
        # Not run today
        sla_status = "BREACHED" if now_utc > sla_deadline else "NOT_RUN"
        completed_at = None
        breach_mins = round((now_utc - sla_deadline).total_seconds() / 60, 1) if now_utc > sla_deadline else None
        duration = None
    else:
        completed_at = row.iloc[0]["last_success"]
        duration = float(row.iloc[0]["total_duration_secs"])

        # Convert to UTC-aware if needed
        if completed_at.tzinfo is None:
            completed_at = completed_at.replace(tzinfo=timezone.utc)

        delta_mins = round((completed_at - sla_deadline).total_seconds() / 60, 1)

        if delta_mins <= 0:
            sla_status = "ON_TIME"
        elif delta_mins <= 15:
            sla_status = "AT_RISK"
        else:
            sla_status = "BREACHED"
        breach_mins = delta_mins

    icon = {"ON_TIME": "✅", "AT_RISK": "⚠️ ", "BREACHED": "❌", "NOT_RUN": "⏳"}.get(sla_status, "")
    comp_str = str(completed_at)[:16] if completed_at else "—"
    breach_str = f"{breach_mins:+.1f}" if breach_mins is not None else "—"

    print(f"{pipeline:<45} {sla_time:>6} {comp_str:>16} {icon} {sla_status:<10} {breach_str:>7}")

    sla_rows.append({
        "sla_check_id":   str(uuid.uuid4()),
        "check_date":     str(check_date),
        "pipeline_name":  pipeline,
        "sla_window_utc": sla_time,
        "completed_at":   str(completed_at) if completed_at else None,
        "duration_secs":  float(duration) if duration else None,
        "sla_status":     sla_status,
        "breach_mins":    float(breach_mins) if breach_mins is not None else None,
        "checked_at":     str(now_utc),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write SLA results to log table

# COMMAND ----------

sla_df = spark.createDataFrame(sla_rows)
sla_df.write.mode("append").saveAsTable(SLA_LOG_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: SLA Summary + Alert on Breach

# COMMAND ----------

sla_result = spark.read.table(SLA_LOG_TABLE).filter(
    F.col("check_date") == F.lit(str(check_date))
)

summary = sla_result.groupBy("sla_status").count().toPandas()
breached = int(summary[summary["sla_status"] == "BREACHED"]["count"].sum() if "BREACHED" in summary["sla_status"].values else 0)
at_risk  = int(summary[summary["sla_status"] == "AT_RISK"]["count"].sum()  if "AT_RISK"  in summary["sla_status"].values else 0)
on_time  = int(summary[summary["sla_status"] == "ON_TIME"]["count"].sum()  if "ON_TIME"  in summary["sla_status"].values else 0)

print(f"\n{'='*55}")
print(f"SLA SUMMARY — {check_date}")
print(f"{'='*55}")
print(f"  ✅ On Time   : {on_time}")
print(f"  ⚠️  At Risk   : {at_risk}")
print(f"  ❌ Breached  : {breached}")
print(f"{'='*55}")

if breached > 0:
    print(f"\n⚠️  SLA BREACH DETECTED — {breached} pipeline(s) missed their SLA window.")
    print("Breached pipelines:")
    sla_result.filter("sla_status = 'BREACHED'").select(
        "pipeline_name", "sla_window_utc", "completed_at", "breach_mins"
    ).show(truncate=False)
    # In production: send Slack/email here via HTTP webhook
    # dbutils.notebook.run("send_alert", 60, {"message": f"SLA breach: {breached} pipelines"})
elif at_risk > 0:
    print(f"\n⚠️  {at_risk} pipeline(s) completed within 15 minutes of their SLA window.")

logger.complete_task("check_sla_compliance", task_log, row_count=breached + at_risk + on_time)

print(f"\nRun ID: {run_id}")
