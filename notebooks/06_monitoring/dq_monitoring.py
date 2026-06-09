# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Monitoring Dashboard
# MAGIC End-to-end DQ health across all pipeline runs.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. DQ Score Trends — per table, per day
# MAGIC 2. Current DQ Status — latest score per table
# MAGIC 3. Quarantine Drill-down — count by table + rule
# MAGIC 4. Pass/Fail Heatmap — table × check_type
# MAGIC 5. Pipeline Log Summary — run durations and failures

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F, Window
import json

SILVER_DQ_QUARANTINE = f"{CATALOG}.{SILVER_SCHEMA}.dq_quarantine"

run_id = str(uuid.uuid4())[:8]
logger = PipelineLogger(spark, "dq_monitoring", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. DQ Score Trends

# COMMAND ----------

task_log = logger.start_task("compute_dq_metrics")

# COMMAND ----------

dq_scores = spark.read.table(SILVER_DQ_SCORES)

print("=" * 75)
print("DQ SCORE TRENDS — LAST 30 DAYS (per table)")
print("=" * 75)

trends = (
    dq_scores
    .filter(F.col("run_date") >= F.date_sub(F.current_date(), 30))
    .groupBy("table_name", "run_date")
    .agg(
        F.round(F.avg("dq_score"), 2).alias("avg_score"),
        F.min("dq_score").alias("min_score"),
        F.count("run_id").alias("run_count"),
        F.sum("failed_checks").alias("total_failed_checks"),
    )
    .orderBy("table_name", "run_date")
)

trends.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Current DQ Status — Latest Score per Table

# COMMAND ----------

w_latest = Window.partitionBy("table_name").orderBy(F.col("recorded_at").desc())

latest_scores = (
    dq_scores
    .withColumn("_rn", F.row_number().over(w_latest))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("health",
        F.when(F.col("dq_score") >= 95, F.lit("🟢 HEALTHY"))
         .when(F.col("dq_score") >= 80, F.lit("🟡 WARNING"))
         .otherwise(                    F.lit("🔴 CRITICAL"))
    )
    .select("table_name", "dq_score", "failed_checks", "total_checks", "run_date", "health")
    .orderBy(F.col("dq_score").asc())
)

print("\n" + "=" * 75)
print("CURRENT DQ STATUS — LATEST RUN PER TABLE")
print("=" * 75)
latest_scores.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quarantine Drill-down

# COMMAND ----------

def show_quarantine(table_name, layer_label):
    try:
        qdf = spark.read.table(table_name)
        total = qdf.count()
        if total == 0:
            print(f"\n{layer_label}: No quarantined records.")
            return

        print(f"\n{layer_label}: {total:,} total quarantined records")

        print("\n  By source table:")
        qdf.groupBy("quarantine_table").count().orderBy(F.col("count").desc()).show(20, truncate=False)

        print("  By rule:")
        qdf.groupBy("quarantine_rule").count().orderBy(F.col("count").desc()).show(20, truncate=False)

        print("  By table + rule:")
        (
            qdf.groupBy("quarantine_table", "quarantine_rule")
            .count().orderBy(F.col("count").desc())
        ).show(30, truncate=False)
    except Exception as e:
        print(f"  {layer_label}: table not found or empty — {e}")

print("=" * 75)
print("QUARANTINE DRILL-DOWN")
print("=" * 75)

show_quarantine(BRONZE_DQ_QUARANTINE, "BRONZE Quarantine")
show_quarantine(SILVER_DQ_QUARANTINE, "SILVER Quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pass/Fail Heatmap — Table × Check Type

# COMMAND ----------

print("\n" + "=" * 75)
print("PASS/FAIL HEATMAP — TABLE × CHECK TYPE")
print("=" * 75)

# Parse check_details JSON to extract per-check pass/fail
from pyspark.sql.types import MapType, StringType

# Explode check_details JSON using from_json into map
check_rows = []
for row in dq_scores.select("table_name", "run_date", "check_details").collect():
    try:
        details = json.loads(row["check_details"])
        for check_name, result in details.items():
            if isinstance(result, dict):
                # Infer check type from prefix
                check_type = check_name.split("_")[0] if "_" in check_name else check_name
                check_rows.append({
                    "table_name":  row["table_name"],
                    "run_date":    row["run_date"],
                    "check_name":  check_name,
                    "check_type":  check_type,
                    "passed":      result.get("passed", True),
                })
    except Exception:
        pass

if check_rows:
    check_df = spark.createDataFrame(check_rows)

    heatmap = (
        check_df
        .groupBy("table_name", "check_type")
        .agg(
            F.sum(F.col("passed").cast("int")).alias("passed"),
            F.sum((~F.col("passed")).cast("int")).alias("failed"),
            F.count("check_name").alias("total"),
        )
        .withColumn("pass_rate_pct", F.round(F.col("passed") / F.col("total") * 100, 1))
        .withColumn("status",
            F.when(F.col("failed") > 0, F.lit("❌ FAIL"))
             .otherwise(               F.lit("✅ PASS"))
        )
        .orderBy("table_name", "check_type")
    )
    heatmap.show(100, truncate=False)
else:
    print("No check details to display yet.")

logger.complete_task("compute_dq_metrics", task_log, row_count=len(check_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pipeline Log Summary

# COMMAND ----------

print("\n" + "=" * 75)
print("PIPELINE LOG SUMMARY — LAST 7 DAYS")
print("=" * 75)

try:
    logs = spark.read.table(BRONZE_PIPELINE_LOGS)

    # Run summary
    run_summary = (
        logs
        .filter(F.col("logged_at") >= F.date_sub(F.current_timestamp(), 7))
        .groupBy("pipeline_name", "status")
        .agg(
            F.count("run_id").alias("runs"),
            F.round(F.avg("duration_secs"), 1).alias("avg_duration_secs"),
            F.round(F.max("duration_secs"), 1).alias("max_duration_secs"),
            F.sum(F.coalesce("row_count", F.lit(0))).alias("total_rows_processed"),
        )
        .orderBy("pipeline_name", "status")
    )
    run_summary.show(truncate=False)

    # Failed runs
    failed = logs.filter(
        (F.col("status") == "FAILED") &
        (F.col("logged_at") >= F.date_sub(F.current_timestamp(), 7))
    ).select("pipeline_name", "task_name", "error_message", "logged_at")

    failed_cnt = failed.count()
    if failed_cnt > 0:
        print(f"\n⚠️  {failed_cnt} FAILED tasks in last 7 days:")
        failed.orderBy(F.col("logged_at").desc()).show(20, truncate=False)
    else:
        print("✅ No failures in last 7 days.")

except Exception as e:
    print(f"Pipeline logs not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Portfolio DQ Score — Overall

# COMMAND ----------

# Latest score per table → portfolio average
portfolio_score = (
    dq_scores
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("table_name").orderBy(F.col("recorded_at").desc())
    ))
    .filter(F.col("_rn") == 1)
    .agg(
        F.round(F.avg("dq_score"), 2).alias("portfolio_dq_score"),
        F.count("table_name").alias("tables_monitored"),
        F.sum(F.when(F.col("dq_score") >= 95, 1).otherwise(0)).alias("healthy_tables"),
        F.sum(F.when(F.col("dq_score") < 80, 1).otherwise(0)).alias("critical_tables"),
    )
    .collect()[0]
)

print("\n" + "=" * 55)
print("PORTFOLIO DQ SCORE")
print("=" * 55)
print(f"  Overall DQ Score  : {portfolio_score['portfolio_dq_score']}%")
print(f"  Tables Monitored  : {portfolio_score['tables_monitored']}")
print(f"  🟢 Healthy (≥95%) : {portfolio_score['healthy_tables']}")
print(f"  🔴 Critical (<80%): {portfolio_score['critical_tables']}")
print("=" * 55)
