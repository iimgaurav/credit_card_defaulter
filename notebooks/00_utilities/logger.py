# Databricks notebook source
# MAGIC %md
# MAGIC # logger — Pipeline Logger

# COMMAND ----------

"""
Custom logging framework for pipeline tracking.
Logs to both console and Delta table for auditability.
"""
from pyspark.sql import functions as F, DataFrame
from datetime import datetime
import traceback
import uuid

# Variables come from %run ../00_utilities/config — no Python import needed


class PipelineLogger:
    """Structured logger that writes pipeline events to a Delta table."""

    def __init__(self, spark, pipeline_name: str, run_id: str = None):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.run_id = run_id or str(uuid.uuid4())
        self.start_time = datetime.utcnow()
        self.task_logs = []

    def _log(self, task_name: str, status: str, row_count: int = 0,
             message: str = "", duration_sec: float = 0.0,
             started_at: datetime = None, completed_at: datetime = None):
        entry = {
            "log_id": str(uuid.uuid4()),
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "task_name": task_name,
            "status": status,
            "row_count": row_count,
            "error_message": message[:500],
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_secs": duration_sec,
            "logged_at": datetime.utcnow(),
        }
        self.task_logs.append(entry)

        if AUDIT_ENABLED:
            df = self.spark.createDataFrame([entry])
            df.write.mode("append").format("delta").saveAsTable(BRONZE_PIPELINE_LOGS)

    def start_task(self, task_name: str) -> datetime:
        now = datetime.utcnow()
        self._log(task_name, "STARTED", started_at=now)
        return now

    def complete_task(self, task_name: str, start: datetime, row_count: int = 0):
        now = datetime.utcnow()
        duration = (now - start).total_seconds()
        self._log(task_name, "COMPLETED", row_count=row_count, duration_sec=duration,
                  started_at=start, completed_at=now)

    def fail_task(self, task_name: str, start: datetime, error: Exception):
        now = datetime.utcnow()
        duration = (now - start).total_seconds()
        tb = traceback.format_exc()
        self._log(task_name, "FAILED", message=f"{type(error).__name__}: {str(error)[:200]}",
                  duration_sec=duration, started_at=start, completed_at=now)

    def summary(self) -> dict:
        statuses = [l["status"] for l in self.task_logs]
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline_name,
            "total_tasks": len(self.task_logs),
            "completed": statuses.count("COMPLETED"),
            "failed": statuses.count("FAILED"),
            "started": statuses.count("STARTED"),
        }

    def get_logs_df(self) -> DataFrame:
        return self.spark.createDataFrame(self.task_logs)
