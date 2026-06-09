"""Pipeline logging — writes structured events to bronze.pipeline_logs Delta table."""

from pyspark.sql import functions as F, DataFrame
from datetime import datetime
import traceback
import uuid


class PipelineLogger:
    def __init__(self, spark, pipeline_name, run_id=None, target_table=None, audit_enabled=True):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.run_id = run_id or str(uuid.uuid4())
        self.start_time = datetime.utcnow()
        self.task_logs = []
        self.target_table = target_table or f"{spark.conf.get('pipeline.target_catalog', 'credit_card_dev')}.bronze.pipeline_logs"
        self.audit_enabled = audit_enabled

    def _log(self, task_name, status, row_count=0, message="", duration_sec=0.0,
             started_at=None, completed_at=None):
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
        if self.audit_enabled:
            df = self.spark.createDataFrame([entry])
            df.write.mode("append").format("delta").saveAsTable(self.target_table)

    def start_task(self, task_name):
        now = datetime.utcnow()
        self._log(task_name, "STARTED", started_at=now)
        return now

    def complete_task(self, task_name, start, row_count=0):
        now = datetime.utcnow()
        duration = (now - start).total_seconds()
        self._log(task_name, "COMPLETED", row_count=row_count, duration_sec=duration,
                  started_at=start, completed_at=now)

    def fail_task(self, task_name, start, error):
        now = datetime.utcnow()
        duration = (now - start).total_seconds()
        tb = traceback.format_exc()
        self._log(task_name, "FAILED", message=f"{type(error).__name__}: {str(error)[:200]}",
                  duration_sec=duration, started_at=start, completed_at=now)

    def summary(self):
        statuses = [l["status"] for l in self.task_logs]
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline_name,
            "total_tasks": len(self.task_logs),
            "completed": statuses.count("COMPLETED"),
            "failed": statuses.count("FAILED"),
            "started": statuses.count("STARTED"),
        }

    def get_logs_df(self):
        return self.spark.createDataFrame(self.task_logs)
