# Databricks notebook source
# MAGIC %md
# MAGIC # watermark — Incremental Load Tracking

# COMMAND ----------

"""
Watermark utility for incremental load tracking.
Stores high-water marks in control.watermark Delta table.

Usage:
    wm = Watermark(spark)
    last_ts = wm.get("silver_transactions")
    # ... process new records since last_ts ...
    wm.update("silver_transactions", new_max_ts)
"""
from pyspark.sql import functions as F
from datetime import datetime, timezone

# CATALOG comes from %run ../00_utilities/config

WATERMARK_TABLE = f"{CATALOG}.control.watermark"


class Watermark:
    def __init__(self, spark):
        self.spark = spark
        self._ensure_table()

    def _ensure_table(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.control")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
                table_name          STRING   NOT NULL COMMENT 'Target table being tracked',
                last_processed_ts   TIMESTAMP         COMMENT 'High-water mark timestamp',
                last_processed_date DATE              COMMENT 'High-water mark date (for date-keyed loads)',
                last_run_id         STRING            COMMENT 'run_id of last successful pipeline run',
                updated_at          TIMESTAMP         COMMENT 'When this watermark was last updated'
            )
            USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            COMMENT 'Incremental load watermark registry'
        """)

    def get(self, table_name: str, default_ts: str = "1900-01-01 00:00:00") -> str:
        """
        Return last_processed_ts for table_name as a string.
        Returns default_ts if no record exists (first run).
        """
        rows = (
            self.spark.read.table(WATERMARK_TABLE)
            .filter(F.col("table_name") == table_name)
            .select("last_processed_ts")
            .collect()
        )
        if rows and rows[0]["last_processed_ts"]:
            return str(rows[0]["last_processed_ts"])
        return default_ts

    def get_date(self, table_name: str, default_date: str = "1900-01-01") -> str:
        """Return last_processed_date for date-keyed incremental loads."""
        rows = (
            self.spark.read.table(WATERMARK_TABLE)
            .filter(F.col("table_name") == table_name)
            .select("last_processed_date")
            .collect()
        )
        if rows and rows[0]["last_processed_date"]:
            return str(rows[0]["last_processed_date"])
        return default_date

    def update(self, table_name: str, new_ts=None, new_date=None, run_id: str = None):
        """
        Update the watermark for table_name.
        new_ts: timestamp string or datetime
        new_date: date string or date
        """
        now = datetime.now(timezone.utc)
        row = self.spark.createDataFrame([{
            "table_name":          table_name,
            "last_processed_ts":   str(new_ts)   if new_ts   else None,
            "last_processed_date": str(new_date) if new_date else None,
            "last_run_id":         run_id,
            "updated_at":          now,
        }])

        row.createOrReplaceTempView("_wm_update")
        self.spark.sql(f"""
            MERGE INTO {WATERMARK_TABLE} t
            USING _wm_update s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET
                t.last_processed_ts   = s.last_processed_ts,
                t.last_processed_date = s.last_processed_date,
                t.last_run_id         = s.last_run_id,
                t.updated_at          = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)

    def list_all(self):
        """Display all watermark entries."""
        return self.spark.read.table(WATERMARK_TABLE)
