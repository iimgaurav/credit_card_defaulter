"""
Watermark utility for incremental load tracking.
Stores high-water marks in control.watermark Delta table.
"""
from pyspark.sql import functions as F
from datetime import datetime, timezone


class Watermark:
    def __init__(self, spark, catalog="credit_card_dev"):
        self.spark = spark
        self.CATALOG = catalog
        self.WATERMARK_TABLE = f"{catalog}.control.watermark"
        self._ensure_table()

    def _ensure_table(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.CATALOG}.control")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.WATERMARK_TABLE} (
                table_name          STRING   NOT NULL,
                last_processed_ts   TIMESTAMP,
                last_processed_date DATE,
                last_run_id         STRING,
                updated_at          TIMESTAMP
            )
            USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)

    def get(self, table_name, default_ts="1900-01-01 00:00:00"):
        rows = (
            self.spark.read.table(self.WATERMARK_TABLE)
            .filter(F.col("table_name") == table_name)
            .select("last_processed_ts")
            .collect()
        )
        if rows and rows[0]["last_processed_ts"]:
            return str(rows[0]["last_processed_ts"])
        return default_ts

    def get_date(self, table_name, default_date="1900-01-01"):
        rows = (
            self.spark.read.table(self.WATERMARK_TABLE)
            .filter(F.col("table_name") == table_name)
            .select("last_processed_date")
            .collect()
        )
        if rows and rows[0]["last_processed_date"]:
            return str(rows[0]["last_processed_date"])
        return default_date

    def update(self, table_name, new_ts=None, new_date=None, run_id=None):
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
            MERGE INTO {self.WATERMARK_TABLE} t
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
        return self.spark.read.table(self.WATERMARK_TABLE)
