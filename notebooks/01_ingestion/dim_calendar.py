# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Calendar Ingestion
# MAGIC **Source:** Reference Data | **Format:** CSV | **Target:** credit_card_dev.bronze.dim_calendar
# MAGIC
# MAGIC Calendar uses full refresh (overwrite) since it's a static dimension.

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

# MAGIC %run ../00_utilities/schema_registry

# COMMAND ----------

# MAGIC %run ../00_utilities/watermark

# COMMAND ----------

from pyspark.sql import functions as F
import uuid
from datetime import datetime

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "bronze_calendar_ingest", run_id)
# Track incremental load watermark
wm = Watermark(spark, CATALOG)
last_ts = wm.get("bronze_dim_calendar")
print("Watermark [bronze_dim_calendar]: last_processed_ts = " + str(last_ts))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Calendar CSV

# COMMAND ----------

task_log = logger.start_task("read_source")

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(CALENDAR_SCHEMA)
    .load(SOURCE_CALENDAR)
)

source_count = df.count()
logger.complete_task("read_source", task_log, row_count=source_count)
print(f"Read {source_count} calendar records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add Metadata Columns

# COMMAND ----------

task_log = logger.start_task("add_metadata")

bronze_df = df.withColumns({
    "ingestion_date": F.current_date(),
    "ingestion_batch_id": F.lit(run_id),
    "source_file": F.lit(SOURCE_CALENDAR),
    "load_timestamp": F.current_timestamp(),
    "_created_at": F.current_timestamp(),
    "_created_by": F.lit("bronze_ingestion_pipeline"),
})

logger.complete_task("add_metadata", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Full Refresh to Bronze Delta Table

# COMMAND ----------

task_log = logger.start_task("write_bronze")

upsert_table(spark, bronze_df, BRONZE_DIM_CALENDAR, pk_cols=["date"], partition_cols=["ingestion_date"])

final_count = spark.read.table(BRONZE_DIM_CALENDAR).count()
logger.complete_task("write_bronze", task_log, row_count=final_count)
wm.update("bronze_dim_calendar", new_ts=datetime.utcnow().isoformat(), run_id=run_id)

# COMMAND ----------

print(f"Table: {BRONZE_DIM_CALENDAR}")
print(f"Source rows: {source_count}")
print(f"Target rows: {final_count}")
print(f"Run ID: {run_id}")
