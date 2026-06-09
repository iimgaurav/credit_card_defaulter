# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Collections Defaults Ingestion
# MAGIC **Source:** Collections System | **Format:** Excel | **Target:** credit_card_dev.bronze.collections_defaults
# MAGIC
# MAGIC Excel files cannot use Auto Loader. Reads via pandas → Spark → Delta with MERGE (upsert) on default_id.

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
logger = PipelineLogger(spark, "bronze_collections_defaults_ingest", run_id)
# Track incremental load watermark
wm = Watermark(spark, CATALOG)
last_ts = wm.get("bronze_collections_defaults")
print("Watermark [bronze_collections_defaults]: last_processed_ts = " + str(last_ts))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Excel File

# COMMAND ----------

# MAGIC %pip install openpyxl -q

# COMMAND ----------

import pandas as pd

task_log = logger.start_task("read_excel")

excel_path = f"{LANDING_VOLUME}/collections/collections_defaults/collections_defaults.xlsx"
pdf = pd.read_excel(excel_path, sheet_name="defaults")
spark_df = spark.createDataFrame(pdf, schema=COLLECTIONS_DEFAULT_SCHEMA)
source_count = spark_df.count()

logger.complete_task("read_excel", task_log, row_count=source_count)
print(f"Read {source_count} records from Excel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add Metadata Columns

# COMMAND ----------

task_log = logger.start_task("add_metadata")

bronze_df = spark_df.withColumns({
    "ingestion_date": F.current_date(),
    "ingestion_batch_id": F.lit(run_id),
    "source_file": F.lit(excel_path),
    "load_timestamp": F.current_timestamp(),
    "_created_at": F.current_timestamp(),
    "_created_by": F.lit("bronze_ingestion_pipeline"),
})

logger.complete_task("add_metadata", task_log)
# Filter by watermark - only process records newer than last watermark
if last_ts != "1900-01-01 00:00:00":
    prior = bronze_df.count()
    bronze_df = bronze_df.filter(F.col("default_date") > F.lit(last_ts).cast("timestamp"))
    kept = bronze_df.count()
    print("Watermark filter: removed " + str(prior - kept) + " old records, keeping " + str(kept) + " new records")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: MERGE (Upsert) into Bronze Delta Table

# COMMAND ----------

task_log = logger.start_task("write_bronze")

target_table = BRONZE_COLLECTIONS_DEFAULTS

upsert_table(spark, bronze_df, target_table, pk_cols=["default_id"], partition_cols=["ingestion_date"])

final_count = spark.read.table(target_table).count()
logger.complete_task("write_bronze", task_log, row_count=final_count)
wm.update("bronze_collections_defaults", new_ts=datetime.utcnow().isoformat(), run_id=run_id)

# COMMAND ----------

print(f"Table: {target_table}")
print(f"Source rows: {source_count}")
print(f"Target rows: {final_count}")
print(f"Run ID: {run_id}")
