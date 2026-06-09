# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — CRM Customer Master Ingestion
# MAGIC **Source:** CRM System | **Format:** CSV | **Frequency:** Daily
# MAGIC **Target:** credit_card_dev.bronze.crm_customer_master
# MAGIC
# MAGIC Auto Loader ingests CSV files from the landing volume into a raw Delta table.
# MAGIC Schema evolution is enabled to handle unexpected source changes.

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
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from datetime import datetime
import uuid
from datetime import datetime

# Init logger
run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "bronze_crm_customer_ingest", run_id)
# Track incremental load watermark
wm = Watermark(spark, CATALOG)
last_ts = wm.get("bronze_crm_customer")
print("Watermark [bronze_crm_customer]: last_processed_ts = " + str(last_ts))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Auto Loader

# COMMAND ----------

source_path = f"{LANDING_VOLUME}/crm/customer_master"
checkpoint_path = f"{CHECKPOINT_BASE}/crm_customer_master"
target_table = BRONZE_CRM_CUSTOMER

reader = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("header", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("badRecordsPath", f"{LANDING_VOLUME}/_bad_records/crm_customer")
    .schema(CRM_CUSTOMER_SCHEMA)
    .load(source_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add Metadata Columns

# COMMAND ----------

task_log = logger.start_task("add_metadata")

bronze_df = reader.withColumns({
    "ingestion_date": F.current_date(),
    "ingestion_batch_id": F.lit(run_id),
    "source_file": F.col("_metadata.file_path"),
    "load_timestamp": F.current_timestamp(),
    "_created_at": F.current_timestamp(),
    "_created_by": F.lit("bronze_ingestion_pipeline"),
})

logger.complete_task("add_metadata", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Bronze Delta Table

# COMMAND ----------

task_log = logger.start_task("write_bronze")

query = (
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .partitionBy("ingestion_date")
    .trigger(availableNow=True)
    .toTable(target_table)
)
query.awaitTermination()

logger.complete_task("write_bronze", task_log, row_count=spark.read.table(target_table).count())
wm.update("bronze_crm_customer", new_ts=datetime.utcnow().isoformat(), run_id=run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Row Count & Summary

# COMMAND ----------

final_count = spark.read.table(target_table).count()
print(f"Table: {target_table}")
print(f"Total rows: {final_count}")
print(f"Run ID: {run_id}")
print(f"Logger summary: {logger.summary()}")
