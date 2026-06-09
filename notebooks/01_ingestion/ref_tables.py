# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Reference Tables Ingestion
# MAGIC **Source:** Reference Data | **Format:** CSV | **Target:** credit_card_dev.bronze.ref_country, ref_state, ref_currency
# MAGIC
# MAGIC Reference tables use MERGE (upsert) for idempotent loads.

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
logger = PipelineLogger(spark, "bronze_ref_tables_ingest", run_id)
# Track incremental load watermark
wm = Watermark(spark, CATALOG)
last_ts = wm.get("bronze_ref_tables")
print("Watermark [bronze_ref_tables]: last_processed_ts = " + str(last_ts))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: Merge reference table

# COMMAND ----------

def write_reference(source_path, target_table, schema):
    """Read CSV, add metadata, and overwrite into target."""
    task_log = logger.start_task(f"write_{target_table.split('.')[-1]}")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(schema)
        .load(source_path)
    )

    bronze_df = df.withColumns({
        "ingestion_date": F.current_date(),
        "ingestion_batch_id": F.lit(run_id),
        "source_file": F.lit(source_path),
        "load_timestamp": F.current_timestamp(),
        "_created_at": F.current_timestamp(),
        "_created_by": F.lit("bronze_ingestion_pipeline"),
    })

    source_count = bronze_df.count()

    (
        upsert_table(spark, bronze_df, target_table, pk_cols=list(bronze_df.columns))
    )

    final_count = spark.read.table(target_table).count()
    logger.complete_task(f"write_{target_table.split('.')[-1]}", task_log, row_count=final_count)
    return source_count, final_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Reference Tables

# COMMAND ----------

# Country
src, tgt = write_reference(SOURCE_REF_COUNTRY, BRONZE_REF_COUNTRY, REF_COUNTRY_SCHEMA)
print(f"ref_country: {src} source → {tgt} target")

# State
src, tgt = write_reference(SOURCE_REF_STATE, BRONZE_REF_STATE, REF_STATE_SCHEMA)
print(f"ref_state: {src} source → {tgt} target")

# Currency
src, tgt = write_reference(SOURCE_REF_CURRENCY, BRONZE_REF_CURRENCY, REF_CURRENCY_SCHEMA)
print(f"ref_currency: {src} source → {tgt} target")

print(f"\nRun ID: {run_id}")
wm.update("bronze_ref_tables", new_ts=datetime.utcnow().isoformat(), run_id=run_id)
print("All reference tables ingested with upsert.")
