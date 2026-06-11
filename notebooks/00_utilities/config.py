# Databricks notebook source
# MAGIC %md
# MAGIC # config — Central Configuration

# COMMAND ----------

"""
Central configuration for Credit Card Defaulter Analysis project.
All environment-specific settings are managed here.
"""

# ██  CATALOG AND SCHEMA CONFIGURATION  ██████████████████████████████████████
# Environment detection:
#   1. Task widget (from DAB job base_parameters) for job runs
#   2. TARGET_CATALOG from Spark conf (set via job_clusters spark_conf)
#   3. Hardcoded default for local notebook development
# NOTE: pipeline.target_catalog is NOT readable in serverless Spark Connect
#       (throws AnalysisException), so we avoid spark.conf.get("pipeline.*")

try:
    _TARGET_CATALOG = dbutils.widgets.get("catalog")
except Exception:
    _TARGET_CATALOG = None

if _TARGET_CATALOG is None:
    try:
        _TARGET_CATALOG = spark.conf.get("TARGET_CATALOG")
    except Exception:
        _TARGET_CATALOG = "credit_card_dev"

CATALOG = _TARGET_CATALOG
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
RAW_SCHEMA = "raw"

CONTROL_SCHEMA = "control"
WATERMARK_TABLE = f"{CATALOG}.{CONTROL_SCHEMA}.watermark"
SCHEMA_DRIFT_LOG = f"{CATALOG}.{CONTROL_SCHEMA}.schema_drift_log"

LANDING_VOLUME = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/landing"

# ██  CONNECTION CONFIGURATION  █████████████████████████████████████████████

try:
    HOST = spark.conf.get("pipeline.host") or "dbc-f49d67a0-6a22.cloud.databricks.com"
except Exception:
    HOST = "dbc-f49d67a0-6a22.cloud.databricks.com"

try:
    HTTP_PATH = spark.conf.get("pipeline.http_path") or "/sql/1.0/warehouses/faced73bbff7e9f2"
except Exception:
    HTTP_PATH = "/sql/1.0/warehouses/faced73bbff7e9f2"

try:
    WAREHOUSE_ID = spark.conf.get("pipeline.warehouse_id") or "faced73bbff7e9f2"
except Exception:
    WAREHOUSE_ID = "faced73bbff7e9f2"

# ██  TABLE NAME CONSTANTS  ████████████████████████████████████████████████

# Bronze tables
BRONZE_CRM_CUSTOMER = f"{CATALOG}.{BRONZE_SCHEMA}.crm_customer_master"
BRONZE_CRM_ADDRESS = f"{CATALOG}.{BRONZE_SCHEMA}.crm_customer_address"
BRONZE_CARD_DETAILS = f"{CATALOG}.{BRONZE_SCHEMA}.card_details"
BRONZE_CARD_STATUS = f"{CATALOG}.{BRONZE_SCHEMA}.card_status"
BRONZE_TXN_TRANSACTIONS = f"{CATALOG}.{BRONZE_SCHEMA}.txn_transactions"
BRONZE_BILLING_STATEMENTS = f"{CATALOG}.{BRONZE_SCHEMA}.billing_statements"
BRONZE_BILLING_PAYMENTS = f"{CATALOG}.{BRONZE_SCHEMA}.billing_payments"
BRONZE_COLLECTIONS_DEFAULTS = f"{CATALOG}.{BRONZE_SCHEMA}.collections_defaults"
BRONZE_COLLECTIONS_RECOVERY = f"{CATALOG}.{BRONZE_SCHEMA}.collections_recovery"
BRONZE_REF_COUNTRY = f"{CATALOG}.{BRONZE_SCHEMA}.ref_country"
BRONZE_REF_STATE = f"{CATALOG}.{BRONZE_SCHEMA}.ref_state"
BRONZE_REF_CURRENCY = f"{CATALOG}.{BRONZE_SCHEMA}.ref_currency"
BRONZE_DIM_CALENDAR = f"{CATALOG}.{BRONZE_SCHEMA}.dim_calendar"
BRONZE_DQ_QUARANTINE = f"{CATALOG}.{BRONZE_SCHEMA}.dq_quarantine"
BRONZE_PIPELINE_LOGS = f"{CATALOG}.{BRONZE_SCHEMA}.pipeline_logs"

# Silver tables
SILVER_CUSTOMER_CLEAN = f"{CATALOG}.{SILVER_SCHEMA}.customer_clean"
SILVER_CARD_CLEAN = f"{CATALOG}.{SILVER_SCHEMA}.card_clean"
SILVER_TRANSACTION_CLEAN = f"{CATALOG}.{SILVER_SCHEMA}.transaction_clean"
SILVER_STATEMENT_CLEAN = f"{CATALOG}.{SILVER_SCHEMA}.statement_clean"
SILVER_PAYMENT_CLEAN = f"{CATALOG}.{SILVER_SCHEMA}.payment_clean"
SILVER_DEFAULT_CLEAN = f"{CATALOG}.{SILVER_SCHEMA}.default_clean"
SILVER_RECOVERY_CLEAN = f"{CATALOG}.{SILVER_SCHEMA}.recovery_clean"
SILVER_CUSTOMER_360 = f"{CATALOG}.{SILVER_SCHEMA}.customer_360_view"
SILVER_DQ_SCORES = f"{CATALOG}.{SILVER_SCHEMA}.dq_scores"

# Gold tables
GOLD_DIM_CUSTOMER = f"{CATALOG}.{GOLD_SCHEMA}.dim_customer"
GOLD_DIM_CARD = f"{CATALOG}.{GOLD_SCHEMA}.dim_card"
GOLD_DIM_DATE = f"{CATALOG}.{GOLD_SCHEMA}.dim_date"
GOLD_DIM_GEOGRAPHY = f"{CATALOG}.{GOLD_SCHEMA}.dim_geography"
GOLD_FACT_TRANSACTION = f"{CATALOG}.{GOLD_SCHEMA}.fact_transaction"
GOLD_FACT_STATEMENT = f"{CATALOG}.{GOLD_SCHEMA}.fact_statement"
GOLD_FACT_DEFAULT_ANALYSIS = f"{CATALOG}.{GOLD_SCHEMA}.fact_default_analysis"

# ██  SOURCE FILE PATHS  ████████████████████████████████████████████████████

SOURCE_CRM_CUSTOMER = f"{LANDING_VOLUME}/crm/customer_master"
SOURCE_CRM_ADDRESS = f"{LANDING_VOLUME}/crm/customer_address"
SOURCE_CARD_DETAILS = f"{LANDING_VOLUME}/card/card_details"
SOURCE_CARD_STATUS = f"{LANDING_VOLUME}/card/card_status"
SOURCE_TXN = f"{LANDING_VOLUME}/txn/transactions"
SOURCE_BILLING_STATEMENTS = f"{LANDING_VOLUME}/billing/billing_statements"
SOURCE_BILLING_PAYMENTS = f"{LANDING_VOLUME}/billing/billing_payments"
SOURCE_COLLECTIONS_DEFAULTS = f"{LANDING_VOLUME}/collections/collections_defaults"
SOURCE_COLLECTIONS_RECOVERY = f"{LANDING_VOLUME}/collections/collections_recovery"
SOURCE_REF_COUNTRY = f"{LANDING_VOLUME}/ref/ref_country"
SOURCE_REF_STATE = f"{LANDING_VOLUME}/ref/ref_state"
SOURCE_REF_CURRENCY = f"{LANDING_VOLUME}/ref/ref_currency"
SOURCE_CALENDAR = f"{LANDING_VOLUME}/ref/dim_calendar"

# ██  CHECKPOINT PATHS  ████████████████████████████████████████████████████

CHECKPOINT_BASE = f"{LANDING_VOLUME}/_checkpoints"

# ██  PIPELINE CONFIG  █████████████████████████████████████████████████████

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 60
DQ_THRESHOLD_NULL_PERCENT = 5.0
DQ_THRESHOLD_DUPLICATE_PERCENT = 0.0
QUARANTINE_ENABLED = True
AUDIT_ENABLED = True

# ██  SPARK CONFIG  ████████████████████████████████████████████████████████

SPARK_CONFIG = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.shuffle.partitions": "auto",
}


# ██  HELPERS  █████████████████████████████████████████████████████████████

def upsert_table(spark, df, table_name, pk_cols, partition_cols=None):
    """MERGE upsert if table exists, else CREATE + overwrite (first run)."""
    if spark.catalog.tableExists(table_name):
        df.createOrReplaceTempView("_upsert_src")
        condition = " AND ".join(f"t.{c} = s.{c}" for c in pk_cols)
        spark.sql(
            f"MERGE INTO {table_name} AS t USING _upsert_src AS s ON {condition} "
            f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    else:
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(table_name)
