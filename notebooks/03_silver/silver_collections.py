# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Collections Clean
# MAGIC **Sources:** `bronze.collections_defaults` + `bronze.collections_recovery`
# MAGIC **Targets:** `silver.default_clean` + `silver.recovery_clean`
# MAGIC **Transforms:** dedup defaults, validate DPD, window lead/lag for repeat default & dormancy, standardize stages

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

# MAGIC %run ../00_utilities/dq_framework

# COMMAND ----------

from pyspark.sql import functions as F, Window
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "silver_collections", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze

# COMMAND ----------

task_log = logger.start_task("read_bronze")

defaults_raw  = spark.read.table(BRONZE_COLLECTIONS_DEFAULTS)
recovery_raw  = spark.read.table(BRONZE_COLLECTIONS_RECOVERY)

logger.complete_task("read_bronze", task_log, row_count=defaults_raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Clean & Dedup Defaults

# COMMAND ----------

task_log = logger.start_task("clean_dedup_defaults")

STAGE_MAP = {
    "EARLY": "EARLY", "E": "EARLY",
    "MID": "MID",     "M": "MID",
    "LATE": "LATE",   "L": "LATE",
    "LEGAL": "LEGAL", "LGL": "LEGAL",
}
stage_expr = F.create_map([F.lit(k) for pair in STAGE_MAP.items() for k in pair])

defaults_clean = (
    defaults_raw
    .filter(
        F.col("default_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("card_id").isNotNull()
    )
    # Parse dates
    .withColumn("default_date",      F.to_date(F.col("default_date"),      "yyyy-MM-dd"))
    .withColumn("last_contact_date", F.to_date(F.col("last_contact_date"), "yyyy-MM-dd"))
    # Validate: default_date must be past
    .withColumn("default_date", F.when(F.col("default_date") <= F.current_date(), F.col("default_date")).otherwise(F.lit(None)))
    # Validate DPD >= 0
    .withColumn("days_past_due", F.when(F.col("days_past_due") >= 0, F.col("days_past_due")).otherwise(F.lit(None)))
    # Validate outstanding_amount >= 0
    .withColumn("outstanding_amount", F.when(F.col("outstanding_amount") >= 0, F.round(F.col("outstanding_amount").cast("decimal(12,2)"), 2)).otherwise(F.lit(None)))
    # Standardize collection_stage
    .withColumn("collection_stage", F.coalesce(stage_expr[F.upper(F.col("collection_stage"))], F.lit("UNKNOWN")))
    # Dedup: keep one row per default_id
    .dropDuplicates(["default_id"])
)

logger.complete_task("clean_dedup_defaults", task_log, row_count=defaults_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Window Functions — is_repeat_default & dormancy_period_days

# COMMAND ----------

task_log = logger.start_task("window_functions")

# Partition by customer_id, order by default_date
w_cust = Window.partitionBy("customer_id").orderBy("default_date")
w_card = Window.partitionBy("card_id").orderBy("default_date")

defaults_enriched = (
    defaults_clean
    # LAG: previous default date for this customer
    .withColumn("prev_default_date", F.lag("default_date", 1).over(w_cust))
    # is_repeat_default: customer had a prior default
    .withColumn("is_repeat_default", F.col("prev_default_date").isNotNull())
    # dormancy_period_days: days since last default (NULL for first default)
    .withColumn("dormancy_period_days", F.datediff(F.col("default_date"), F.col("prev_default_date")))
    # LEAD: next default date (for trend analysis)
    .withColumn("next_default_date", F.lead("default_date", 1).over(w_cust))
    # default_sequence: rank of this default per customer
    .withColumn("default_sequence", F.rank().over(w_cust))
    # DPD trend: lag DPD to compare with previous
    .withColumn("prev_dpd", F.lag("days_past_due", 1).over(w_card))
    .withColumn("dpd_trend",
        F.when(F.col("prev_dpd").isNull(), F.lit("FIRST"))
         .when(F.col("days_past_due") > F.col("prev_dpd"), F.lit("WORSENING"))
         .when(F.col("days_past_due") < F.col("prev_dpd"), F.lit("IMPROVING"))
         .otherwise(F.lit("STABLE"))
    )
    .drop("prev_default_date", "prev_dpd")
    # Surrogate key
    .withColumn("default_sk", F.monotonically_increasing_id())
    .withColumn("_silver_created_at", F.current_timestamp())
    .select(
        "default_sk", "default_id", "customer_id", "card_id",
        "default_date", "days_past_due", "outstanding_amount",
        "collection_stage", "last_contact_date",
        "is_repeat_default", "dormancy_period_days",
        "default_sequence", "next_default_date", "dpd_trend",
        "_silver_created_at",
    )
)

logger.complete_task("window_functions", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Clean Recovery

# COMMAND ----------

task_log = logger.start_task("clean_recovery")

RECOVERY_METHOD_MAP = {
    "SETTLEMENT": "SETTLEMENT", "SET": "SETTLEMENT",
    "GARNISHMENT": "GARNISHMENT", "GARN": "GARNISHMENT",
    "CHARGEOFF": "CHARGEOFF", "CO": "CHARGEOFF",
}
RECOVERY_STATUS_MAP = {
    "PENDING": "PENDING", "P": "PENDING",
    "PARTIAL": "PARTIAL", "PART": "PARTIAL",
    "FULL": "FULL", "F": "FULL",
}

rm_expr = F.create_map([F.lit(k) for pair in RECOVERY_METHOD_MAP.items() for k in pair])
rs_expr = F.create_map([F.lit(k) for pair in RECOVERY_STATUS_MAP.items() for k in pair])

recovery_clean = (
    recovery_raw
    .filter(
        F.col("recovery_id").isNotNull()
        & F.col("default_id").isNotNull()
        & F.col("recovery_amount").isNotNull()
        & (F.col("recovery_amount") >= 0)
    )
    .withColumn("recovery_date",   F.to_date(F.col("recovery_date"), "yyyy-MM-dd"))
    .withColumn("recovery_date",   F.when(F.col("recovery_date") <= F.current_date(), F.col("recovery_date")).otherwise(F.lit(None)))
    .withColumn("recovery_amount", F.round(F.col("recovery_amount").cast("decimal(12,2)"), 2))
    .withColumn("recovery_method", F.coalesce(rm_expr[F.upper(F.col("recovery_method"))], F.lit("OTHER")))
    .withColumn("recovery_status", F.coalesce(rs_expr[F.upper(F.col("recovery_status"))], F.lit("UNKNOWN")))
    .dropDuplicates(["recovery_id"])
    .withColumn("recovery_sk", F.monotonically_increasing_id())
    .withColumn("_silver_created_at", F.current_timestamp())
    .select(
        "recovery_sk", "recovery_id", "default_id",
        "recovery_date", "recovery_amount", "recovery_method", "recovery_status",
        "_silver_created_at",
    )
)

logger.complete_task("clean_recovery", task_log, row_count=recovery_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write default_clean (MERGE on default_id)

# COMMAND ----------

task_log = logger.start_task("write_default_clean")
(
    upsert_table(spark, defaults_enriched, SILVER_DEFAULT_CLEAN, ["default_id"])
)
spark.sql(f"ALTER TABLE {SILVER_DEFAULT_CLEAN} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
default_count = spark.read.table(SILVER_DEFAULT_CLEAN).count()
logger.complete_task("write_default_clean", task_log, row_count=default_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write recovery_clean (MERGE on recovery_id)

# COMMAND ----------

task_log = logger.start_task("write_recovery_clean")
(
    upsert_table(spark, recovery_clean, SILVER_RECOVERY_CLEAN, ["recovery_id"])
)
spark.sql(f"ALTER TABLE {SILVER_RECOVERY_CLEAN} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
recovery_count = spark.read.table(SILVER_RECOVERY_CLEAN).count()
logger.complete_task("write_recovery_clean", task_log, row_count=recovery_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: DQ Checks

# COMMAND ----------

df_def = spark.read.table(SILVER_DEFAULT_CLEAN)
df_rec = spark.read.table(SILVER_RECOVERY_CLEAN)

dq_def = run_dq_suite(df_def, "default_clean", [
    {"type": "null",      "column": "default_id",    "threshold": 0.0},
    {"type": "null",      "column": "customer_id",   "threshold": 0.0},
    {"type": "null",      "column": "days_past_due", "threshold": 5.0},
    {"type": "duplicate", "pk_columns": ["default_id"]},
    {"type": "range",     "column": "days_past_due", "min": 0, "max": 3650},
    {"type": "domain",    "column": "collection_stage", "accepted_values": ["EARLY", "MID", "LATE", "LEGAL", "UNKNOWN"]},
    {"type": "domain",    "column": "dpd_trend", "accepted_values": ["FIRST", "WORSENING", "IMPROVING", "STABLE"]},
], spark=spark, pipeline_name="silver_collections", run_id=run_id)

dq_rec = run_dq_suite(df_rec, "recovery_clean", [
    {"type": "null",      "column": "recovery_id",     "threshold": 0.0},
    {"type": "null",      "column": "recovery_amount", "threshold": 0.0},
    {"type": "duplicate", "pk_columns": ["recovery_id"]},
    {"type": "range",     "column": "recovery_amount", "min": 0, "max": 10000000},
    {"type": "domain",    "column": "recovery_status", "accepted_values": ["PENDING", "PARTIAL", "FULL", "UNKNOWN"]},
], spark=spark, pipeline_name="silver_collections", run_id=run_id)

print(f"default_clean  DQ Score: {dq_def['dq_score']}% | rows: {default_count:,}")
print(f"recovery_clean DQ Score: {dq_rec['dq_score']}% | rows: {recovery_count:,}")
print(f"Run ID: {run_id}")
