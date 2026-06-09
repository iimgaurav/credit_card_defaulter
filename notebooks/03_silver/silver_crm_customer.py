# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — CRM Customer Clean
# MAGIC **Sources:** `bronze.crm_customer_master` + `bronze.crm_customer_address`
# MAGIC **Target:** `silver.customer_clean`
# MAGIC **Transforms:** dedup (keep latest), standardize gender/status, validate email/DOB/credit_score, join address

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
logger = PipelineLogger(spark, "silver_crm_customer", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze

# COMMAND ----------

task_log = logger.start_task("read_bronze")
try:
    crm_raw = spark.read.table(BRONZE_CRM_CUSTOMER)
    addr_raw = spark.read.table(BRONZE_CRM_ADDRESS)

    logger.complete_task("read_bronze", task_log, row_count=crm_raw.count())
except Exception as e:
    logger.fail_task("read_bronze", task_log, e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Dedup CRM — keep latest record per customer_id

# COMMAND ----------

task_log = logger.start_task("dedup")
try:
    w_latest = Window.partitionBy("customer_id").orderBy(F.col("load_timestamp").desc())
    crm_dedup = (
        crm_raw
        .filter(F.col("customer_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    logger.complete_task("dedup", task_log, row_count=crm_dedup.count())
except Exception as e:
    logger.fail_task("dedup", task_log, e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Standardize & Validate

# COMMAND ----------

task_log = logger.start_task("standardize_validate")
try:
    GENDER_MAP = {"M": "MALE", "F": "FEMALE", "O": "OTHER"}
    MARITAL_MAP = {"S": "SINGLE", "M": "MARRIED", "D": "DIVORCED", "W": "WIDOWED"}
    EMPLOYMENT_MAP = {
        "EMP": "EMPLOYED", "SELF": "SELF_EMPLOYED",
        "RET": "RETIRED", "UNEMP": "UNEMPLOYED", "STU": "STUDENT"
    }

    gender_expr = F.create_map([F.lit(k) for pair in GENDER_MAP.items() for k in pair])
    marital_expr = F.create_map([F.lit(k) for pair in MARITAL_MAP.items() for k in pair])
    employ_expr = F.create_map([F.lit(k) for pair in EMPLOYMENT_MAP.items() for k in pair])

    crm_std = (
        crm_dedup
        .withColumn("gender", F.coalesce(gender_expr[F.upper(F.col("gender"))], F.lit("OTHER")))
        .withColumn("marital_status", F.coalesce(marital_expr[F.upper(F.col("marital_status"))], F.lit("UNKNOWN")))
        .withColumn("employment_status", F.coalesce(employ_expr[F.upper(F.col("employment_status"))], F.lit("UNKNOWN")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("email", F.when(F.col("email").rlike(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$"), F.col("email")).otherwise(F.lit(None)))
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
        .withColumn("date_of_birth", F.when(F.col("date_of_birth") < F.current_date(), F.col("date_of_birth")).otherwise(F.lit(None)))
        .withColumn("annual_income", F.when(F.col("annual_income") > 0, F.col("annual_income")).otherwise(F.lit(None)))
        .withColumn("credit_score", F.when(F.col("credit_score").between(300, 850), F.col("credit_score")).otherwise(F.lit(None)))
        .withColumn("phone_number", F.regexp_replace(F.col("phone_number"), r"[^\d+]", ""))
        .withColumn("full_name", F.concat_ws(" ", F.initcap(F.col("first_name")), F.initcap(F.col("last_name"))))
    )

    logger.complete_task("standardize_validate", task_log)
except Exception as e:
    logger.fail_task("standardize_validate", task_log, e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Join Address (latest HOME address per customer)

# COMMAND ----------

task_log = logger.start_task("join_address")
try:
    w_addr = Window.partitionBy("customer_id").orderBy(F.col("load_timestamp").desc())

    addr_dedup = (
        addr_raw
        .filter(F.col("customer_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w_addr))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select("customer_id", "city", "state_code", "country_code", "zip_code")
    )

    crm_enriched = crm_std.join(addr_dedup, on="customer_id", how="left")

    logger.complete_task("join_address", task_log)
except Exception as e:
    logger.fail_task("join_address", task_log, e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Final Select + Surrogate Key

# COMMAND ----------

customer_clean = (
    crm_enriched
    .withColumn("customer_sk", F.monotonically_increasing_id())
    .withColumn("_silver_created_at", F.current_timestamp())
    .select(
        "customer_sk", "customer_id", "full_name", "date_of_birth",
        "gender", "marital_status", "email", "phone_number",
        "employment_status", "annual_income", "credit_score",
        "city", "state_code", "country_code", "zip_code",
        "_silver_created_at",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write to Silver (MERGE on customer_id)

# COMMAND ----------

task_log = logger.start_task("write_silver")
try:
    (
        upsert_table(spark, customer_clean, SILVER_CUSTOMER_CLEAN, ["customer_id"])
    )
    spark.sql(f"ALTER TABLE {SILVER_CUSTOMER_CLEAN} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    final_count = spark.read.table(SILVER_CUSTOMER_CLEAN).count()
    logger.complete_task("write_silver", task_log, row_count=final_count)
except Exception as e:
    logger.fail_task("write_silver", task_log, e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: DQ Checks

# COMMAND ----------

df_silver = spark.read.table(SILVER_CUSTOMER_CLEAN)

dq_results = run_dq_suite(df_silver, "customer_clean", [
    {"type": "null", "column": "customer_id",  "threshold": 0.0},
    {"type": "null", "column": "email",        "threshold": 5.0},
    {"type": "null", "column": "credit_score", "threshold": 10.0},
    {"type": "duplicate", "pk_columns": ["customer_id"]},
    {"type": "range", "column": "credit_score", "min": 300, "max": 850},
    {"type": "domain", "column": "gender", "accepted_values": ["MALE", "FEMALE", "OTHER"]},
], spark=spark, pipeline_name="silver_crm_customer", run_id=run_id)

print(f"DQ Score: {dq_results['dq_score']}%")
print(f"Final row count: {final_count:,}")
print(f"Run ID: {run_id}")
