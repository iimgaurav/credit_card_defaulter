# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Billing Clean
# MAGIC **Sources:** `bronze.billing_statements` + `bronze.billing_payments`
# MAGIC **Targets:** `silver.statement_clean` + `silver.payment_clean`
# MAGIC **Transforms:** cast dates, compute utilization/payment ratios, validate balances, join payment summary

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
logger = PipelineLogger(spark, "silver_billing", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze

# COMMAND ----------

task_log = logger.start_task("read_bronze")

stmt_raw = spark.read.table(BRONZE_BILLING_STATEMENTS)
pay_raw  = spark.read.table(BRONZE_BILLING_PAYMENTS)

logger.complete_task("read_bronze", task_log, row_count=stmt_raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Clean Statements

# COMMAND ----------

task_log = logger.start_task("clean_statements")

stmt_clean = (
    stmt_raw
    .filter(F.col("statement_id").isNotNull() & F.col("card_id").isNotNull())
    # Parse dates
    .withColumn("statement_date", F.to_date(F.col("statement_date"), "yyyy-MM-dd"))
    .withColumn("due_date",       F.to_date(F.col("due_date"),       "yyyy-MM-dd"))
    # Validate date logic
    .withColumn("due_date", F.when(F.col("due_date") > F.col("statement_date"), F.col("due_date")).otherwise(F.lit(None)))
    # Validate amounts >= 0
    .withColumn("opening_balance",  F.coalesce(F.col("opening_balance"),  F.lit(0.0)))
    .withColumn("total_purchases",  F.when(F.col("total_purchases")  >= 0, F.col("total_purchases")).otherwise(F.lit(0.0)))
    .withColumn("total_payments",   F.when(F.col("total_payments")   >= 0, F.col("total_payments")).otherwise(F.lit(0.0)))
    .withColumn("total_credits",    F.when(F.col("total_credits")    >= 0, F.col("total_credits")).otherwise(F.lit(0.0)))
    .withColumn("interest_charged", F.when(F.col("interest_charged") >= 0, F.col("interest_charged")).otherwise(F.lit(0.0)))
    .withColumn("fees_charged",     F.when(F.col("fees_charged")     >= 0, F.col("fees_charged")).otherwise(F.lit(0.0)))
    .withColumn("minimum_due",      F.when(F.col("minimum_due")      >= 0, F.col("minimum_due")).otherwise(F.lit(0.0)))
    # Round all monetary columns
    .withColumn("closing_balance", F.round(F.col("closing_balance").cast("decimal(12,2)"), 2))
    # Computed fields
    .withColumn("payment_due_flag", F.col("closing_balance") > F.col("minimum_due"))
    .withColumn("days_to_due", F.datediff(F.col("due_date"), F.col("statement_date")))
    .withColumn("utilization_ratio",
        F.round(F.col("closing_balance") / F.nullif(F.col("total_purchases") + F.col("opening_balance"), F.lit(0)), 4)
    )
    .withColumn("payment_ratio",
        F.round(F.col("total_payments") / F.nullif(F.col("closing_balance"), F.lit(0)), 4)
    )
    # Dedup: keep latest per statement_id
    .dropDuplicates(["statement_id"])
    # Surrogate key
    .withColumn("statement_sk", F.monotonically_increasing_id())
    .withColumn("_silver_created_at", F.current_timestamp())
    .select(
        "statement_sk", "statement_id", "card_id",
        "statement_date", "due_date",
        "opening_balance", "total_purchases", "total_payments", "total_credits",
        "interest_charged", "fees_charged", "closing_balance", "minimum_due",
        "payment_due_flag", "days_to_due", "utilization_ratio", "payment_ratio",
        "_silver_created_at",
    )
)

logger.complete_task("clean_statements", task_log, row_count=stmt_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Clean Payments

# COMMAND ----------

task_log = logger.start_task("clean_payments")

PAY_METHOD_MAP = {
    "ACH": "ACH", "WIRE": "WIRE", "CHQ": "CHEQUE", "CHEQUE": "CHEQUE",
    "CASH": "CASH", "CARD": "CARD",
}
pm_expr = F.create_map([F.lit(k) for pair in PAY_METHOD_MAP.items() for k in pair])

pay_clean = (
    pay_raw
    .filter(
        F.col("payment_id").isNotNull()
        & F.col("statement_id").isNotNull()
        & F.col("payment_amount").isNotNull()
        & (F.col("payment_amount") > 0)
    )
    .withColumn("payment_date", F.to_date(F.col("payment_date"), "yyyy-MM-dd"))
    .withColumn("payment_date", F.when(F.col("payment_date") <= F.current_date(), F.col("payment_date")).otherwise(F.lit(None)))
    .withColumn("payment_amount", F.round(F.col("payment_amount").cast("decimal(12,2)"), 2))
    .withColumn("payment_method", F.coalesce(pm_expr[F.upper(F.col("payment_method"))], F.lit("OTHER")))
    .dropDuplicates(["payment_id"])
    .withColumn("payment_sk", F.monotonically_increasing_id())
    .withColumn("_silver_created_at", F.current_timestamp())
    .select(
        "payment_sk", "payment_id", "statement_id",
        "payment_date", "payment_amount", "payment_method",
        "_silver_created_at",
    )
)

logger.complete_task("clean_payments", task_log, row_count=pay_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write statement_clean (MERGE on statement_id)

# COMMAND ----------

task_log = logger.start_task("write_statement_clean")
(
    upsert_table(spark, stmt_clean, SILVER_STATEMENT_CLEAN, ["statement_id"])
)
spark.sql(f"ALTER TABLE {SILVER_STATEMENT_CLEAN} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
stmt_count = spark.read.table(SILVER_STATEMENT_CLEAN).count()
logger.complete_task("write_statement_clean", task_log, row_count=stmt_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write payment_clean (MERGE on payment_id)

# COMMAND ----------

task_log = logger.start_task("write_payment_clean")
(
    upsert_table(spark, pay_clean, SILVER_PAYMENT_CLEAN, ["payment_id"])
)
spark.sql(f"ALTER TABLE {SILVER_PAYMENT_CLEAN} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
pay_count = spark.read.table(SILVER_PAYMENT_CLEAN).count()
logger.complete_task("write_payment_clean", task_log, row_count=pay_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: DQ Checks

# COMMAND ----------

df_stmt = spark.read.table(SILVER_STATEMENT_CLEAN)
df_pay  = spark.read.table(SILVER_PAYMENT_CLEAN)

dq_stmt = run_dq_suite(df_stmt, "statement_clean", [
    {"type": "null",      "column": "statement_id",   "threshold": 0.0},
    {"type": "null",      "column": "card_id",        "threshold": 0.0},
    {"type": "null",      "column": "closing_balance","threshold": 1.0},
    {"type": "duplicate", "pk_columns": ["statement_id"]},
    {"type": "range",     "column": "utilization_ratio", "min": 0, "max": 10},
], spark=spark, pipeline_name="silver_billing", run_id=run_id)

dq_pay = run_dq_suite(df_pay, "payment_clean", [
    {"type": "null",      "column": "payment_id",     "threshold": 0.0},
    {"type": "null",      "column": "payment_amount", "threshold": 0.0},
    {"type": "duplicate", "pk_columns": ["payment_id"]},
    {"type": "range",     "column": "payment_amount", "min": 0.01, "max": 1000000},
    {"type": "domain",    "column": "payment_method", "accepted_values": ["ACH", "WIRE", "CHEQUE", "CASH", "CARD", "OTHER"]},
], spark=spark, pipeline_name="silver_billing", run_id=run_id)

print(f"statement_clean DQ Score: {dq_stmt['dq_score']}% | rows: {stmt_count:,}")
print(f"payment_clean   DQ Score: {dq_pay['dq_score']}%  | rows: {pay_count:,}")
print(f"Run ID: {run_id}")
