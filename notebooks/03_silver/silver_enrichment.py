# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Customer 360 Enrichment
# MAGIC **Sources:** all Silver clean tables
# MAGIC **Target:** `silver.customer_360_view`
# MAGIC **Transforms:** join all silver entities per customer, compute behavioral aggregates, risk signals

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
logger = PipelineLogger(spark, "silver_enrichment", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read all Silver tables

# COMMAND ----------

task_log = logger.start_task("read_silver_tables")

customers  = spark.read.table(SILVER_CUSTOMER_CLEAN)
cards      = spark.read.table(SILVER_CARD_CLEAN).cache()   # reused 3× — cache to avoid re-scan
txns       = spark.read.table(SILVER_TRANSACTION_CLEAN)
statements = spark.read.table(SILVER_STATEMENT_CLEAN)
payments   = spark.read.table(SILVER_PAYMENT_CLEAN)
defaults   = spark.read.table(SILVER_DEFAULT_CLEAN)
recoveries = spark.read.table(SILVER_RECOVERY_CLEAN)

logger.complete_task("read_silver_tables", task_log, row_count=customers.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Card aggregates per customer

# COMMAND ----------

task_log = logger.start_task("agg_cards")

card_agg = (
    cards
    .groupBy("customer_id")
    .agg(
        F.count("card_id").alias("total_cards"),
        F.sum(F.when(F.col("current_status") == "ACTIVE", 1).otherwise(0)).alias("active_cards"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.avg("interest_rate").alias("avg_interest_rate"),
    )
    .withColumn("avg_interest_rate", F.round("avg_interest_rate", 2))
)

logger.complete_task("agg_cards", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transaction aggregates per customer

# COMMAND ----------

task_log = logger.start_task("agg_transactions")

txn_agg = (
    txns
    .groupBy("customer_id")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum(F.when(F.col("transaction_type") == "PURCHASE", F.col("amount")).otherwise(F.lit(0))).alias("total_spend"),
        F.avg(F.when(F.col("transaction_type") == "PURCHASE", F.col("amount"))).alias("avg_txn_amount"),
        F.max("transaction_datetime").alias("last_transaction_date"),
        F.countDistinct("merchant_category_code").alias("unique_merchant_categories"),
        F.sum(F.when(F.col("transaction_type") == "REFUND", F.col("amount")).otherwise(F.lit(0))).alias("total_refunds"),
    )
    .withColumn("total_spend", F.round("total_spend", 2))
    .withColumn("avg_txn_amount", F.round("avg_txn_amount", 2))
    .withColumn("total_refunds", F.round("total_refunds", 2))
)

logger.complete_task("agg_transactions", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Billing aggregates per customer (via card join)

# COMMAND ----------

task_log = logger.start_task("agg_billing")

# Join statements to cards to get customer_id
stmt_with_cust = statements.join(
    cards.select("card_id", "customer_id"), on="card_id", how="left"
)

billing_agg = (
    stmt_with_cust
    .groupBy("customer_id")
    .agg(
        F.count("statement_id").alias("total_statements"),
        F.avg("closing_balance").alias("avg_closing_balance"),
        F.avg("utilization_ratio").alias("avg_utilization_ratio"),
        F.avg("payment_ratio").alias("avg_payment_ratio"),
        F.sum(F.when(F.col("payment_due_flag"), 1).otherwise(0)).alias("months_payment_due"),
        F.avg("interest_charged").alias("avg_monthly_interest"),
        F.max("statement_date").alias("last_statement_date"),
    )
    .withColumn("avg_closing_balance",   F.round("avg_closing_balance",   2))
    .withColumn("avg_utilization_ratio", F.round("avg_utilization_ratio", 4))
    .withColumn("avg_payment_ratio",     F.round("avg_payment_ratio",     4))
    .withColumn("avg_monthly_interest",  F.round("avg_monthly_interest",  2))
)

logger.complete_task("agg_billing", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Default/Risk aggregates per customer

# COMMAND ----------

task_log = logger.start_task("agg_defaults")

default_agg = (
    defaults
    .groupBy("customer_id")
    .agg(
        F.count("default_id").alias("total_defaults"),
        F.max("days_past_due").alias("max_days_past_due"),
        F.sum("outstanding_amount").alias("total_outstanding_at_default"),
        F.max("default_date").alias("last_default_date"),
        F.max("default_sequence").alias("max_default_sequence"),
        F.sum(F.when(F.col("dpd_trend") == "WORSENING", 1).otherwise(0)).alias("worsening_dpd_events"),
        F.first("collection_stage", ignorenulls=True).alias("latest_collection_stage"),
    )
    .withColumn("total_outstanding_at_default", F.round("total_outstanding_at_default", 2))
    .withColumn("has_defaulted", F.lit(True))
)

logger.complete_task("agg_defaults", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Join everything into customer_360_view

# COMMAND ----------

task_log = logger.start_task("join_360")

customer_360 = (
    customers
    .join(card_agg,    on="customer_id", how="left")
    .join(txn_agg,     on="customer_id", how="left")
    .join(billing_agg, on="customer_id", how="left")
    .join(default_agg, on="customer_id", how="left")
    # Derived risk signals
    .withColumn("has_defaulted",     F.coalesce(F.col("has_defaulted"),     F.lit(False)))
    .withColumn("total_defaults",    F.coalesce(F.col("total_defaults"),     F.lit(0)))
    .withColumn("total_transactions",F.coalesce(F.col("total_transactions"), F.lit(0)))
    .withColumn("total_spend",       F.coalesce(F.col("total_spend"),        F.lit(0.0)))
    # Risk score bucket (simple rules-based, Gold layer will do full ML features)
    .withColumn("risk_band",
        F.when(F.col("has_defaulted") & (F.col("total_defaults") >= 2), F.lit("HIGH"))
         .when(F.col("has_defaulted"), F.lit("MEDIUM"))
         .when(F.col("credit_score") < 600, F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    )
    # Days since last transaction
    .withColumn("days_since_last_txn",
        F.datediff(F.current_date(), F.col("last_transaction_date").cast("date"))
    )
    .withColumn("_silver_created_at", F.current_timestamp())
)

logger.complete_task("join_360", task_log, row_count=customer_360.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Silver (full overwrite — snapshot view)

# COMMAND ----------

task_log = logger.start_task("write_silver")

(
    upsert_table(spark, customer_360, SILVER_CUSTOMER_360, ["customer_id"])
)

final_count = spark.read.table(SILVER_CUSTOMER_360).count()
logger.complete_task("write_silver", task_log, row_count=final_count)

# Release cached DataFrame
cards.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: DQ Checks

# COMMAND ----------

df_360 = spark.read.table(SILVER_CUSTOMER_360)

dq_results = run_dq_suite(df_360, "customer_360_view", [
    {"type": "null",      "column": "customer_id",   "threshold": 0.0},
    {"type": "null",      "column": "credit_score",  "threshold": 10.0},
    {"type": "duplicate", "pk_columns": ["customer_id"]},
    {"type": "domain",    "column": "risk_band", "accepted_values": ["LOW", "MEDIUM", "HIGH"]},
    {"type": "domain",    "column": "has_defaulted", "accepted_values": [True, False]},
], spark=spark, pipeline_name="silver_enrichment", run_id=run_id)

print(f"DQ Score: {dq_results['dq_score']}%")
print(f"Final row count: {final_count:,}")
print(f"Run ID: {run_id}")
