# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics — Monthly Trend Analysis
# MAGIC **KPIs:** spend trend, default rate trend, recovery rate trend, new defaulters, MoM growth
# MAGIC **Sources:** `gold.fact_transaction`, `gold.fact_statement`, `gold.fact_default_analysis`, `gold.dim_date`
# MAGIC **Output:** `gold.analytics_monthly_trends` (full refresh)

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F, Window
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "analytics_monthly_trends", run_id)

# COMMAND ----------

task_log = logger.start_task("compute_trends")

dim_date = spark.read.table(GOLD_DIM_DATE).select(
    "date_sk", "year", "month_number", "month_name", "quarter", "fiscal_year", "fiscal_quarter"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Monthly Spend (from fact_transaction)

# COMMAND ----------

fact_txn = spark.read.table(GOLD_FACT_TRANSACTION)

monthly_spend = (
    fact_txn
    .join(dim_date, on="date_sk", how="left")
    .filter(F.col("year").isNotNull())
    .groupBy("year", "month_number", "month_name", "quarter", "fiscal_year", "fiscal_quarter")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.countDistinct("customer_sk").alias("active_customers"),
        F.countDistinct("card_sk").alias("active_cards"),
        F.round(F.sum(F.when(F.col("transaction_type") == "PURCHASE", F.col("amount"))), 2).alias("total_spend"),
        F.round(F.sum(F.when(F.col("transaction_type") == "REFUND",   F.col("amount"))), 2).alias("total_refunds"),
        F.round(F.avg(F.when(F.col("transaction_type") == "PURCHASE", F.col("amount"))), 2).alias("avg_txn_amount"),
        F.round(F.sum(F.when(F.col("transaction_type") == "WITHDRAWAL", F.col("amount"))), 2).alias("total_withdrawals"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Monthly Statement KPIs (from fact_statement)

# COMMAND ----------

fact_stmt = spark.read.table(GOLD_FACT_STATEMENT)

monthly_stmt = (
    fact_stmt
    .join(dim_date.withColumnRenamed("date_sk", "statement_date_sk"), on="statement_date_sk", how="left")
    .filter(F.col("year").isNotNull())
    .groupBy("year", "month_number")
    .agg(
        F.count("statement_id").alias("total_statements"),
        F.round(F.avg("utilization_ratio"), 4).alias("avg_utilization_ratio"),
        F.round(F.avg("payment_ratio"), 4).alias("avg_payment_ratio"),
        F.round(F.sum("closing_balance"), 2).alias("total_outstanding_balance"),
        F.round(F.sum("interest_charged"), 2).alias("total_interest_charged"),
        F.round(F.avg("closing_balance"), 2).alias("avg_balance_per_card"),
        F.sum(F.col("payment_due_flag").cast("int")).alias("cards_with_payment_due"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Monthly Default & Recovery (from fact_default_analysis)

# COMMAND ----------

fact_def = spark.read.table(GOLD_FACT_DEFAULT_ANALYSIS)

monthly_defaults = (
    fact_def
    .join(dim_date.withColumnRenamed("date_sk", "default_date_sk"), on="default_date_sk", how="left")
    .filter(F.col("year").isNotNull())
    .groupBy("year", "month_number")
    .agg(
        F.count("default_id").alias("new_defaults"),
        F.countDistinct("customer_sk").alias("defaulted_customers"),
        F.sum(F.col("is_repeat_default").cast("int")).alias("repeat_defaults"),
        F.round(F.avg("days_past_due"), 1).alias("avg_days_past_due"),
        F.round(F.sum("outstanding_amount"), 2).alias("total_outstanding_at_default"),
        F.round(F.sum("recovery_amount"), 2).alias("total_recovered"),
        F.sum((F.col("recovery_status") == "FULL").cast("int")).alias("full_recoveries"),
        F.sum((F.col("recovery_status") == "NO_RECOVERY").cast("int")).alias("no_recoveries"),
    )
    .withColumn("recovery_rate_pct",
        F.round(F.col("total_recovered") / F.nullif(F.col("total_outstanding_at_default"), F.lit(0)) * 100, 2)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Combine into monthly trends + MoM growth

# COMMAND ----------

w_time = Window.orderBy("year", "month_number")

monthly_combined = (
    monthly_spend
    .join(monthly_stmt,     on=["year", "month_number"], how="left")
    .join(monthly_defaults, on=["year", "month_number"], how="left")
    .fillna({"new_defaults": 0, "total_recovered": 0, "total_outstanding_at_default": 0})
    # Month-over-Month spend growth
    .withColumn("prev_spend", F.lag("total_spend", 1).over(w_time))
    .withColumn("spend_mom_pct",
        F.round((F.col("total_spend") - F.col("prev_spend")) / F.nullif(F.col("prev_spend"), F.lit(0)) * 100, 2)
    )
    # MoM default rate change
    .withColumn("prev_defaults", F.lag("new_defaults", 1).over(w_time))
    .withColumn("defaults_mom_delta", F.col("new_defaults") - F.coalesce(F.col("prev_defaults"), F.lit(0)))
    # Default rate: defaults / active_customers
    .withColumn("default_rate_pct",
        F.round(F.col("defaulted_customers") / F.nullif(F.col("active_customers"), F.lit(0)) * 100, 4)
    )
    # 3-month rolling average spend
    .withColumn("spend_3m_avg",
        F.round(F.avg("total_spend").over(w_time.rowsBetween(-2, 0)), 2)
    )
    # Year-month key for easy filtering
    .withColumn("year_month", F.concat_ws("-", F.col("year"), F.lpad(F.col("month_number").cast("string"), 2, "0")))
    .drop("prev_spend", "prev_defaults")
    .withColumn("_created_at", F.current_timestamp())
    .orderBy("year", "month_number")
)

# ── YTD / MTD / QTD ──────────────────────────────────────────────────────────

# Re-read base to compute running totals per fiscal year
w_ytd = Window.partitionBy("fiscal_year").orderBy("year", "month_number").rowsBetween(Window.unboundedPreceding, 0)
w_qtd = Window.partitionBy("fiscal_year", "fiscal_quarter").orderBy("year", "month_number").rowsBetween(Window.unboundedPreceding, 0)

monthly_combined = (
    monthly_combined
    # YTD: cumulative spend within fiscal year
    .withColumn("ytd_spend",            F.round(F.sum("total_spend").over(w_ytd), 2))
    .withColumn("ytd_transactions",     F.sum("total_transactions").over(w_ytd))
    .withColumn("ytd_defaults",         F.sum("new_defaults").over(w_ytd))
    # QTD: cumulative spend within fiscal quarter
    .withColumn("qtd_spend",            F.round(F.sum("total_spend").over(w_qtd), 2))
    .withColumn("qtd_transactions",     F.sum("total_transactions").over(w_qtd))
    # MTD: current month totals (same as monthly values — already at monthly grain)
    .withColumn("mtd_spend",            F.col("total_spend"))
    .withColumn("mtd_transactions",     F.col("total_transactions"))
    .withColumn("mtd_new_defaults",     F.col("new_defaults"))
)

(
    monthly_combined.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.analytics_monthly_trends")
)

row_count = monthly_combined.count()
logger.complete_task("compute_trends", task_log, row_count=row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Trend Summary

# COMMAND ----------

mt = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_monthly_trends")
print("=" * 100)
print("MONTHLY TRENDS — KEY METRICS")
print("=" * 100)

mt.select(
    "year_month", "total_spend", "spend_mom_pct",
    "new_defaults", "default_rate_pct", "recovery_rate_pct",
    "avg_utilization_ratio", "active_customers",
).orderBy("year", "month_number").show(24, truncate=False)

# Aggregate by year
print("\nYEARLY ROLLUP:")
mt.groupBy("year").agg(
    F.round(F.sum("total_spend") / 1e6, 2).alias("total_spend_M"),
    F.sum("new_defaults").alias("total_defaults"),
    F.round(F.avg("avg_utilization_ratio") * 100, 1).alias("avg_util_pct"),
    F.round(F.avg("recovery_rate_pct"), 1).alias("avg_recovery_rate_pct"),
).orderBy("year").show(truncate=False)

print(f"Total months: {mt.count()} | Run ID: {run_id}")
