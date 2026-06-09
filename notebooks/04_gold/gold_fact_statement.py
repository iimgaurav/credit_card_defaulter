# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_statement
# MAGIC **Source:** `silver.statement_clean`
# MAGIC **Target:** `gold.fact_statement`
# MAGIC **Grain:** 1 row per statement (card × month)
# MAGIC **FKs:** customer_sk, card_sk, statement_date_sk, due_date_sk

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "gold_fact_statement", run_id)

# COMMAND ----------

task_log = logger.start_task("build_fact_statement")

stmt = spark.read.table(SILVER_STATEMENT_CLEAN)

dim_customer = F.broadcast(
    spark.read.table(GOLD_DIM_CUSTOMER).filter("is_current = true")
    .select("customer_sk", F.col("customer_sk").alias("dim_customer_sk"))
)
dim_card = F.broadcast(
    spark.read.table(GOLD_DIM_CARD).filter("is_current = true")
    .select(F.col("card_sk").alias("dim_card_sk"), "card_id", "customer_sk")
)
dim_date = F.broadcast(spark.read.table(GOLD_DIM_DATE).select("date_sk", "full_date"))

# Reusable helper: resolve date_sk for a date column
def resolve_date_sk(df, date_col, alias):
    date_sk_col = F.date_format(date_col, "yyyyMMdd").cast("int")
    return (
        df.join(
            dim_date.withColumnRenamed("date_sk", alias),
            date_sk_col == F.col(alias),
            how="left"
        )
        .withColumn(alias, F.coalesce(F.col(alias), date_sk_col))
    )

# Join card → get dim_card_sk and customer_sk, then customer dim
stmt_with_cust = stmt.join(
    dim_card.select("card_id", "dim_card_sk", "customer_sk"),
    on="card_id", how="left"
).join(
    dim_customer, on="customer_sk", how="left"
)

# Resolve both date FKs
stmt_with_dates = resolve_date_sk(stmt_with_cust, "statement_date", "statement_date_sk")
stmt_with_dates = resolve_date_sk(stmt_with_dates, "due_date", "due_date_sk")

fact = (
    stmt_with_dates
    .withColumn("customer_sk", F.coalesce(F.col("dim_customer_sk"), F.lit(-1)))
    .withColumn("card_sk",     F.coalesce(F.col("dim_card_sk"),     F.lit(-1)))
    .withColumn("statement_sk", F.monotonically_increasing_id())
    .withColumn("_created_at", F.current_timestamp())
    .select(
        "statement_sk", "statement_id",
        "customer_sk", "card_sk",
        "statement_date_sk", "due_date_sk",
        "opening_balance", "total_purchases", "total_payments", "total_credits",
        "interest_charged", "fees_charged", "closing_balance", "minimum_due",
        "payment_due_flag", "days_to_due", "utilization_ratio", "payment_ratio",
        "_created_at",
    )
    .dropDuplicates(["statement_id"])
)

(
    upsert_table(spark, fact, GOLD_FACT_STATEMENT, ["statement_id"])
)

cnt = spark.read.table(GOLD_FACT_STATEMENT).count()
logger.complete_task("build_fact_statement", task_log, row_count=cnt)
print(f"fact_statement: {cnt:,} rows | Run ID: {run_id}")
