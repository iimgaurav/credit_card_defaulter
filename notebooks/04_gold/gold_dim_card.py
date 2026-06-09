# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_card (SCD Type 2)
# MAGIC **Source:** `silver.card_clean`
# MAGIC **Target:** `gold.dim_card`
# MAGIC **Type:** SCD Type 2 — tracks changes to credit_limit, cash_limit, interest_rate, current_status
# MAGIC **SK:** `card_sk` — new surrogate per version

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "gold_dim_card", run_id)

SCD2_EXPIRY = "9999-12-31"
TODAY       = F.current_date()

SCD2_COLS = ["credit_limit", "cash_limit", "interest_rate", "current_status"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source + resolve customer_sk (current version)

# COMMAND ----------

task_log = logger.start_task("read_source")

silver_card = spark.read.table(SILVER_CARD_CLEAN)

# FK: resolve customer_id → current customer_sk from dim_customer
dim_cust_current = (
    spark.read.table(GOLD_DIM_CUSTOMER)
    .filter(F.col("is_current") == True)
    .select(F.col("customer_sk").alias("dim_customer_sk"), "customer_id")
)

source = (
    silver_card
    .join(dim_cust_current, on="customer_id", how="left")
    .withColumnRenamed("dim_customer_sk", "customer_sk_fk")
    .withColumn("scd_hash",
        F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SCD2_COLS]))
    )
)

logger.complete_task("read_source", task_log, row_count=source.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: SCD2 — first load or incremental

# COMMAND ----------

task_log = logger.start_task("scd2_merge")

table_exists = spark.catalog.tableExists(GOLD_DIM_CARD)

def build_card_row(df):
    return df.select(
        "card_sk", "card_id", F.col("customer_sk_fk").alias("customer_sk"),
        "card_type", "card_network", "issued_date",
        F.col("expiry_date").alias("card_expiry_date"),
        "credit_limit", "cash_limit", "interest_rate", "current_status",
        "effective_date", "expiry_date", "is_current", "scd_hash",
        "_created_at", "_modified_at",
    )

# ── Migration: rename scd_expiry_date → expiry_date on existing table ─
if table_exists:
    try:
        spark.sql(f"ALTER TABLE {GOLD_DIM_CARD} RENAME COLUMN scd_expiry_date TO expiry_date")
        print("Migrated column: scd_expiry_date → expiry_date")
    except Exception:
        pass  # already migrated or never had old name

if not table_exists:
    first_load = (
        source
        .withColumn("card_sk",          F.monotonically_increasing_id())
        .withColumn("effective_date",    TODAY)
        .withColumn("expiry_date",       F.lit(SCD2_EXPIRY).cast("date"))
        .withColumn("is_current",        F.lit(True))
        .withColumn("_created_at",       F.current_timestamp())
        .withColumn("_modified_at",      F.current_timestamp())
    )
    build_card_row(first_load).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DIM_CARD)
else:
    dim = DeltaTable.forName(spark, GOLD_DIM_CARD)
    dim_current = spark.read.table(GOLD_DIM_CARD).filter("is_current = true")

    changed = (
        source.alias("src")
        .join(dim_current.alias("dim"), source.card_id == dim_current.card_id, how="left")
        .filter(
            F.col("dim.card_id").isNull() |
            (F.col("src.scd_hash") != F.col("dim.scd_hash"))
        )
        .select("src.*")
    )

    # Expire old rows
    (
        dim.alias("dim")
        .merge(
            changed.select("card_id", "scd_hash").alias("chg"),
            "dim.card_id = chg.card_id AND dim.is_current = true AND dim.scd_hash != chg.scd_hash"
        )
        .whenMatchedUpdate(set={
            "is_current":     F.lit(False),
            "expiry_date":    F.date_sub(TODAY, 1),
            "_modified_at":   F.current_timestamp(),
        })
        .execute()
    )

    # Insert new versions
    new_rows = (
        changed
        .withColumn("card_sk",        F.monotonically_increasing_id())
        .withColumn("effective_date",  TODAY)
        .withColumn("expiry_date",     F.lit(SCD2_EXPIRY).cast("date"))
        .withColumn("is_current",      F.lit(True))
        .withColumn("_created_at",     F.current_timestamp())
        .withColumn("_modified_at",    F.current_timestamp())
    )
    build_card_row(new_rows).write.format("delta").mode("append").saveAsTable(GOLD_DIM_CARD)

cnt = spark.read.table(GOLD_DIM_CARD).count()
current_cnt = spark.read.table(GOLD_DIM_CARD).filter("is_current = true").count()
logger.complete_task("scd2_merge", task_log, row_count=cnt)
print(f"dim_card: {cnt:,} total rows | {current_cnt:,} current | Run ID: {run_id}")
