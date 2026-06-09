# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Validation Report
# MAGIC Validates star schema integrity:
# MAGIC - FK checks (all fact → dim relationships)
# MAGIC - SCD2 timeline integrity (no overlaps, single current record per NK)
# MAGIC - Orphan fact rows (date_sk / customer_sk / card_sk = -1)
# MAGIC - Row count summary

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

# MAGIC %run ../00_utilities/dq_framework

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())
logger = PipelineLogger(spark, "gold_validate", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold Tables

# COMMAND ----------

task_log = logger.start_task("load_gold")

dim_date     = spark.read.table(GOLD_DIM_DATE)
dim_geo      = spark.read.table(GOLD_DIM_GEOGRAPHY)
dim_customer = spark.read.table(GOLD_DIM_CUSTOMER)
dim_card     = spark.read.table(GOLD_DIM_CARD)
fact_txn     = spark.read.table(GOLD_FACT_TRANSACTION)
fact_stmt    = spark.read.table(GOLD_FACT_STATEMENT)
fact_default = spark.read.table(GOLD_FACT_DEFAULT_ANALYSIS)

logger.complete_task("load_gold", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row Counts

# COMMAND ----------

tables = {
    "dim_date":            dim_date,
    "dim_geography":       dim_geo,
    "dim_customer (total)":dim_customer,
    "dim_customer (curr)": dim_customer.filter("is_current = true"),
    "dim_card (total)":    dim_card,
    "dim_card (curr)":     dim_card.filter("is_current = true"),
    "fact_transaction":    fact_txn,
    "fact_statement":      fact_stmt,
    "fact_default_analysis": fact_default,
}

print("=== ROW COUNTS ===")
for name, df in tables.items():
    print(f"  {name:<35} {df.count():>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. FK Integrity — Facts → Dims

# COMMAND ----------

task_log = logger.start_task("fk_checks")

# Helpers
dim_customer_sks = dim_customer.select("customer_sk")
dim_card_sks     = dim_card.select("card_sk")
dim_date_sks     = dim_date.select("date_sk")
dim_geo_sks      = dim_geo.select("geo_sk")

def count_orphans(fact_df, fk_col, dim_df, dim_col, sentinel=-1):
    """Count FK values not in dim, excluding sentinel."""
    return (
        fact_df.filter(F.col(fk_col) != sentinel)
        .join(dim_df, fact_df[fk_col] == dim_df[dim_col], "left_anti")
        .count()
    )

fk_results = {
    "fact_txn  → dim_customer": count_orphans(fact_txn,  "customer_sk", dim_customer_sks, "customer_sk"),
    "fact_txn  → dim_card":     count_orphans(fact_txn,  "card_sk",     dim_card_sks,     "card_sk"),
    "fact_txn  → dim_date":     count_orphans(fact_txn,  "date_sk",     dim_date_sks,     "date_sk", sentinel=0),
    "fact_txn  → dim_geography":count_orphans(fact_txn,  "geo_sk",      dim_geo_sks,      "geo_sk"),
    "fact_stmt → dim_customer": count_orphans(fact_stmt, "customer_sk", dim_customer_sks, "customer_sk"),
    "fact_stmt → dim_card":     count_orphans(fact_stmt, "card_sk",     dim_card_sks,     "card_sk"),
    "fact_stmt → dim_date(stmt)":count_orphans(fact_stmt,"statement_date_sk", dim_date_sks, "date_sk", sentinel=0),
    "fact_stmt → dim_date(due)": count_orphans(fact_stmt,"due_date_sk", dim_date_sks,     "date_sk", sentinel=0),
    "fact_def  → dim_customer": count_orphans(fact_default,"customer_sk",dim_customer_sks,"customer_sk"),
    "fact_def  → dim_card":     count_orphans(fact_default,"card_sk",   dim_card_sks,     "card_sk"),
    "fact_def  → dim_date":     count_orphans(fact_default,"default_date_sk",dim_date_sks,"date_sk", sentinel=0),
}

print("\n=== FK INTEGRITY ===")
all_fk_pass = True
for rel, orphans in fk_results.items():
    status = "✅ PASS" if orphans == 0 else f"❌ {orphans:,} orphans"
    if orphans > 0: all_fk_pass = False
    print(f"  {rel:<40} {status}")

logger.complete_task("fk_checks", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD2 Timeline Integrity

# COMMAND ----------

task_log = logger.start_task("scd2_checks")

def check_scd2(dim_df, nk_col, label, expiry_col="expiry_date"):
    results = {}
    # Only one current row per natural key
    multi_current = (
        dim_df.filter("is_current = true")
        .groupBy(nk_col).count()
        .filter(F.col("count") > 1).count()
    )
    results["multi_current"] = multi_current

    # No date overlaps: effective_date <= expiry
    bad_dates = dim_df.filter(F.col("effective_date") > F.col(expiry_col)).count()
    results["bad_dates"] = bad_dates

    # Expiry = 9999-12-31 only for current rows
    bad_expiry = dim_df.filter(
        (F.col(expiry_col) == F.lit("9999-12-31").cast("date")) &
        (F.col("is_current") == False)
    ).count()
    results["open_expiry_non_current"] = bad_expiry

    print(f"\n  {label}:")
    print(f"    Multi-current NK    : {'✅ 0' if multi_current == 0 else f'❌ {multi_current}'}")
    print(f"    Bad date range      : {'✅ 0' if bad_dates == 0 else f'❌ {bad_dates}'}")
    print(f"    Open expiry non-curr: {'✅ 0' if bad_expiry == 0 else f'❌ {bad_expiry}'}")
    return results

print("\n=== SCD2 TIMELINE ===")
scd2_customer = check_scd2(dim_customer, "customer_id", "dim_customer")
scd2_card     = check_scd2(dim_card,     "card_id",     "dim_card",     expiry_col="expiry_date")

logger.complete_task("scd2_checks", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Orphan Sentinel Check (fact rows without dim match)

# COMMAND ----------

task_log = logger.start_task("sentinel_check")

print("\n=== ORPHAN SENTINEL ROWS (-1 FK) ===")
for fact_name, fact_df in [("fact_transaction", fact_txn), ("fact_statement", fact_stmt), ("fact_default", fact_default)]:
    for fk in ["customer_sk", "card_sk"]:
        if fk in fact_df.columns:
            cnt = fact_df.filter(F.col(fk) == -1).count()
            pct = round(cnt / fact_df.count() * 100, 2) if fact_df.count() > 0 else 0
            status = "✅" if pct < 1.0 else "⚠️ "
            print(f"  {fact_name:<25} {fk:<15} sentinel={cnt:>6,} ({pct}%) {status}")

logger.complete_task("sentinel_check", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

scd2_pass = all(v == 0 for d in [scd2_customer, scd2_card] for v in d.values())
overall = "✅ PASS" if (all_fk_pass and scd2_pass) else "⚠️  WARNINGS — review above"

print(f"\n{'='*55}")
print(f"GOLD VALIDATION SUMMARY")
print(f"{'='*55}")
print(f"FK Integrity  : {'✅ ALL PASS' if all_fk_pass else '❌ FAILURES'}")
print(f"SCD2 Timeline : {'✅ ALL PASS' if scd2_pass else '❌ FAILURES'}")
print(f"Overall       : {overall}")
print(f"Run ID        : {run_id}")
print(f"{'='*55}")
