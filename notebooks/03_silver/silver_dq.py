# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — DQ Validation Report
# MAGIC **Runs comprehensive DQ checks across all 8 Silver tables:**
# MAGIC - PK uniqueness on all tables
# MAGIC - FK integrity (cards → customers, transactions → cards, defaults → customers)
# MAGIC - Range checks (credit_score, DPD, amounts, interest_rate)
# MAGIC - Domain checks (gender, card_type, transaction_type, collection_stage, risk_band)
# MAGIC - Null checks on all critical columns
# MAGIC - Cross-table reconciliation counts

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
logger = PipelineLogger(spark, "silver_dq_validation", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load all Silver tables

# COMMAND ----------

task_log = logger.start_task("load_silver_tables")

df_customer    = spark.read.table(SILVER_CUSTOMER_CLEAN)
df_card        = spark.read.table(SILVER_CARD_CLEAN)
df_txn         = spark.read.table(SILVER_TRANSACTION_CLEAN)
df_statement   = spark.read.table(SILVER_STATEMENT_CLEAN)
df_payment     = spark.read.table(SILVER_PAYMENT_CLEAN)
df_default     = spark.read.table(SILVER_DEFAULT_CLEAN)
df_recovery    = spark.read.table(SILVER_RECOVERY_CLEAN)
df_360         = spark.read.table(SILVER_CUSTOMER_360)

logger.complete_task("load_silver_tables", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. PK Uniqueness Checks

# COMMAND ----------

task_log = logger.start_task("pk_checks")

pk_checks = {
    "customer_clean":     check_duplicates(df_customer,  ["customer_id"],    "customer_clean"),
    "card_clean":         check_duplicates(df_card,       ["card_id"],        "card_clean"),
    "transaction_clean":  check_duplicates(df_txn,        ["transaction_id"], "transaction_clean"),
    "statement_clean":    check_duplicates(df_statement,  ["statement_id"],   "statement_clean"),
    "payment_clean":      check_duplicates(df_payment,    ["payment_id"],     "payment_clean"),
    "default_clean":      check_duplicates(df_default,    ["default_id"],     "default_clean"),
    "recovery_clean":     check_duplicates(df_recovery,   ["recovery_id"],    "recovery_clean"),
    "customer_360_view":  check_duplicates(df_360,        ["customer_id"],    "customer_360_view"),
}

print("\n=== PK UNIQUENESS ===")
for table, result in pk_checks.items():
    status = "✅ PASS" if result["passed"] else "❌ FAIL"
    print(f"  {table:<30} dup_groups={result['duplicate_groups']:>6}  {status}")

logger.complete_task("pk_checks", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. FK Integrity Checks

# COMMAND ----------

task_log = logger.start_task("fk_checks")

fk_checks = {
    "card_clean → customer_clean":
        check_fk_validity(df_card,      df_customer,  "customer_id",   "customer_id",   "card_clean"),
    "transaction_clean → card_clean":
        check_fk_validity(df_txn,       df_card,      "card_id",       "card_id",        "transaction_clean"),
    "transaction_clean → customer_clean":
        check_fk_validity(df_txn.dropna(subset=["customer_id"]), df_customer, "customer_id", "customer_id", "transaction_clean"),
    "statement_clean → card_clean":
        check_fk_validity(df_statement, df_card,      "card_id",       "card_id",        "statement_clean"),
    "payment_clean → statement_clean":
        check_fk_validity(df_payment,   df_statement, "statement_id",  "statement_id",   "payment_clean"),
    "default_clean → customer_clean":
        check_fk_validity(df_default,   df_customer,  "customer_id",   "customer_id",    "default_clean"),
    "default_clean → card_clean":
        check_fk_validity(df_default,   df_card,      "card_id",       "card_id",        "default_clean"),
    "recovery_clean → default_clean":
        check_fk_validity(df_recovery,  df_default,   "default_id",    "default_id",     "recovery_clean"),
}

print("\n=== FK INTEGRITY ===")
for rel, result in fk_checks.items():
    status = "✅ PASS" if result["passed"] else f"⚠️  {result['orphan_fk_count']} orphans"
    print(f"  {rel:<45} {status}")

logger.complete_task("fk_checks", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Range Checks

# COMMAND ----------

task_log = logger.start_task("range_checks")

range_checks = {
    "customer.credit_score [300-850]":         check_range(df_customer.dropna(subset=["credit_score"]), "credit_score",      300,   850),
    "customer.annual_income [>0]":             check_range(df_customer.dropna(subset=["annual_income"]), "annual_income",     0.01,  None),
    "card.interest_rate [0-50]":               check_range(df_card.dropna(subset=["interest_rate"]),    "interest_rate",     0,     50),
    "card.credit_limit [>0]":                  check_range(df_card.dropna(subset=["credit_limit"]),     "credit_limit",      0.01,  None),
    "transaction.amount [0.01-1M]":            check_range(df_txn,       "amount",                      0.01,  1_000_000),
    "statement.utilization_ratio [0-10]":      check_range(df_statement.dropna(subset=["utilization_ratio"]), "utilization_ratio", 0, 10),
    "default.days_past_due [0-3650]":          check_range(df_default.dropna(subset=["days_past_due"]), "days_past_due",     0,     3650),
    "default.outstanding_amount [>=0]":        check_range(df_default.dropna(subset=["outstanding_amount"]), "outstanding_amount", 0, None),
}

print("\n=== RANGE CHECKS ===")
for check_name, result in range_checks.items():
    status = "✅ PASS" if result["passed"] else f"❌ {result['violations']} violations"
    print(f"  {check_name:<45} {status}")

logger.complete_task("range_checks", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Domain Checks

# COMMAND ----------

task_log = logger.start_task("domain_checks")

domain_checks = {
    "customer.gender":              check_domain(df_customer, "gender",         ["MALE", "FEMALE", "OTHER"]),
    "card.card_type":               check_domain(df_card,     "card_type",       ["CREDIT", "DEBIT", "PREPAID"]),
    "card.card_network":            check_domain(df_card,     "card_network",    ["VISA", "MASTERCARD", "AMEX"]),
    "card.current_status":          check_domain(df_card,     "current_status",  ["ACTIVE", "BLOCKED", "CLOSED", "SUSPENDED", "UNKNOWN"]),
    "txn.transaction_type":         check_domain(df_txn,      "transaction_type",["PURCHASE", "WITHDRAWAL", "REFUND", "OTHER"]),
    "txn.pos_entry_mode":           check_domain(df_txn,      "pos_entry_mode",  ["CHIP", "SWIPE", "CONTACTLESS", "ONLINE", "UNKNOWN"]),
    "payment.payment_method":       check_domain(df_payment,  "payment_method",  ["ACH", "WIRE", "CHEQUE", "CASH", "CARD", "OTHER"]),
    "default.collection_stage":     check_domain(df_default,  "collection_stage",["EARLY", "MID", "LATE", "LEGAL", "UNKNOWN"]),
    "default.dpd_trend":            check_domain(df_default,  "dpd_trend",       ["FIRST", "WORSENING", "IMPROVING", "STABLE"]),
    "recovery.recovery_status":     check_domain(df_recovery, "recovery_status", ["PENDING", "PARTIAL", "FULL", "UNKNOWN"]),
    "360.risk_band":                check_domain(df_360,      "risk_band",       ["LOW", "MEDIUM", "HIGH"]),
}

print("\n=== DOMAIN CHECKS ===")
for check_name, result in domain_checks.items():
    status = "✅ PASS" if result["passed"] else f"❌ {result['invalid_distinct_count']} invalid values"
    print(f"  {check_name:<45} {status}")

logger.complete_task("domain_checks", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Null Checks on Critical Columns

# COMMAND ----------

task_log = logger.start_task("null_checks")

null_checks = {
    "customer_clean": check_nulls(df_customer, ["customer_id", "email", "credit_score"], "customer_clean"),
    "card_clean":     check_nulls(df_card,     ["card_id", "customer_id", "credit_limit"], "card_clean"),
    "txn_clean":      check_nulls(df_txn,      ["transaction_id", "card_id", "amount"], "transaction_clean"),
    "stmt_clean":     check_nulls(df_statement,["statement_id", "closing_balance"],     "statement_clean"),
    "default_clean":  check_nulls(df_default,  ["default_id", "customer_id", "days_past_due"], "default_clean"),
}

print("\n=== NULL CHECKS ===")
for table, cols in null_checks.items():
    for col, result in cols.items():
        status = "✅ PASS" if result["passed"] else f"❌ {result['null_pct']}% nulls"
        print(f"  {table:<25} {col:<25} {status}")

logger.complete_task("null_checks", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cross-table Row Count Reconciliation

# COMMAND ----------

task_log = logger.start_task("reconciliation")

recon_pairs = [
    (BRONZE_CRM_CUSTOMER,          SILVER_CUSTOMER_CLEAN),
    (BRONZE_CARD_DETAILS,          SILVER_CARD_CLEAN),
    (BRONZE_TXN_TRANSACTIONS,      SILVER_TRANSACTION_CLEAN),
    (BRONZE_BILLING_STATEMENTS,    SILVER_STATEMENT_CLEAN),
    (BRONZE_BILLING_PAYMENTS,      SILVER_PAYMENT_CLEAN),
    (BRONZE_COLLECTIONS_DEFAULTS,  SILVER_DEFAULT_CLEAN),
    (BRONZE_COLLECTIONS_RECOVERY,  SILVER_RECOVERY_CLEAN),
]

print("\n=== BRONZE → SILVER RECONCILIATION ===")
print(f"  {'Bronze Table':<40} {'Bronze':>8} {'Silver':>8} {'Diff':>8} {'Status':>10}")
print("  " + "-" * 78)

for bronze_tbl, silver_tbl in recon_pairs:
    result = reconcile_counts(spark, bronze_tbl, silver_tbl)
    pct_retained = round(result["target_count"] / result["source_count"] * 100, 1) if result["source_count"] > 0 else 0
    status = "✅" if pct_retained >= 90 else "⚠️ "
    bronze_name = bronze_tbl.split(".")[-1]
    print(f"  {bronze_name:<40} {result['source_count']:>8,} {result['target_count']:>8,} {result['difference']:>+8,} {status} {pct_retained}%")

logger.complete_task("reconciliation", task_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

all_checks = {
    **{f"pk_{k}": v for k, v in pk_checks.items()},
    **{f"fk_{k}": v for k, v in fk_checks.items()},
    **{f"range_{k}": v for k, v in range_checks.items()},
    **{f"domain_{k}": v for k, v in domain_checks.items()},
}

total = len(all_checks)
passed = sum(1 for v in all_checks.values() if v.get("passed", False))
failed = total - passed
overall_score = round(passed / total * 100, 1)

print(f"\n{'='*60}")
print(f"SILVER DQ VALIDATION SUMMARY")
print(f"{'='*60}")
print(f"Total checks : {total}")
print(f"Passed       : {passed}")
print(f"Failed       : {failed}")
print(f"Overall Score: {overall_score}%")
print(f"Run ID       : {run_id}")
print(f"{'='*60}")

# Write overall DQ score
record_dq_score(spark, "silver_all_tables", all_checks, "silver_dq_validation", run_id)
