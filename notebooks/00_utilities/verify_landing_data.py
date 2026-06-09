# Databricks Notebook: Verify Raw Landing Data
# Run this in a Databricks notebook attached to your cluster

# MAGIC %md
# MAGIC # Raw Landing Data Verification
# MAGIC Verify that source files exist at `credit_card_dev.raw.landing`

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

landing_base = LANDING_VOLUME

# MAGIC %md
# MAGIC ## Step 1: List top-level landing contents

# COMMAND ----------

try:
    top = dbutils.fs.ls(landing_base)
    display(spark.createDataFrame([(f.path, f.isDirectory, f.size) for f in top], ["path", "is_dir", "size"]))
except Exception as e:
    print(f"ERROR listing {landing_base}: {e}")
    dbutils.notebook.exit("LANDING_NOT_FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Drill into each domain folder

# COMMAND ----------

domains = ["crm", "card", "txn", "billing", "collections", "ref"]

for domain in domains:
    domain_path = f"{landing_base}/{domain}"
    try:
        files = dbutils.fs.ls(domain_path)
        print(f"\n=== {domain} ({len(files)} items) ===")
        for f in files:
            print(f"  {f.path}  dir={f.isDirectory}  size={f.size}")
    except Exception as e:
        print(f"ERROR: {domain_path} -> {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify specific source files

# COMMAND ----------

expected = {
    "crm/customer_master/customer_master.csv": "CRM Customer Master",
    "crm/customer_address/customer_address.csv": "CRM Customer Address",
    "card/card_details/card_details.parquet": "Card Details",
    "card/card_status/card_status.parquet": "Card Status",
    "txn/transactions/transactions.json": "Transactions",
    "billing/billing_statements/billing_statements.csv": "Billing Statements",
    "billing/billing_payments/billing_payments.csv": "Billing Payments",
    "collections/collections_defaults/collections_defaults.xlsx": "Collections Defaults",
    "collections/collections_recovery/collections_recovery.xlsx": "Collections Recovery",
    "ref/ref_country/ref_country.csv": "Reference Country",
    "ref/ref_state/ref_state.csv": "Reference State",
    "ref/ref_currency/ref_currency.csv": "Reference Currency",
    "ref/dim_calendar/dim_calendar.csv": "Calendar",
}

print("\n=== EXPECTED FILES ===")
for rel_path, name in expected.items():
    full_path = f"{landing_base}/{rel_path}"
    try:
        f = dbutils.fs.ls(full_path)
        print(f"  {name}: {f[0].path}  size={f[0].size}")
    except Exception as e:
        print(f"  {name}: NOT FOUND at {full_path}")
