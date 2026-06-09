# Databricks notebook source
# MAGIC %md
# MAGIC # Power BI Direct Lake — Connectivity Guide
# MAGIC
# MAGIC This notebook documents how to connect Power BI to the Credit Card Defaulter
# MAGIC gold layer using **Direct Lake** mode (zero-copy, no import, no DirectQuery latency).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC | Requirement | Value |
# MAGIC |---|---|
# MAGIC | Databricks workspace | `dbc-f49d67a0-6a22.cloud.databricks.com` |
# MAGIC | Unity Catalog | `credit_card_dev` |
# MAGIC | SQL Warehouse | `faced73bbff7e9f2` |
# MAGIC | Power BI Desktop | ≥ March 2024 (Direct Lake support) |
# MAGIC | Power BI Premium | P1 / Fabric capacity required for Direct Lake |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Option A — Direct Lake via Microsoft Fabric (recommended)
# MAGIC
# MAGIC Direct Lake reads Delta tables from OneLake/ADLS directly without importing data.
# MAGIC
# MAGIC ### Step 1: Mirror Databricks catalog into Fabric
# MAGIC 1. In Microsoft Fabric workspace → **+ New** → **Mirrored Azure Databricks Catalog**
# MAGIC 2. Enter connection:
# MAGIC    - Server: `dbc-f49d67a0-6a22.cloud.databricks.com`
# MAGIC    - Catalog: `credit_card_dev`
# MAGIC    - Auth: Personal Access Token (PAT) or service principal
# MAGIC 3. Select schemas to mirror: `gold`
# MAGIC 4. Start mirroring → Fabric will continuously sync Delta table changes
# MAGIC
# MAGIC ### Step 2: Create Semantic Model (Direct Lake)
# MAGIC 1. In Fabric workspace → **+ New** → **Semantic model**
# MAGIC 2. Select the mirrored `credit_card_dev` lakehouse
# MAGIC 3. Add tables:
# MAGIC    - `gold.dim_customer` (current only: filter `is_current = true`)
# MAGIC    - `gold.dim_card`
# MAGIC    - `gold.dim_date`
# MAGIC    - `gold.dim_geography`
# MAGIC    - `gold.fact_transaction`
# MAGIC    - `gold.fact_statement`
# MAGIC    - `gold.fact_default_analysis`
# MAGIC    - `gold.analytics_risk_scores`
# MAGIC    - `gold.analytics_customer_segments`
# MAGIC    - `gold.analytics_monthly_trends`
# MAGIC
# MAGIC ### Step 3: Define relationships in semantic model
# MAGIC ```
# MAGIC fact_transaction[customer_sk]  → dim_customer[customer_sk]  (Many:1)
# MAGIC fact_transaction[card_sk]      → dim_card[card_sk]          (Many:1)
# MAGIC fact_transaction[date_sk]      → dim_date[date_sk]          (Many:1)
# MAGIC fact_transaction[geo_sk]       → dim_geography[geo_sk]      (Many:1)
# MAGIC fact_statement[customer_sk]    → dim_customer[customer_sk]  (Many:1)
# MAGIC fact_statement[card_sk]        → dim_card[card_sk]          (Many:1)
# MAGIC fact_statement[statement_date_sk] → dim_date[date_sk]       (Many:1)
# MAGIC fact_default_analysis[customer_sk] → dim_customer[customer_sk] (Many:1)
# MAGIC fact_default_analysis[card_sk]     → dim_card[card_sk]         (Many:1)
# MAGIC fact_default_analysis[default_date_sk] → dim_date[date_sk]     (Many:1)
# MAGIC analytics_risk_scores[customer_id]     → dim_customer[customer_id] (Many:1)
# MAGIC analytics_customer_segments[customer_id] → dim_customer[customer_id] (Many:1)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Option B — DirectQuery via Databricks Connector
# MAGIC
# MAGIC Use when Fabric/Premium is not available.
# MAGIC
# MAGIC 1. Power BI Desktop → **Get Data** → **Databricks**
# MAGIC 2. Enter:
# MAGIC    - Server hostname: `dbc-f49d67a0-6a22.cloud.databricks.com`
# MAGIC    - HTTP path: `/sql/1.0/warehouses/faced73bbff7e9f2`
# MAGIC 3. Auth: **Personal Access Token**
# MAGIC 4. Navigator → expand `credit_card_dev` → `gold` → select tables
# MAGIC 5. **Do not click Import** → choose **DirectQuery**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Recommended DAX Measures
# MAGIC
# MAGIC ```dax
# MAGIC Total Spend = SUM(fact_transaction[amount])
# MAGIC
# MAGIC Avg Transaction Value = AVERAGE(fact_transaction[amount])
# MAGIC
# MAGIC Default Rate % =
# MAGIC     DIVIDE(
# MAGIC         COUNTROWS(FILTER(fact_default_analysis, fact_default_analysis[days_past_due] > 0)),
# MAGIC         DISTINCTCOUNT(dim_customer[customer_id])
# MAGIC     ) * 100
# MAGIC
# MAGIC Avg DQ Score =
# MAGIC     CALCULATE(
# MAGIC         AVERAGE(dq_scores[dq_score]),
# MAGIC         LASTDATE(dq_scores[run_date])
# MAGIC     )
# MAGIC
# MAGIC Recovery Rate % =
# MAGIC     DIVIDE(
# MAGIC         SUM(fact_default_analysis[recovery_amount]),
# MAGIC         SUM(fact_default_analysis[outstanding_amount])
# MAGIC     ) * 100
# MAGIC
# MAGIC High Risk Customers =
# MAGIC     CALCULATE(
# MAGIC         COUNTROWS(analytics_risk_scores),
# MAGIC         analytics_risk_scores[risk_tier] IN {"HIGH", "VERY_HIGH"}
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Suggested Report Pages
# MAGIC
# MAGIC | Page | Key Visuals |
# MAGIC |---|---|
# MAGIC | Executive Summary | Total spend (card), Default rate (KPI), Recovery rate (KPI), MoM trend (line) |
# MAGIC | Customer Risk | Risk tier distribution (donut), Risk score histogram, Top 20 high-risk customers (table) |
# MAGIC | Credit Utilization | Utilization bucket (bar), Avg util by card type (bar), Over-limit cards (KPI) |
# MAGIC | Payment Behavior | Payment segment distribution, Delinquency score scatter, Consecutive late months |
# MAGIC | Default Analysis | DPD trend by month (line), Collection stage funnel, Recovery rate by method |
# MAGIC | Pipeline Health | DQ score trends (line), Pipeline log status (table), SLA breach count (KPI) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Row-Level Security (RLS)
# MAGIC
# MAGIC For production deployments, apply RLS in the semantic model:
# MAGIC
# MAGIC ```dax
# MAGIC -- Example: restrict by state_code for regional managers
# MAGIC [state_code] = USERPRINCIPALNAME()  -- map UPN to state in a security table
# MAGIC ```
# MAGIC
# MAGIC Or configure Unity Catalog row-level security on the Gold tables:
# MAGIC ```sql
# MAGIC -- Row filter on dim_customer
# MAGIC CREATE ROW FILTER credit_card_dev.gold.customer_region_filter
# MAGIC ON credit_card_dev.gold.dim_customer
# MAGIC AS (state_code) -> is_account_group_member('region_' || LOWER(state_code));
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Gold Tables are accessible from SQL Warehouse

# COMMAND ----------

# MAGIC %run ../00_utilities/config

# COMMAND ----------

# MAGIC %run ../00_utilities/logger

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

run_id = str(uuid.uuid4())[:8]
logger = PipelineLogger(spark, "powerbi_connectivity", run_id)

# COMMAND ----------

task_log = logger.start_task("verify_gold_tables")

GOLD_TABLES = {
    "dim_date":              GOLD_DIM_DATE,
    "dim_geography":         GOLD_DIM_GEOGRAPHY,
    "dim_customer":          GOLD_DIM_CUSTOMER,
    "dim_card":              GOLD_DIM_CARD,
    "fact_transaction":      GOLD_FACT_TRANSACTION,
    "fact_statement":        GOLD_FACT_STATEMENT,
    "fact_default_analysis": GOLD_FACT_DEFAULT_ANALYSIS,
    "analytics_risk_scores":        f"{CATALOG}.{GOLD_SCHEMA}.analytics_risk_scores",
    "analytics_customer_segments":  f"{CATALOG}.{GOLD_SCHEMA}.analytics_customer_segments",
    "analytics_monthly_trends":     f"{CATALOG}.{GOLD_SCHEMA}.analytics_monthly_trends",
    "analytics_credit_utilization": f"{CATALOG}.{GOLD_SCHEMA}.analytics_credit_utilization",
    "analytics_payment_behavior":   f"{CATALOG}.{GOLD_SCHEMA}.analytics_payment_behavior",
}

print("GOLD LAYER — POWER BI TABLE INVENTORY")
print(f"{'Table':<40} {'Rows':>10} {'Columns':>8} {'Status'}")
print("=" * 65)

for alias, full_name in GOLD_TABLES.items():
    try:
        df = spark.read.table(full_name)
        cnt  = df.count()
        cols = len(df.columns)
        print(f"  {alias:<40} {cnt:>10,} {cols:>8} ✅")
    except Exception as e:
        print(f"  {alias:<40} {'—':>10} {'—':>8} ❌ {str(e)[:40]}")

print(f"\nWorkspace : {HOST}")
print(f"Catalog   : {CATALOG}")
print(f"Warehouse : {WAREHOUSE_ID}")
logger.complete_task("verify_gold_tables", task_log, row_count=len(GOLD_TABLES))

print(f"\nPower BI connection string:")
print(f"  Server: {HOST}")
print(f"  HTTP Path: /sql/1.0/warehouses/{WAREHOUSE_ID}")
print(f"Run ID: {run_id}")
