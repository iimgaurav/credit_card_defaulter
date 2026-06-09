# Credit Card Defaulter Analysis

An end-to-end Databricks lakehouse project for credit card default-risk analytics using PySpark, Spark SQL, Delta Lake, Databricks Workflows, and Databricks Asset Bundles.

## What This Project Builds

```text
Synthetic banking source files
  -> Unity Catalog Volume / DBFS landing zone
  -> Bronze raw Delta tables (incremental via watermark)
  -> Silver cleansed and validated tables (upsert)
  -> Gold dimensional model and analytics tables (upsert)
  -> DQ scoring, reconciliation, SLA monitoring
  -> Observability dashboard
```

## Run on Community Edition

This project supports Databricks Community Edition (CE). Here's how to get started:

### 1. Clone & Setup

```bash
git clone <repo-url>
cd credit_card_defaulter
pip install -e .
```

### 2. Configure CE Cluster

Create a single-node cluster in CE:
- **Databricks Runtime:** 12.2.x-scala2.12
- **Node type:** i3.xlarge (or whatever CE provides)
- **Workers:** 0 (single node)
- **Spark config:**
  ```
  spark.databricks.clusterUsageTags.project credit_card_defaulter
  spark.sql.sources.partitionOverwriteMode dynamic
  spark.sql.adaptive.enabled true
  ```

### 3. Generate PAT & Authenticate

```bash
export DATABRICKS_HOST="https://community.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

### 4. Deploy & Run

```bash
# One-click deploy
databricks bundle deploy --target ce --auto-approve

# Run full pipeline
databricks bundle run full-pipeline-job --target ce --refresh-all
```

Or use the `resources/ce_full_pipeline.yml` for the Jobs API.

### 5. View Results

```sql
-- Check pipeline logs
SELECT * FROM default.bronze_pipeline_logs ORDER BY logged_at DESC;

-- Check DQ scores
SELECT * FROM default.silver_dq_scores ORDER BY recorded_at DESC;

-- Check watermarks
SELECT * FROM default.control_watermark;
```

## Architecture

| Layer | Location | Output | Method |
|---|---|---|---|
| Landing | UC Volume / DBFS | CSV, Parquet, JSON, Excel | Auto Loader / Batch |
| Bronze | `bronze` schema | 13 raw Delta tables | Incremental (watermark) |
| Silver | `silver` schema | 7 clean + customer_360 + DQ scores | MERGE upsert |
| Gold | `gold` schema | 4 dims + 3 facts + analytics | MERGE upsert |
| Control | `control` schema | Watermark, schema drift logs | Watermark utility |
| Monitoring | `06_monitoring` | DQ, SLA, reconciliation | DQ framework |

## Key Technical Features

- **Auto Loader** for incremental CSV/Parquet/JSON ingestion with schema evolution
- **Watermark-based incremental loads** — never re-process old data
- **MERGE upsert** pattern — `upsert_table()` helper for idempotent writes
- **SCD Type 2** for `dim_customer` and `dim_card`
- **DQ Framework** — null, duplicate, range, domain, regex, FK, schema drift checks (alert but never block)
- **Pipeline Logger** — structured Delta table for full audit trail
- **Cost Guardrails** — cluster tags + 2-hour runtime watchdog
- **Observability Dashboard** — 6 tiles in Databricks SQL
- **Databricks Asset Bundles** with dev/uat/prod/ce targets

## Commands

```bash
# Validate bundle
databricks bundle validate -t dev --strict

# Deploy
databricks bundle deploy -t dev --auto-approve

# Run pipeline
databricks bundle run full-pipeline-job -t dev --refresh-all

# Unit tests (local)
pytest tests/unit/ -v

# Integration tests (needs Databricks connection)
pytest tests/integration/ -v
```

## Documentation

| File | Purpose |
|---|---|
| `architecture.md` | Complete architecture, diagrams, readiness assessment |
| `DataDictionary.md` | Table-level data dictionary |
| `lineage.md` | Source-to-target lineage |
| `pipeline_flow.md` | Workflow DAG and execution flow |
| `CONTRIBUTING.md` | CE-specific development workflow |
| `observability_dashboard.md` | SQL queries and dashboard layout |
| `docs/` | Advanced diagrams and interview materials |

## Environments

| Target | Catalog | Mode | Purpose |
|---|---|---|---|
| `dev` | `credit_card_dev` | development | Developer sandbox |
| `uat` | `credit_card_uat` | development | Shared test |
| `prod` | `credit_card_prod` | production | Production |
| `ce` | Hive `default` | development | Community Edition |

## Security

- No secrets in code — use `secrets.json` (git-ignored) loaded via `dbutils.widgets`
- PAT-based auth for CI/CD (CE-compatible)
- Cluster tags for cost attribution
- See `schemas/security_rls.sql` for Unity Catalog RLS (UC workspace only)
