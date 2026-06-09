# Changelog — Credit Card Defaulter Analysis

All infrastructure, pipeline, and configuration changes made during implementation.

---

## [1.0] — 2026-06-06 — Gold Layer Complete

### Feature: Bronze Ingestion Layer (11 Notebooks)
**Files:** `notebooks/01_ingestion/*.py`, `resources/bronze_job.yml`, `resources/bronze_pipeline.yml`

| Task | File | Changes |
|------|------|---------|
| CRM Customer | `crm_customer.py` | Auto Loader from CSV, schema from `schema_registry.py`, metadata columns added, batch_id generated via hash |
| CRM Address | `crm_address.py` | Same pattern as customer, composite PK `(customer_id, address_type)` |
| Card Details | `card_details.py` | Auto Loader from Parquet |
| Card Status | `card_status.py` | Auto Loader from Parquet |
| Transactions | `txn_transactions.py` | Auto Loader from JSON, largest dataset (~500K rows) |
| Billing Statements | `billing_statements.py` | Auto Loader from CSV |
| Billing Payments | `billing_payments.py` | Auto Loader from CSV |
| Collections Defaults | `collections_defaults.py` | **Excel** — uses pandas `read_excel()` batch pattern, not Auto Loader |
| Collections Recovery | `collections_recovery.py` | Same Excel batch pattern |
| Ref Tables | `ref_tables.py` | Loops over 3 ref tables (country, state, currency) + MERGE upsert |
| Calendar | `dim_calendar.py` | Full refresh pattern |
| Bronze Validate | `bronze_validate.py` | Prints row counts, schema presence, null % checks; **no PipelineLogger** |
| Bronze Job | `bronze_job.yml` | DABs job: 11 parallel ingestion tasks + validate |
| Bronze Pipeline | `bronze_pipeline.yml` | Lakeflow SDP pipeline definition |

---

### Feature: Silver Cleansing Layer (7 Transform Notebooks)
**Files:** `notebooks/03_silver/*.py`, `resources/silver_pipeline.yml`

| Task | File | Key Changes |
|------|------|-------------|
| CRM Customer | `silver_crm_customer.py` | Dedup via `row_number()`, gender→M/F/U, credit_score 300-850, email regex, DOB→age, join latest HOME address, MERGE + DELETE CDC |
| Card | `silver_card.py` | Join details + latest status, validate credit_limit>0, cash_limit≤limit, interest_rate 0-50%, standardize card_type/network codes |
| Transactions | `silver_transactions.py` | Filter amount>0, combine date+time→datetime, enrich MCC codes via lookup, resolve customer_id via card |
| Billing | `silver_billing.py` | Validate balance equation, compute `utilization_ratio`, `payment_ratio`, `payment_due_flag`, `days_to_due` |
| Collections | `silver_collections.py` | Lead/lag DPD analysis, `is_repeat_default`, `dormancy_period`, `dpd_trend` classification |
| Enrichment | `silver_enrichment.py` | Full outer join all silver tables→`customer_360_view`, aggregate total_spend, avg_txn, default_count, risk_band |
| DQ Validation | `silver_dq.py` | Runs `run_dq_suite()` with null, duplicate, range, domain checks; records DQ scores |
| Silver Pipeline | `silver_pipeline.yml` | Lakeflow pipeline with 7 notebooks |

**Silver DAG Order:** `crm → card → txn \| billing \| collections → enrichment → dq`

---

### Feature: Gold Dimensional Model (7 Transform + 1 Validate)
**Files:** `notebooks/04_gold/*.py`, `resources/gold_pipeline.yml`

| Task | File | Key Changes |
|------|------|-------------|
| dim_date | `gold_dim_date.py` | Full refresh from `bronze.dim_calendar`, date_sk in YYYYMMDD int format, fiscal calendar |
| dim_geography | `gold_dim_geography.py` | Join `ref_country` + `ref_state` on country_code, rename `state.region`→`state_region` to avoid ambiguous column error |
| dim_customer (SCD2) | `gold_dim_customer.py` | Hash-based change detection on 7 tracked columns, MERGE expire old + insert new, `expiry_date` as SCD2 col |
| dim_card (SCD2) | `gold_dim_card.py` | Same SCD2 pattern, resolve `customer_sk` via `dim_customer` lookup |
| fact_transaction | `gold_fact_transaction.py` | Broadcast join 4 dims (date, geo, customer, card), coalesce miss→-1 sentinel, **removed `.cache()`** (serverless unsupported), join dim_date on `date_sk` INT not `full_date` STRING |
| fact_statement | `gold_fact_statement.py` | Broadcast join 3 dims, join `dim_card` on **`customer_sk`** (not `customer_id`, since dim_card stores the FK directly) |
| fact_default | `gold_fact_default.py` | Broadcast join 3 dims, link recovery records to defaults, coalesce→-1 sentinel |
| gold_validate | `gold_validate.py` | FK checks (11 relationships), SCD2 timeline integrity, sentinel orphan check, **fixed `expiry_col="scd_expiry_date"` param** for dim_card |
| Gold Pipeline | `gold_pipeline.yml` | DABs job with DAG: dims→facts→validate |

**Gold DAG Order:**
```
dim_date ──────────────────────────┐
dim_geography → dim_customer → dim_card ─┬─ fact_transaction ─┐
                                         ├─ fact_statement   ─┤
                                         └─ fact_default     ─┤
                                                              ↓
                                                        gold_validate
```

---

### Issues Resolved During Gold Build

| # | Issue | Root Cause | Fix | File |
|---|-------|-----------|-----|------|
| 1 | `gold_dim_geography` fails — ambiguous `region` | Both `ref_country` and `ref_state` have `region` column | Renamed `state.region` → `state_region` before join | `gold_dim_geography.py:38` |
| 2 | `gold_fact_transaction` fails — `AttributeError: 'DataFrame' object has no attribute 'cache'` | Serverless compute doesn't support `.cache()` | Removed `.cache()`, `.count()`, `.unpersist()` calls | `gold_fact_transaction.py:37-40` |
| 3 | `gold_fact_transaction` fails — `cannot resolve 'full_date'` joining dim_date | `dim_date` has `date_sk` (INT) and `full_date` (DATE), string join mismatch  | Changed join to use `date_sk` (INT) with `F.col("date_sk")` cast | `gold_fact_transaction.py:72` |
| 4 | `gold_fact_statement` fails — `cannot resolve 'customer_id'` in dim_card | `dim_card` stores `customer_sk` FK, not `customer_id` | Changed select to `customer_sk`, joined dim_customer on `customer_sk` | `gold_fact_statement.py:52-62` |
| 5 | `gold_validate` fails — `cannot resolve 'expiry_date'` in dim_card | `dim_card` uses `scd_expiry_date`, not `expiry_date` | Added `expiry_col` parameter to `check_scd2()`, pass `expiry_col="scd_expiry_date"` for dim_card | `gold_validate.py:127,138-143,156` |

---

### Feature: Data Generator
**Files:** `data_generator/*.py`

| File | Purpose |
|------|---------|
| `gen_utils.py` | Shared utilities: random data generation helpers, weighted choices |
| `generate_crm.py` | Generates `customer_master.csv` + `customer_address.csv` (~10K / ~30K rows) |
| `generate_card.py` | Generates `card_details.parquet` + `card_status.parquet` (~15K / ~37K rows) |
| `generate_transactions.py` | Generates `transactions.json` (~500K rows) |
| `generate_billing.py` | Generates `billing_statements.csv` + `billing_payments.csv` (~100K each) |
| `generate_collections.py` | Generates `collections_defaults.xlsx` + `collections_recovery.xlsx` (~5K each) |
| `generate_reference_calendar.py` | Generates all reference tables + calendar (~4K rows) |
| `inject_data_issues.py` | Intentionally injects nulls, duplicates, range violations for DQ testing |
| `upload_to_volume.py` | Uploads all generated files to UC Volume `/Volumes/credit_card_dev/raw/landing/` |
| `validate_generated_data.py` | Validates generated data quality |

---

### Feature: DQ Framework
**File:** `notebooks/00_utilities/dq_framework.py`

| Function | Purpose |
|----------|---------|
| `check_nulls()` | Null % per column vs threshold (default 5%) |
| `check_duplicates()` | PK combination uniqueness |
| `check_full_row_dedup()` | Exact full-row duplicate detection |
| `check_fk_validity()` | Orphan FK detection |
| `check_range()` | Min/max value bounds |
| `check_domain()` | Accepted values list |
| `check_regex()` | Regex pattern validation |
| `move_to_quarantine()` | Write bad rows to `dq_quarantine` table |
| `record_dq_score()` | Persist DQ score to `silver.dq_scores` |
| `run_dq_suite()` | Run declarative list of checks |
| `reconcile_counts()` | Source vs target row count comparison |

---

### Feature: Pipeline Logging
**File:** `notebooks/00_utilities/logger.py`

| Component | Status |
|-----------|--------|
| `PipelineLogger` class | ✅ Implemented — logs to `bronze.pipeline_logs` Delta table |
| Bronze ingestion (11 notebooks) | ✅ All have start/complete per sub-task |
| Bronze validate | ❌ **Missing** — only `print()` statements |
| Silver transforms (7 notebooks) | ✅ All have detailed logging with row counts |
| Gold transforms (8 notebooks) | ✅ All have start/complete + error handling |
| Analytics (5 notebooks) | ❌ **Missing** — only `print()` statements |
| Monitoring (6 notebooks) | ⚠️ Only `maintenance.py` has PipelineLogger |

---

### Feature: Infrastructure & Deployment
**Files:** `databricks.yml`, `resources/*.yml`

| Resource | File | Details |
|----------|------|---------|
| Bundle root | `databricks.yml` | 2 targets: dev, prod; catalog `credit_card_dev`; warehouse `faced73bbff7e9f2` |
| Full pipeline job | `full_pipeline_job.yml` | 30+ task DAG, 5 stages, scheduled 02:00 UTC daily |
| Bronze job | `resources/bronze_job.yml` | 11 parallel ingest + validate |
| Bronze pipeline | `resources/bronze_pipeline.yml` | Lakeflow SDP pipeline |
| Silver pipeline | `resources/silver_pipeline.yml` | Lakeflow SDP pipeline |
| Gold pipeline | `resources/gold_pipeline.yml` | DABs job: dims→facts→validate |
| Monitoring job | `resources/monitoring_job.yml` | DQ + SLA + reconciliation + maintenance |

---

### Removed Files (Cleanup)
**57 debug scripts** deleted from root:
- 40 `_check_*` and `_deploy_*` scripts (one-off debug tools)
- 7 `check_*` scripts
- 4 workspace utility scripts (`push_notebooks.py`, etc.)
- 6 `run_verify_*` scripts

**4 empty directories** deleted: `common/`, `sql/`, `src/`, `logs/`

---

### Known Gaps / Future Work

| Area | Status | Notes |
|------|--------|-------|
| Analytics layer | ⏳ Notebooks exist, not yet run | `05_analytics/*.py` — risk scoring, segmentation, trends |
| Monitoring layer | ⏳ Partially built | `06_monitoring/*.py` — DQ, reconciliation, SLA, alerts |
| Power BI connection | 📅 Planned | Via Direct Lake / DirectQuery through SQL Warehouse |
| SCD2 column consistency | ⚠️ Inconsistency | `dim_customer` uses `expiry_date`, `dim_card` uses `scd_expiry_date` |
| CI/CD promotion | 📅 Planned | Dev→prod promotion via DABs |
| PipelineLogger coverage | ❌ 11 notebooks missing | Analytics (5) + monitoring (5) + bronze_validate (1) |
| Liquid Clustering | 📅 Planned | Not yet applied to `fact_transaction` |
| Weekly maintenance | 📅 Planned | OPTIMIZE + VACUUM in `maintenance.py` |
