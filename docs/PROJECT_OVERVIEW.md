# Credit Card Defaulter — Project Overview

## Goal
Predict customer default risk on credit card portfolios using a medallion-architected data pipeline on Databricks.

## Stack
**Databricks** (AWS, Serverless) + **Unity Catalog** + **Delta Lake** + **PySpark** + **DABs**

## Medallion Layers (credit_card_dev)

| Layer | Schema | Tables | Status |
|-------|--------|--------|--------|
| 🟫 Bronze | `bronze` | 15 (13 source + dq_quarantine + pipeline_logs) | ✅ Verified |
| 🥈 Silver | `silver` | 8 (cleansed entities + customer_360 + dq_scores) | ✅ Verified |
| 🥇 Gold | `gold` | 7 (4 dims + 3 facts) | ✅ Verified |
| ⚙️ Control | `control` | 1 (watermark) | ✅ |

## Gold Star Schema

### Dimensions
- `dim_date` (date_sk, full_date, day, month, year, quarter, is_weekend, fiscal)
- `dim_geography` (geo_sk, country, state, region, currency)
- `dim_customer` — **SCD2** (customer_sk, credit_score, income, geo_sk, expiry_date)
- `dim_card` — **SCD2** (card_sk, customer_sk, limit, rate, status, expiry_date)

### Facts
- `fact_transaction` (~500K rows) — 4 FK → all dims
- `fact_statement` (~100K rows) — 3 FK → customer, card, date
- `fact_default_analysis` (~5K rows) — 3 FK → customer, card, date + recovery linkage

## Key Commands

```bash
# Deploy
databricks bundle deploy -t dev --profile DEFAULT

# Validate
databricks bundle validate -t dev --profile DEFAULT

# Run full pipeline
databricks bundle run full-pipeline-job -t dev --profile DEFAULT

# Run individual stage
databricks bundle run gold-build-job -t dev --profile DEFAULT

# Check tables
databricks tables list credit_card_dev gold --profile DEFAULT
```

## Pipeline Structure (full-pipeline-job)

```
Bronze (11 parallel ingest) → validate
  → Silver (crm → card → txn / billing / collections → enrichment → dq)
    → Gold (dims → facts → validate)
      → Analytics (utilization / payment / trends → risk → segmentation)
        → Monitoring (dq → reconciliation)
```

**Schedule:** Daily 02:00 UTC (Quartz cron)

## Build Issues Resolved

| Issue | Fix |
|-------|-----|
| Serverless + `.cache()` | Removed cache, used broadcast joins |
| `dim_geography` ambiguous `region` | Renamed `state.region` → `state_region` |
| `dim_card` lacks `customer_id` | Join on `customer_sk` FK directly |
| `dim_date` STRING vs INT join | Joined on `date_sk` (INT, yyyyMMdd) |
| `gold_validate` `expiry_date` not found | Added `expiry_col` param for dim_card; standardized column to `expiry_date` |

## Next Steps
1. Run Analytics notebooks (05_analytics/) for risk scoring & segmentation
2. Run Monitoring notebooks (06_monitoring/) for DQ/SLA tracking
3. Connect Power BI via Direct Lake / DirectQuery
4. Set up CI/CD for dev→prod promotion
