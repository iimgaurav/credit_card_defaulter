# Operations Runbook — Credit Card Defaulter Analysis

**Catalog:** `credit_card_dev`  
**Workspace:** `dbc-f49d67a0-6a22.cloud.databricks.com`  
**On-call:** on-call@bank.com | Slack: #data-pipeline-alerts

---

## Pipeline Schedule

| Job | Schedule | SLA | Owner |
|---|---|---|---|
| `full-pipeline-job` | Daily 02:00 UTC | Complete by 05:30 UTC | Data Engineering |
| `monitoring-job` | Daily 06:00 UTC | Complete by 07:00 UTC | Data Engineering |

---

## Normal Trigger Flow

```
02:00 UTC — full-pipeline-job starts
  ├─ Bronze ingestion (parallel, ~45 min)
  ├─ Bronze validate (~5 min)
  ├─ Silver transforms (~30 min)
  ├─ Gold build (~20 min)
  ├─ Analytics (~15 min)
  └─ DQ + Reconciliation (~10 min)
~05:30 UTC — pipeline complete

06:00 UTC — monitoring-job starts
  ├─ DQ monitoring report
  ├─ SLA compliance check
  └─ Row count reconciliation
~06:30 UTC — monitoring complete
```

---

## Manual Trigger

### Run full pipeline manually
```bash
databricks jobs run-now --job-id <full-pipeline-job-id>
```

### Run a single layer
```bash
# Bronze only
databricks jobs run-now --job-id <bronze-ingestion-job-id>

# Silver only
databricks jobs run-now --job-id <silver-transform-job-id>

# Gold only
databricks jobs run-now --job-id <gold-build-job-id>
```

### Run a single notebook ad-hoc
```bash
databricks runs submit --wait \
  --existing-cluster-id <cluster-id> \
  --notebook-task '{"notebook_path": "/path/to/notebook"}'
```

---

## Failure Response Playbook

### Step 1 — Identify the failing task
1. Go to Databricks workspace → **Workflows** → find the failed job run
2. Click the failed task (red) → view logs
3. Check `bronze.pipeline_logs` for error details:
```sql
SELECT pipeline_name, task_name, error_message, started_at
FROM credit_card_dev.bronze.pipeline_logs
WHERE status = 'FAILED'
ORDER BY logged_at DESC
LIMIT 20;
```

---

### Failure: Bronze Ingestion

**Symptoms:** `ingest_*` task fails, red in Databricks job UI  
**Common causes:**
- Source file not arrived in UC Volume → check `/Volumes/credit_card_dev/raw/landing/`
- Schema change in source → check `_rescued_data` column in bronze table
- Auto Loader checkpoint corrupted

**Resolution:**
```bash
# Check files exist
dbutils.fs.ls("/Volumes/credit_card_dev/raw/landing/crm/customer_master/")

# Check _rescued_data for schema issues
SELECT _rescued_data FROM credit_card_dev.bronze.crm_customer_master
WHERE _rescued_data IS NOT NULL LIMIT 10;

# Reset Auto Loader checkpoint (triggers full re-ingest)
dbutils.fs.rm("/Volumes/credit_card_dev/raw/landing/_checkpoints/crm_customer_master", recurse=True)
```

**Rerun:** Restart failed task from Databricks UI (task-level retry) or re-run full job.

---

### Failure: Silver Transform

**Symptoms:** `silver_*` task fails  
**Common causes:**
- Bronze table empty (upstream bronze failure)
- DQ check failures quarantining too many rows
- MERGE conflict on incremental key

**Resolution:**
```sql
-- Check bronze row counts
SELECT 'crm_customer' AS tbl, COUNT(*) FROM credit_card_dev.bronze.crm_customer_master
UNION ALL
SELECT 'card_details', COUNT(*) FROM credit_card_dev.bronze.card_details;

-- Check quarantine volume
SELECT quarantine_table, quarantine_rule, COUNT(*) AS rejected
FROM credit_card_dev.bronze.dq_quarantine
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Check DQ scores
SELECT table_name, dq_score, failed_checks FROM credit_card_dev.silver.dq_scores
WHERE run_date = CURRENT_DATE() ORDER BY dq_score ASC;
```

**Rerun:** Fix root cause → restart failed silver task in job UI.  
**Full silver rerun:** Run `silver-transform-job` independently.

---

### Failure: Gold Build

**Symptoms:** `gold_*` task fails  
**Common causes:**
- Silver table missing or empty
- SCD2 merge collision (duplicate customer_id in source)
- FK resolution producing too many -1 sentinel rows

**Resolution:**
```sql
-- Check silver row counts
SELECT COUNT(*) FROM credit_card_dev.silver.customer_clean;
SELECT COUNT(*) FROM credit_card_dev.silver.card_clean;

-- Check SCD2 integrity
SELECT customer_id, COUNT(*) AS versions
FROM credit_card_dev.gold.dim_customer
WHERE is_current = true
GROUP BY 1 HAVING COUNT(*) > 1;  -- should return 0

-- Check sentinel rate in fact_transaction
SELECT customer_sk, COUNT(*) FROM credit_card_dev.gold.fact_transaction
WHERE customer_sk = -1 GROUP BY 1;
```

**Rerun:** Run `gold-build-job` independently.

---

### Failure: DQ Score Below Threshold

**Symptoms:** Databricks SQL Alert fires — "DQ Score Below 90%"

**Resolution:**
```sql
-- See which tables failed
SELECT * FROM credit_card_dev.silver.alert_dq_below_threshold;

-- Drill into check details
SELECT table_name, check_details, run_date
FROM credit_card_dev.silver.dq_scores
WHERE dq_score < 90 AND run_date = CURRENT_DATE();

-- Check quarantine for that table
SELECT quarantine_rule, COUNT(*) FROM credit_card_dev.bronze.dq_quarantine
WHERE quarantine_table = '<table_name>'
GROUP BY 1;
```

---

### Failure: SLA Breach

**Symptoms:** `sla_monitoring` notebook reports `BREACHED` status

**Response:**
1. Check which pipeline missed its window in `bronze.sla_log`
2. If pipeline ran but was slow: investigate cluster sizing (increase workers in job cluster config)
3. If pipeline didn't run: check job trigger, look for upstream failures
4. Notify business stakeholders if Gold layer is unavailable after 06:00 UTC

```sql
SELECT pipeline_name, sla_window_utc, completed_at, breach_mins, sla_status
FROM credit_card_dev.bronze.sla_log
WHERE check_date = CURRENT_DATE() AND sla_status IN ('BREACHED', 'NOT_RUN')
ORDER BY breach_mins DESC;
```

---

## Rerun Guide

| Scenario | Action |
|---|---|
| Single task failed | Databricks UI → Job run → Repair run (restart from failed task) |
| Full layer needs rerun | Run the layer-specific job (`bronze/silver/gold-build-job`) |
| Data corruption in Bronze | Reset Auto Loader checkpoint → re-ingest |
| Data corruption in Silver | Drop and recreate table → re-run silver job |
| Gold full rebuild | Silver and Gold jobs with `--full-refresh` flag |
| Analytics stale | Re-run `full-pipeline-job` from `gold_validate` task onwards |

---

## Adding New Source Files

1. Add file to UC Volume: `/Volumes/credit_card_dev/raw/landing/<domain>/`
2. Define schema in `notebooks/00_utilities/schema_registry.py`
3. Create ingestion notebook in `notebooks/01_ingestion/`
4. Add notebook path to `bronze_pipeline.yml` and `bronze_job.yml`
5. Add Silver transform notebook in `notebooks/03_silver/`
6. Deploy: `databricks bundle deploy --target dev`

---

## Key Contacts

| Role | Contact |
|---|---|
| On-call Engineer | on-call@bank.com |
| Data Engineering Lead | data-team@bank.com |
| Databricks Admin | databricks-admin@bank.com |
| Slack Channel | #data-pipeline-alerts |

---

## Useful Queries

```sql
-- Latest pipeline run status
SELECT pipeline_name, status, completed_at, duration_secs
FROM credit_card_dev.bronze.pipeline_logs
WHERE logged_at >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY logged_at DESC;

-- Row count trend (last 7 days)
SELECT TO_DATE(logged_at) AS run_date, pipeline_name, SUM(row_count) AS rows
FROM credit_card_dev.bronze.pipeline_logs
WHERE status = 'SUCCESS' AND logged_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY 1, 2 ORDER BY 1 DESC, 3 DESC;

-- DQ score trend
SELECT table_name, run_date, dq_score
FROM credit_card_dev.silver.dq_scores
WHERE run_date >= CURRENT_DATE() - 14
ORDER BY table_name, run_date;

-- _rescued_data check (tables with schema evolution issues)
SELECT table_name, COUNT(*) AS rescued_rows
FROM (
    SELECT _rescued_data, _metadata.file_path AS table_name
    FROM credit_card_dev.bronze.crm_customer_master
    WHERE _rescued_data IS NOT NULL
    UNION ALL
    SELECT _rescued_data, 'crm_customer_address'
    FROM credit_card_dev.bronze.crm_customer_address WHERE _rescued_data IS NOT NULL
    UNION ALL
    SELECT _rescued_data, 'card_details'
    FROM credit_card_dev.bronze.card_details WHERE _rescued_data IS NOT NULL
)
GROUP BY table_name ORDER BY rescued_rows DESC;
```

---

## Data Retention Policy

| Layer | Retention | Method | Rationale |
|-------|-----------|--------|-----------|
| Bronze | 90 days | `DELETE BY ingestion_date` | Raw source data; regulatory requirement |
| Silver | 365 days | `DELETE BY ingestion_date` | Cleaned data; ML feature recomputation window |
| Gold | Indefinite (append-only) | None | Aggregated facts; no PII; analytical queries |
| Pipeline Logs | 90 days | `DELETE BY logged_at` | Operational logs; debugging window |
| DQ Scores | 365 days | `DELETE BY recorded_at` | Trend analysis needs 1 year |

**Retention cleanup command (run in maintenance.py or weekly SQL):**
```sql
-- Bronze: purge records older than 90 days
DELETE FROM credit_card_dev.bronze.crm_customer_master
WHERE ingestion_date < CURRENT_DATE() - INTERVAL 90 DAYS;

-- Pipeline logs: purge older than 90 days
DELETE FROM credit_card_dev.bronze.pipeline_logs
WHERE logged_at < CURRENT_TIMESTAMP() - INTERVAL 90 DAYS;

-- DQ scores: purge older than 365 days
DELETE FROM credit_card_dev.silver.dq_scores
WHERE recorded_at < CURRENT_TIMESTAMP() - INTERVAL 365 DAYS;
```

---
