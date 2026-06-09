# Observability Dashboard — Databricks SQL

A single Databricks SQL dashboard that reads three Delta tables to provide full observability of the pipeline.

## Tables Referenced

| Table | Catalog.Schema | Source |
|---|---|---|
| `pipeline_logs` | `credit_card_dev.bronze.pipeline_logs` | Written by `PipelineLogger` |
| `dq_scores` | `credit_card_dev.silver.dq_scores` | Written by `record_dq_score()` |
| `sla_log` | `credit_card_dev.control.sla_log` | Written by `sla_monitoring.py` |

## Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  HEADER: Credit Card Defaulter — Pipeline Observability         │
│  Filters: [Date Range] [Pipeline Name] [Status]                 │
├──────────────────────────┬──────────────────────────────────────┤
│  Tile 1: Pipeline Health  │  Tile 2: DQ Score Trend             │
│  (KPI cards)              │  (Line chart, 30d)                  │
│  ┌──────┬──────┬──────┐  │                                      │
│  │ Total│Passed│Failed│  │  ▁▃▅▇▅▃▁ ████████████                │
│  │ 1,234│ 1,200│   34 │  │                                      │
│  └──────┴──────┴──────┘  │                                      │
├──────────────────────────┴──────────────────────────────────────┤
│  Tile 3: Task Duration (bar chart, last 10 runs)               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ ████████████ bronze_validate                         12s │   │
│  │ ████████████████████ silver_crm_customer              22s │   │
│  │ ████████████████████████████ gold_fact_transaction    35s │   │
│  └─────────────────────────────────────────────────────────┘   │
├──────────────────────────┬──────────────────────────────────────┤
│  Tile 4: Failed Tasks    │  Tile 5: SLA Compliance             │
│  (table, last 24h)       │  (gauge, % within SLA)             │
│  ┌───────────────────┐   │  ┌──────────────────────────┐       │
│  │ Task  │ Error     │   │  │ 98.5%                    │       │
│  │ ingest│ Null ptr  │   │  │ within 120min SLA        │       │
│  └───────────────────┘   │  └──────────────────────────┘       │
├──────────────────────────┴──────────────────────────────────────┤
│  Tile 6: DQ Failures by Table (heatmap, last 7d)               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Table          │ Null │ Dup │ Range │ Domain │ Total    │   │
│  │ crm_customer   │  2   │  0  │   1   │   0    │   3     │   │
│  │ txn_transactions│  5  │  0  │   0   │   0    │   5     │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## SQL Queries

### Tile 1: Pipeline Health (KPI Cards)

```sql
-- Total runs today
SELECT COUNT(DISTINCT run_id) AS total_runs
FROM credit_card_dev.bronze.pipeline_logs
WHERE logged_at >= CURRENT_DATE();

-- Passed tasks today
SELECT COUNT(*) AS passed_tasks
FROM credit_card_dev.bronze.pipeline_logs
WHERE logged_at >= CURRENT_DATE()
  AND status = 'COMPLETED';

-- Failed tasks today
SELECT COUNT(*) AS failed_tasks
FROM credit_card_dev.bronze.pipeline_logs
WHERE logged_at >= CURRENT_DATE()
  AND status = 'FAILED';
```

### Tile 2: DQ Score Trend (Last 30 Days)

```sql
SELECT
  run_date,
  ROUND(AVG(dq_score), 1) AS avg_dq_score,
  MIN(dq_score) AS min_dq_score,
  MAX(dq_score) AS max_dq_score
FROM credit_card_dev.silver.dq_scores
WHERE run_date >= DATE_ADD(CURRENT_DATE(), -30)
GROUP BY run_date
ORDER BY run_date;
```

### Tile 3: Task Duration (Last 10 Runs)

```sql
SELECT
  pipeline_name,
  task_name,
  ROUND(AVG(duration_secs), 1) AS avg_duration_secs
FROM credit_card_dev.bronze.pipeline_logs
WHERE status = 'COMPLETED'
  AND logged_at >= DATE_ADD(CURRENT_DATE(), -7)
GROUP BY pipeline_name, task_name
ORDER BY avg_duration_secs DESC;
```

### Tile 4: Failed Tasks (Last 24 Hours)

```sql
SELECT
  logged_at,
  pipeline_name,
  task_name,
  error_message
FROM credit_card_dev.bronze.pipeline_logs
WHERE status = 'FAILED'
  AND logged_at >= DATE_ADD(CURRENT_DATE(), -1)
ORDER BY logged_at DESC;
```

### Tile 5: SLA Compliance

```sql
-- Pipeline SLA: 120 minutes (2 hours)
WITH pipeline_runs AS (
  SELECT
    run_id,
    MIN(logged_at) AS started_at,
    MAX(logged_at) AS completed_at
  FROM credit_card_dev.bronze.pipeline_logs
  WHERE logged_at >= DATE_ADD(CURRENT_DATE(), -30)
  GROUP BY run_id
)
SELECT
  COUNT(*) AS total_runs,
  SUM(CASE
    WHEN TIMESTAMPDIFF(MINUTE, started_at, completed_at) <= 120 THEN 1
    ELSE 0
  END) AS within_sla,
  ROUND(
    SUM(CASE
      WHEN TIMESTAMPDIFF(MINUTE, started_at, completed_at) <= 120 THEN 1
      ELSE 0
    END) * 100.0 / COUNT(*), 1
  ) AS sla_compliance_pct
FROM pipeline_runs;
```

### Tile 6: DQ Failures by Table (Last 7 Days)

```sql
SELECT
  table_name,
  SUM(CASE WHEN check_details LIKE '%null%FAILED%' THEN 1 ELSE 0 END) AS null_failures,
  SUM(CASE WHEN check_details LIKE '%duplicate%FAILED%' THEN 1 ELSE 0 END) AS dup_failures,
  SUM(CASE WHEN check_details LIKE '%range%FAILED%' THEN 1 ELSE 0 END) AS range_failures,
  SUM(CASE WHEN check_details LIKE '%domain%FAILED%' THEN 1 ELSE 0 END) AS domain_failures,
  COUNT(*) AS total_checks,
  ROUND(AVG(dq_score), 1) AS avg_dq_score
FROM credit_card_dev.silver.dq_scores
WHERE run_date >= DATE_ADD(CURRENT_DATE(), -7)
GROUP BY table_name
ORDER BY avg_dq_score;
```

## How to Create the Dashboard

1. Open Databricks SQL Editor in your workspace
2. Create a new dashboard named `Credit Card Defaulter — Pipeline Observability`
3. Add each SQL query as a separate visualization tile
4. Pin the dashboard to your workspace home folder
5. Set auto-refresh to every 15 minutes
6. Share with stakeholders via the dashboard URL (read-only access)

## Alerting (via Databricks SQL Alerts)

Create SQL alerts on these queries:

| Alert | Query | Threshold | Action |
|---|---|---|---|
| Pipeline Failure | `SELECT COUNT(*) FROM ... WHERE status='FAILED' AND logged_at >= DATE_ADD(CURRENT_DATE(), -1)` | > 0 | Email on-call |
| DQ Score Drop | `SELECT AVG(dq_score) FROM ... WHERE run_date = CURRENT_DATE()` | < 90 | Email data team |
| SLA Breach | Tile 5 query | < 95% | Email on-call |
