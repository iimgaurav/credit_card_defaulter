# Contributing — Credit Card Defaulter on Community Edition

This project targets **Databricks Community Edition (CE)** — the free tier. CE has specific constraints that shape the development workflow.

## CE Constraints

| Constraint | Impact | Workaround |
|---|---|---|
| Single-node cluster only | No autoscaling, no multi-node | Use `num_workers: 0`, DBR 12.x |
| No Unity Catalog | No three-level namespace | Use Hive metastore (`default.schema.table`) |
| No secret scopes | Can't use `dbutils.secrets` | Use `secrets.json` (git-ignored) loaded via `dbutils.widgets` |
| ~10 GB DBFS storage | Keep artifacts small | Avoid large wheels; use notebooks direct |
| No job scheduling API | Can't create schedules via API | Use "Run Now" UI or simple cron via GH Actions |
| 2-hour job timeout | Long pipelines will be killed | Use `cost_guardrails.check_runtime_limit()` |
| DBR 12.x max | No DBR 13+ features | Pin `spark_version: 12.2.x-scala2.12` |

## Development Flow

```
Fork repo → git clone → make changes → databricks bundle validate -t ce
  → pytest tests/unit/ -v → commit → push → PR
```

### Prerequisites

1. **Databricks CLI** (`>= 0.218.0`) — install via `brew install databricks` or curl
2. **Personal Access Token** — generate in CE workspace: User Settings → Developer → Access Tokens
3. **Python 3.10+** with `pyspark>=3.5.0` for local testing

### Authentication

CE does not support OAuth or service principals. Use a PAT:

```bash
export DATABRICKS_HOST="https://community.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

Store in `secrets.json` (git-ignored) for local development:

```json
{
  "host": "https://community.cloud.databricks.com",
  "token": "dapi...",
  "warehouse_id": ""
}
```

### Local Testing

```bash
# Install package in dev mode
pip install -e .

# Run unit tests (no Databricks connection needed)
pytest tests/unit/ -v

# Check notebook syntax
find notebooks -name '*.py' -exec python -m py_compile {} \;
```

### Bundle Commands (CE)

```bash
# Validate
databricks bundle validate --target ce --strict

# Deploy
databricks bundle deploy --target ce --auto-approve

# Run pipeline
databricks bundle run full-pipeline-job --target ce --refresh-all
```

### Code Style

- 120 char line limit
- Follow existing notebook patterns (`%run` config, PipelineLogger, Watermark, metadata cols)
- All DQ checks must alert but NOT block (schema drift tolerance)
- Every notebook must call `PipelineLogger` for visibility
- Use `upsert_table()` for Delta MERGE, never `mode("overwrite")`
- Tag your commits: `ce:`, `fix:`, `feat:`, `docs:`, `refactor:`

## How to Run the Full Pipeline (One Click)

1. Deploy: `databricks bundle deploy --target ce --auto-approve`
2. Trigger: `databricks bundle run full-pipeline-job --target ce --refresh-all`
3. Monitor: Open the observability dashboard in Databricks SQL
4. Check logs: `SELECT * FROM default.bronze_pipeline_logs ORDER BY logged_at DESC`
