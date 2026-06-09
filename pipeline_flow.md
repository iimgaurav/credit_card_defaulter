# Pipeline Flow

## Full Pipeline DAG

The full workflow is defined in `resources/full_pipeline_job.yml`.

```mermaid
flowchart TD
    Start["Scheduled daily<br/>02:00 UTC"] --> BStart

    subgraph Bronze["Stage 1: Bronze ingestion"]
        BStart["11 parallel ingest tasks"]
        B1["ingest_crm_customer"]
        B2["ingest_crm_address"]
        B3["ingest_card_details"]
        B4["ingest_card_status"]
        B5["ingest_transactions"]
        B6["ingest_billing_statements"]
        B7["ingest_billing_payments"]
        B8["ingest_collections_defaults"]
        B9["ingest_collections_recovery"]
        B10["ingest_ref_tables"]
        B11["ingest_dim_calendar"]
        BV["bronze_validate"]
        BStart --> B1 --> BV
        BStart --> B2 --> BV
        BStart --> B3 --> BV
        BStart --> B4 --> BV
        BStart --> B5 --> BV
        BStart --> B6 --> BV
        BStart --> B7 --> BV
        BStart --> B8 --> BV
        BStart --> B9 --> BV
        BStart --> B10 --> BV
        BStart --> B11 --> BV
    end

    BV --> S1

    subgraph Silver["Stage 2: Silver transforms"]
        S1["silver_crm_customer"]
        S2["silver_card"]
        S3["silver_transactions"]
        S4["silver_billing"]
        S5["silver_collections"]
        S6["silver_enrichment"]
        S7["silver_dq"]
        S1 --> S2 --> S3
        S1 --> S4
        S1 --> S5
        S3 --> S6
        S4 --> S6
        S5 --> S6
        S6 --> S7
    end

    S7 --> G1
    S7 --> G2

    subgraph Gold["Stage 3: Gold dimensional model"]
        G1["gold_dim_date"]
        G2["gold_dim_geography"]
        G3["gold_dim_customer"]
        G4["gold_dim_card"]
        G5["gold_fact_transaction"]
        G6["gold_fact_statement"]
        G7["gold_fact_default"]
        G8["gold_validate"]
        G2 --> G3 --> G4
        G1 --> G5
        G2 --> G5
        G3 --> G5
        G4 --> G5
        G1 --> G6
        G3 --> G6
        G4 --> G6
        G1 --> G7
        G3 --> G7
        G4 --> G7
        G5 --> G8
        G6 --> G8
        G7 --> G8
    end

    G8 --> A1
    G8 --> A2
    G8 --> A3

    subgraph Analytics["Stage 4: Analytics"]
        A1["analytics_credit_utilization"]
        A2["analytics_payment_behavior"]
        A3["analytics_monthly_trends"]
        A4["analytics_risk_scoring"]
        A5["analytics_customer_segmentation"]
        A1 --> A4
        A2 --> A4
        A4 --> A5
    end

    A5 --> M1
    A3 --> M1

    subgraph Monitoring["Stage 5: Monitoring"]
        M1["dq_monitoring"]
        M2["reconciliation"]
        M1 --> M2
    end
```

## Execution Characteristics

| Area | Current behavior |
|---|---|
| Scheduling | Full pipeline at 02:00 UTC; monitoring job at 06:00 UTC |
| Concurrency | Full pipeline `max_concurrent_runs: 1` |
| Retries | Many workflow tasks define retries and timeout seconds |
| Bronze | Parallel ingestion followed by validation |
| Silver | Customer first, then card/transaction plus billing and collections branches |
| Gold | Date/geography first, customer/card SCD2 next, facts in parallel, then validation |
| Analytics | Utilization, payment behavior, and trends first; risk score then segmentation |
| Monitoring | DQ monitoring then reconciliation; separate monitoring job also includes SLA and maintenance |

## Current Load Patterns

| Layer | Pattern |
|---|---|
| Bronze streaming-like file ingestion | Auto Loader `availableNow=True`, Delta append, checkpoints |
| Bronze reference tables | Batch read and MERGE |
| Bronze Excel collections | pandas read, Spark DataFrame, Delta MERGE or first overwrite |
| Silver | Mostly full overwrite snapshots with CDF enabled; comments mention MERGE but code often overwrites |
| Gold dimensions | Type 1 full refresh for date/geography; SCD2 MERGE and append for customer/card |
| Gold facts | Full overwrite snapshots |
| Analytics | Full overwrite snapshots |

## Failure and Recovery

Current mechanisms:

- Workflow task retries and timeouts.
- Pipeline logger writes `STARTED`, `COMPLETED`, and `FAILED` events.
- DQ framework can quarantine failed records.
- Auto Loader checkpoints support restart.
- Bronze runner can reset checkpoints when explicitly run.

Recommended additions:

- Parameterized backfill date range.
- Controlled checkpoint reset procedure.
- Per-table watermark integration.
- Dead-letter dashboard for quarantine review.
- Automated rollback/replay playbooks.
- GitHub Actions promotion gates.

