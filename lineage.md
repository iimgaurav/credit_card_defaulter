# Data Lineage

## Source to Bronze

```mermaid
flowchart LR
    CRM1["crm/customer_master.csv"] --> B1["bronze.crm_customer_master"]
    CRM2["crm/customer_address.csv"] --> B2["bronze.crm_customer_address"]
    CARD1["card/card_details.parquet"] --> B3["bronze.card_details"]
    CARD2["card/card_status.parquet"] --> B4["bronze.card_status"]
    TXN["txn/transactions.json"] --> B5["bronze.txn_transactions"]
    BS["billing/billing_statements.csv"] --> B6["bronze.billing_statements"]
    BP["billing/billing_payments.csv"] --> B7["bronze.billing_payments"]
    CD["collections/collections_defaults.xlsx"] --> B8["bronze.collections_defaults"]
    CR["collections/collections_recovery.xlsx"] --> B9["bronze.collections_recovery"]
    RC["ref/ref_country.csv"] --> B10["bronze.ref_country"]
    RS["ref/ref_state.csv"] --> B11["bronze.ref_state"]
    RCU["ref/ref_currency.csv"] --> B12["bronze.ref_currency"]
    CAL["ref/dim_calendar.csv"] --> B13["bronze.dim_calendar"]
```

## Bronze to Silver

```mermaid
flowchart LR
    B1["bronze.crm_customer_master"] --> S1["silver.customer_clean"]
    B2["bronze.crm_customer_address"] --> S1
    B3["bronze.card_details"] --> S2["silver.card_clean"]
    B4["bronze.card_status"] --> S2
    B5["bronze.txn_transactions"] --> S3["silver.transaction_clean"]
    S2 --> S3
    B6["bronze.billing_statements"] --> S4["silver.statement_clean"]
    B7["bronze.billing_payments"] --> S5["silver.payment_clean"]
    B8["bronze.collections_defaults"] --> S6["silver.default_clean"]
    B9["bronze.collections_recovery"] --> S7["silver.recovery_clean"]
    S1 --> S8["silver.customer_360_view"]
    S2 --> S8
    S3 --> S8
    S4 --> S8
    S5 --> S8
    S6 --> S8
    S7 --> S8
```

## Silver to Gold

```mermaid
flowchart LR
    B13["bronze.dim_calendar"] --> D1["gold.dim_date"]
    B10["bronze.ref_country"] --> D2["gold.dim_geography"]
    B11["bronze.ref_state"] --> D2
    B12["bronze.ref_currency"] --> D2
    S1["silver.customer_clean"] --> D3["gold.dim_customer SCD2"]
    D2 --> D3
    S2["silver.card_clean"] --> D4["gold.dim_card SCD2"]
    D3 --> D4
    S3["silver.transaction_clean"] --> F1["gold.fact_transaction"]
    D1 --> F1
    D2 --> F1
    D3 --> F1
    D4 --> F1
    S4["silver.statement_clean"] --> F2["gold.fact_statement"]
    D1 --> F2
    D3 --> F2
    D4 --> F2
    S6["silver.default_clean"] --> F3["gold.fact_default_analysis"]
    S7["silver.recovery_clean"] --> F3
    D1 --> F3
    D3 --> F3
    D4 --> F3
```

## Gold to Analytics and Power BI

```mermaid
flowchart LR
    F1["fact_transaction"] --> A1["analytics_credit_utilization"]
    F2["fact_statement"] --> A1
    F2 --> A2["analytics_payment_behavior"]
    F3["fact_default_analysis"] --> A2
    F1 --> A3["analytics_risk_scores"]
    F2 --> A3
    F3 --> A3
    A1 --> A3
    A2 --> A3
    A3 --> A4["analytics_customer_segments"]
    F1 --> A5["analytics_monthly_trends"]
    F2 --> A5
    F3 --> A5
    A1 --> PBI["Power BI reports"]
    A2 --> PBI
    A3 --> PBI
    A4 --> PBI
    A5 --> PBI
```

## Dependency Notes

- `silver_card` depends on `silver_crm_customer` in the workflow.
- `silver_transactions` depends on `silver_card` because transactions are enriched with card/customer mapping.
- `silver_enrichment` depends on transactions, billing, and collections.
- `gold_dim_customer` depends on `gold_dim_geography`.
- `gold_dim_card` depends on `gold_dim_customer`.
- Gold facts depend on all required current dimensions.
- Monitoring depends on analytics completion in the full pipeline job.

