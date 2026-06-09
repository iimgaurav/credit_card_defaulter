# Data Dictionary

This dictionary summarizes the implemented tables discovered from `schemas/*.py`, `config.py`, and transformation notebooks.

## Bronze Tables

| Table | Source | Format | Key columns | Notes |
|---|---|---|---|---|
| `bronze.crm_customer_master` | CRM generator | CSV | `customer_id` | Raw customer identity, demographics, income, credit score |
| `bronze.crm_customer_address` | CRM generator | CSV | `customer_id`, `address_type` | Raw address data |
| `bronze.card_details` | Card generator | Parquet | `card_id` | Card product, limits, issue/expiry dates |
| `bronze.card_status` | Card generator | Parquet | `card_id`, `status_date` | Status history |
| `bronze.txn_transactions` | Transaction generator | JSON | `transaction_id` | Transaction events |
| `bronze.billing_statements` | Billing generator | CSV | `statement_id` | Monthly statement balances |
| `bronze.billing_payments` | Billing generator | CSV | `payment_id` | Statement payments |
| `bronze.collections_defaults` | Collections generator | Excel | `default_id` | Default events |
| `bronze.collections_recovery` | Collections generator | Excel | `recovery_id` | Recovery events |
| `bronze.ref_country` | Reference generator | CSV | `country_code` | Country reference |
| `bronze.ref_state` | Reference generator | CSV | `state_code`, `country_code` | State/region reference |
| `bronze.ref_currency` | Reference generator | CSV | `currency_code` | Currency reference |
| `bronze.dim_calendar` | Calendar generator | CSV | `date_key` | Calendar source for date dimension |
| `bronze.dq_quarantine` | DQ framework | Delta | generated | Quarantined bad records |
| `bronze.pipeline_logs` | Logger | Delta | `run_id`, `pipeline_name`, `task_name` | Audit and execution logs |

Common Bronze metadata:

- `ingestion_date`
- `ingestion_batch_id`
- `source_file`
- `load_timestamp`
- `_created_at`
- `_created_by`

## Silver Tables

| Table | Source | Grain | Major transformations |
|---|---|---|---|
| `silver.customer_clean` | `crm_customer_master`, `crm_customer_address` | One current row per customer | Latest-row dedup, gender/marital/employment standardization, email regex, DOB validation, phone cleanup, credit score range, address enrichment |
| `silver.card_clean` | `card_details`, `card_status` | One current row per card | Latest card/status selection, type/network standardization, date parsing, credit/cash limit validation, interest-rate validation |
| `silver.transaction_clean` | `txn_transactions`, `card_clean` | One row per transaction | Date/time timestamp, amount decimal, merchant and currency cleanup, transaction type and POS standardization, MCC enrichment, customer enrichment |
| `silver.statement_clean` | `billing_statements` | One row per statement | Date validation, amount validation, closing balance, payment due flag, days to due, utilization ratio, payment ratio |
| `silver.payment_clean` | `billing_payments` | One row per payment | Payment date validation, amount decimal, payment method standardization |
| `silver.default_clean` | `collections_defaults` | One row per default event | Date validation, DPD validation, stage standardization, repeat default, dormancy period, default sequence, DPD trend |
| `silver.recovery_clean` | `collections_recovery` | One row per recovery event | Date validation, recovery amount validation, method/status standardization |
| `silver.customer_360_view` | All Silver clean tables | One row per customer | Aggregates cards, transactions, billing, defaults, risk band |
| `silver.dq_scores` | DQ framework | One row per DQ run/table | DQ score, check details, run metadata |

Silver tables set Delta Change Data Feed where implemented by notebooks.

## Gold Dimensions

| Table | Type | Grain | Natural key | Surrogate key | Notes |
|---|---|---|---|---|---|
| `gold.dim_date` | Type 1/static | One row per day | `full_date` / `date_key` | `date_sk` | Calendar attributes and fiscal fields |
| `gold.dim_geography` | Type 1 | One row per country/state | `country_code`, `state_code` | `geo_sk` | Includes unknown geography fallback |
| `gold.dim_customer` | SCD Type 2 | One row per customer version | `customer_id` | `customer_sk` | Tracks credit score, income, employment, marital status, city, state, country |
| `gold.dim_card` | SCD Type 2 | One row per card version | `card_id` | `card_sk` | Tracks credit limit, cash limit, interest rate, current status |

SCD2 columns:

| Table | Effective column | Expiry column | Current flag | Hash |
|---|---|---|---|---|
| `gold.dim_customer` | `effective_date` | `expiry_date` | `is_current` | `scd_hash` |
| `gold.dim_card` | `effective_date` | `expiry_date` | `is_current` | `scd_hash` |

## Gold Facts

| Table | Grain | Source | Foreign keys | Measures |
|---|---|---|---|---|
| `gold.fact_transaction` | One transaction | `silver.transaction_clean` | `customer_sk`, `card_sk`, `date_sk`, `geo_sk` | `amount` |
| `gold.fact_statement` | One statement | `silver.statement_clean` | `customer_sk`, `card_sk`, `statement_date_sk`, `due_date_sk` | balances, purchases, payments, credits, interest, fees, utilization ratio, payment ratio |
| `gold.fact_default_analysis` | One default event | `silver.default_clean`, `silver.recovery_clean` | `customer_sk`, `card_sk`, `default_date_sk` | days past due, outstanding amount, recovery amount, recovery rate |

Unknown or unresolved foreign keys use sentinel values such as `-1` or unknown dimension rows where implemented.

## Analytics Tables

| Table | Notebook | Purpose |
|---|---|---|
| `gold.analytics_credit_utilization` | `credit_utilization.py` | Utilization ratio, utilization buckets, over-limit flags |
| `gold.analytics_payment_behavior` | `payment_behavior.py` | Payment classification, late payment behavior, delinquency score |
| `gold.analytics_risk_scores` | `default_risk_scoring.py` | Composite default risk score and risk tier |
| `gold.analytics_customer_segments` | `customer_segmentation.py` | RFM-style customer segmentation |
| `gold.analytics_monthly_trends` | `monthly_trends.py` | MTD, QTD, YTD, month-over-month trends |

