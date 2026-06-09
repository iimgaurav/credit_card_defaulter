# Data Dictionary — Credit Card Defaulter Analysis

**Catalog:** `credit_card_dev`  
**Last Updated:** 2026-06-06

---

## Bronze Layer (`bronze` schema)

### `bronze.crm_customer_master`
| Column | Type | Description |
|---|---|---|
| customer_id | STRING | Unique customer identifier (NK) |
| first_name | STRING | Customer first name |
| last_name | STRING | Customer last name |
| date_of_birth | STRING | DOB in YYYY-MM-DD (raw, unparsed) |
| gender | STRING | Raw gender code: M/F/O |
| marital_status | STRING | Raw code: S/M/D/W |
| email | STRING | Email address (raw) |
| phone_number | STRING | Contact phone (raw) |
| employment_status | STRING | Employment code: EMP/SELF/RET/UNEMP/STU |
| annual_income | DOUBLE | Self-reported income |
| credit_score | INT | Credit bureau score (raw) |
| ingestion_date | DATE | Date file was loaded |
| ingestion_batch_id | STRING | UUID for the ingestion run |
| source_file | STRING | Source file path in UC Volume |
| load_timestamp | TIMESTAMP | Auto Loader processing time |
| _created_at | TIMESTAMP | Row insert timestamp |
| _created_by | STRING | Pipeline identifier |

### `bronze.crm_customer_address`
| Column | Type | Description |
|---|---|---|
| customer_id | STRING | FK → crm_customer_master |
| address_line1 | STRING | Street address |
| city | STRING | City name |
| state_code | STRING | State abbreviation |
| country_code | STRING | ISO 2-letter country code |
| zip_code | STRING | Postal code |
| address_type | STRING | HOME / WORK / BILLING |
| ingestion_date | DATE | Load date |
| load_timestamp | TIMESTAMP | Processing timestamp |

### `bronze.card_details`
| Column | Type | Description |
|---|---|---|
| card_id | STRING | Unique card identifier (NK) |
| customer_id | STRING | FK → crm_customer_master |
| card_type | STRING | Raw: CREDIT/DEBIT/PREPAID |
| card_network | STRING | Raw: VISA/MASTERCARD/AMEX |
| issued_date | STRING | Card issue date (raw) |
| expiry_date | STRING | Card expiry date (raw) |
| credit_limit | DOUBLE | Credit limit amount |
| cash_limit | DOUBLE | Cash advance limit |
| interest_rate | DOUBLE | Annual interest rate % |
| ingestion_date | DATE | Load date |
| load_timestamp | TIMESTAMP | Processing timestamp |

### `bronze.card_status`
| Column | Type | Description |
|---|---|---|
| card_id | STRING | FK → card_details |
| status_code | STRING | ACTIVE/BLOCKED/CLOSED/SUSPENDED |
| status_date | STRING | Status effective date (raw) |
| reason_code | STRING | Reason for status change |

### `bronze.txn_transactions`
| Column | Type | Description |
|---|---|---|
| transaction_id | STRING | Unique transaction identifier (NK) |
| card_id | STRING | FK → card_details |
| transaction_date | STRING | Date of transaction (YYYY-MM-DD) |
| transaction_time | STRING | Time of transaction (HH:MM:SS) |
| merchant_name | STRING | Merchant/retailer name |
| merchant_category | STRING | MCC category code |
| merchant_country | STRING | ISO country code |
| amount | DOUBLE | Transaction amount |
| currency_code | STRING | ISO currency code |
| transaction_type | STRING | PURCHASE/WITHDRAWAL/REFUND |
| pos_entry_mode | STRING | CHIP/SWIPE/CONTACTLESS/ONLINE |

### `bronze.billing_statements`
| Column | Type | Description |
|---|---|---|
| statement_id | STRING | Unique statement ID (NK) |
| card_id | STRING | FK → card_details |
| statement_date | STRING | Statement generation date |
| due_date | STRING | Payment due date |
| opening_balance | DOUBLE | Balance at start of cycle |
| total_purchases | DOUBLE | Purchases this cycle |
| total_payments | DOUBLE | Payments received |
| total_credits | DOUBLE | Credits/refunds |
| interest_charged | DOUBLE | Interest this cycle |
| fees_charged | DOUBLE | Fees this cycle |
| closing_balance | DOUBLE | Balance at end of cycle |
| minimum_due | DOUBLE | Minimum payment required |

### `bronze.billing_payments`
| Column | Type | Description |
|---|---|---|
| payment_id | STRING | Unique payment ID (NK) |
| statement_id | STRING | FK → billing_statements |
| payment_date | STRING | Payment date (raw) |
| payment_amount | DOUBLE | Amount paid |
| payment_method | STRING | ACH/WIRE/CHEQUE/CASH |

### `bronze.collections_defaults`
| Column | Type | Description |
|---|---|---|
| default_id | STRING | Unique default event ID (NK) |
| customer_id | STRING | FK → crm_customer_master |
| card_id | STRING | FK → card_details |
| default_date | STRING | Date declared default |
| days_past_due | INT | DPD at default declaration |
| outstanding_amount | DOUBLE | Balance at default |
| collection_stage | STRING | EARLY/MID/LATE/LEGAL |
| last_contact_date | STRING | Last agent contact date |

### `bronze.collections_recovery`
| Column | Type | Description |
|---|---|---|
| recovery_id | STRING | Unique recovery ID (NK) |
| default_id | STRING | FK → collections_defaults |
| recovery_date | STRING | Recovery transaction date |
| recovery_amount | DOUBLE | Amount recovered |
| recovery_method | STRING | SETTLEMENT/GARNISHMENT/CHARGEOFF |
| recovery_status | STRING | PENDING/PARTIAL/FULL |

### `bronze.pipeline_logs`
| Column | Type | Description |
|---|---|---|
| log_id | STRING | UUID for this log entry |
| run_id | STRING | Pipeline run UUID |
| pipeline_name | STRING | Notebook/pipeline name |
| task_name | STRING | Subtask name |
| status | STRING | STARTED / SUCCESS / FAILED |
| row_count | BIGINT | Rows processed |
| error_message | STRING | Error details if failed |
| started_at | TIMESTAMP | Task start time |
| completed_at | TIMESTAMP | Task completion time |
| duration_secs | DOUBLE | Elapsed seconds |

### `bronze.dq_quarantine`
| Column | Type | Description |
|---|---|---|
| quarantine_table | STRING | Source table of rejected record |
| quarantine_rule | STRING | DQ rule that rejected it |
| quarantine_details | STRING | JSON: check result details |
| quarantine_timestamp | STRING | ISO timestamp |
| quarantine_layer | STRING | bronze / silver |

### `bronze.sla_log`
| Column | Type | Description |
|---|---|---|
| sla_check_id | STRING | UUID |
| check_date | DATE | Date checked |
| pipeline_name | STRING | Pipeline being checked |
| sla_window_utc | STRING | SLA deadline (HH:MM) |
| completed_at | TIMESTAMP | Actual completion (NULL if not run) |
| duration_secs | DOUBLE | Run duration |
| sla_status | STRING | ON_TIME / AT_RISK / BREACHED / NOT_RUN |
| breach_mins | DOUBLE | Minutes past SLA (negative = early) |

---

## Silver Layer (`silver` schema)

### `silver.customer_clean`
| Column | Type | Description |
|---|---|---|
| customer_sk | BIGINT | Surrogate key |
| customer_id | STRING | Natural key (unique) |
| full_name | STRING | `first_name + ' ' + last_name` |
| date_of_birth | DATE | Validated, past dates only |
| gender | STRING | MALE / FEMALE / OTHER |
| marital_status | STRING | MARRIED / SINGLE / DIVORCED / WIDOWED / UNKNOWN |
| email | STRING | Lowercased, regex-validated |
| phone_number | STRING | Digits + `+` only |
| employment_status | STRING | EMPLOYED / SELF_EMPLOYED / RETIRED / UNEMPLOYED / STUDENT |
| annual_income | DOUBLE | Validated > 0 |
| credit_score | INT | Validated 300–850 |
| city | STRING | From address (left join) |
| state_code | STRING | From address |
| country_code | STRING | ISO code |
| zip_code | STRING | Postal code |
| _silver_created_at | TIMESTAMP | Row created timestamp |

### `silver.card_clean`
| Column | Type | Description |
|---|---|---|
| card_sk | BIGINT | Surrogate key |
| card_id | STRING | Natural key |
| customer_id | STRING | FK → customer_clean |
| card_type | STRING | CREDIT / DEBIT / PREPAID |
| card_network | STRING | VISA / MASTERCARD / AMEX |
| issued_date | DATE | Validated |
| expiry_date | DATE | Future dates only |
| credit_limit | DECIMAL(12,2) | Validated > 0 |
| cash_limit | DECIMAL(12,2) | Validated ≤ credit_limit |
| interest_rate | DECIMAL(5,2) | Validated 0–50% |
| current_status | STRING | ACTIVE / BLOCKED / CLOSED / SUSPENDED / UNKNOWN |
| status_reason | STRING | Reason for current status |
| status_effective_date | DATE | When current status took effect |

### `silver.transaction_clean`
| Column | Type | Description |
|---|---|---|
| transaction_sk | BIGINT | Surrogate key |
| transaction_id | STRING | Natural key |
| card_id | STRING | FK → card_clean |
| customer_id | STRING | Enriched from card_clean |
| transaction_datetime | TIMESTAMP | Combined date + time |
| merchant_name | STRING | Title-cased |
| merchant_category_code | STRING | MCC code (uppercased) |
| merchant_category_desc | STRING | Enriched description |
| merchant_country | STRING | ISO country code |
| amount | DECIMAL(12,2) | Absolute value, validated > 0 |
| currency_code | STRING | ISO code |
| transaction_type | STRING | PURCHASE / WITHDRAWAL / REFUND / OTHER |
| pos_entry_mode | STRING | CHIP / SWIPE / CONTACTLESS / ONLINE / UNKNOWN |
| card_type | STRING | Enriched from card_clean |
| card_network | STRING | Enriched from card_clean |

### `silver.statement_clean`
| Column | Type | Description |
|---|---|---|
| statement_sk | BIGINT | Surrogate key |
| statement_id | STRING | Natural key |
| card_id | STRING | FK → card_clean |
| statement_date | DATE | Validated |
| due_date | DATE | Validated > statement_date |
| opening_balance | DECIMAL(12,2) | Validated |
| total_purchases | DECIMAL(12,2) | ≥ 0 |
| total_payments | DECIMAL(12,2) | ≥ 0 |
| total_credits | DECIMAL(12,2) | ≥ 0 |
| interest_charged | DECIMAL(12,2) | ≥ 0 |
| fees_charged | DECIMAL(12,2) | ≥ 0 |
| closing_balance | DECIMAL(12,2) | Rounded to 2dp |
| minimum_due | DECIMAL(12,2) | ≥ 0 |
| payment_due_flag | BOOLEAN | `closing_balance > minimum_due` |
| days_to_due | INT | `datediff(due_date, statement_date)` |
| utilization_ratio | DECIMAL(6,4) | `closing_balance / (purchases + opening)` |
| payment_ratio | DECIMAL(6,4) | `total_payments / closing_balance` |

### `silver.payment_clean`
| Column | Type | Description |
|---|---|---|
| payment_sk | BIGINT | Surrogate key |
| payment_id | STRING | Natural key |
| statement_id | STRING | FK → statement_clean |
| payment_date | DATE | Validated, past only |
| payment_amount | DECIMAL(12,2) | Validated > 0 |
| payment_method | STRING | ACH / WIRE / CHEQUE / CASH / CARD / OTHER |

### `silver.default_clean`
| Column | Type | Description |
|---|---|---|
| default_sk | BIGINT | Surrogate key |
| default_id | STRING | Natural key |
| customer_id | STRING | FK → customer_clean |
| card_id | STRING | FK → card_clean |
| default_date | DATE | Validated past |
| days_past_due | INT | Validated ≥ 0 |
| outstanding_amount | DECIMAL(12,2) | Validated ≥ 0 |
| collection_stage | STRING | EARLY / MID / LATE / LEGAL / UNKNOWN |
| last_contact_date | DATE | Validated |
| is_repeat_default | BOOLEAN | Customer had prior default |
| dormancy_period_days | INT | Days since previous default (NULL for first) |
| default_sequence | INT | Rank of this default per customer |
| next_default_date | DATE | Lead: next default date |
| dpd_trend | STRING | FIRST / WORSENING / IMPROVING / STABLE |

### `silver.recovery_clean`
| Column | Type | Description |
|---|---|---|
| recovery_sk | BIGINT | Surrogate key |
| recovery_id | STRING | Natural key |
| default_id | STRING | FK → default_clean |
| recovery_date | DATE | Validated past |
| recovery_amount | DECIMAL(12,2) | Validated ≥ 0 |
| recovery_method | STRING | SETTLEMENT / GARNISHMENT / CHARGEOFF / OTHER |
| recovery_status | STRING | PENDING / PARTIAL / FULL / UNKNOWN |

### `silver.customer_360_view`
| Column | Type | Description |
|---|---|---|
| customer_id | STRING | Natural key |
| full_name | STRING | Customer name |
| credit_score | INT | Latest credit score |
| annual_income | DOUBLE | Annual income |
| total_cards | LONG | Count of all cards |
| active_cards | LONG | Count of ACTIVE cards |
| total_credit_limit | DOUBLE | Sum of all credit limits |
| total_transactions | LONG | Count of all transactions |
| total_spend | DOUBLE | Sum of PURCHASE amounts |
| avg_txn_amount | DOUBLE | Avg purchase amount |
| avg_closing_balance | DOUBLE | Avg monthly closing balance |
| avg_utilization_ratio | DOUBLE | Avg statement utilization |
| total_defaults | LONG | Count of default events |
| max_days_past_due | INT | Worst DPD on record |
| has_defaulted | BOOLEAN | Any default in history |
| risk_band | STRING | LOW / MEDIUM / HIGH |
| days_since_last_txn | INT | Days since most recent transaction |

### `silver.dq_scores`
| Column | Type | Description |
|---|---|---|
| run_id | STRING | Pipeline run UUID |
| pipeline_name | STRING | Pipeline that ran the check |
| table_name | STRING | Table checked |
| dq_score | DOUBLE | Score 0–100 |
| total_checks | INT | Number of checks run |
| failed_checks | INT | Number that failed |
| check_details | STRING | JSON: per-check results |
| run_date | STRING | YYYY-MM-DD |
| recorded_at | TIMESTAMP | Log timestamp |

---

## Gold Layer (`gold` schema)

### `gold.dim_date`
| Column | Type | Description |
|---|---|---|
| date_sk | INT | Surrogate key (YYYYMMDD) |
| full_date | DATE | Calendar date |
| day_of_week | INT | 1=Sunday, 7=Saturday |
| day_name | STRING | Monday, Tuesday… |
| day_of_month | INT | 1–31 |
| day_of_year | INT | 1–366 |
| week_of_year | INT | 1–53 |
| month_number | INT | 1–12 |
| month_name | STRING | January, February… |
| quarter | INT | 1–4 |
| year | INT | YYYY |
| is_weekend | BOOLEAN | Saturday or Sunday |
| is_holiday | BOOLEAN | Indian public holiday |
| fiscal_year | INT | April-start fiscal year |
| fiscal_quarter | INT | 1–4 (Q1=Apr–Jun) |

### `gold.dim_geography`
| Column | Type | Description |
|---|---|---|
| geo_sk | BIGINT | Surrogate key |
| country_code | STRING | ISO 2-letter code |
| country_name | STRING | Full country name |
| country_region | STRING | Continent/region |
| state_code | STRING | State/province code (UNKNOWN = country-level) |
| state_name | STRING | Full state name |
| region | STRING | State region |
| currency_code | STRING | ISO currency code |

### `gold.dim_customer` (SCD Type 2)
| Column | Type | Description |
|---|---|---|
| customer_sk | BIGINT | Surrogate key (new per version) |
| customer_id | STRING | Natural key |
| full_name | STRING | Name |
| date_of_birth | DATE | DOB |
| age | INT | Computed age |
| gender | STRING | MALE/FEMALE/OTHER |
| marital_status | STRING | Current status |
| email | STRING | Email |
| phone_number | STRING | Phone |
| employment_status | STRING | Employment type |
| annual_income | DECIMAL(12,2) | Income |
| credit_score | INT | Score 300–850 |
| city | STRING | City |
| state_code | STRING | State |
| country_code | STRING | Country |
| geo_sk | BIGINT | FK → dim_geography |
| effective_date | DATE | SCD2: version start |
| expiry_date | DATE | SCD2: version end (9999-12-31 = current) |
| is_current | BOOLEAN | TRUE for active version |
| scd_hash | STRING | MD5 of tracked SCD2 columns |
| _created_at | TIMESTAMP | Row created |
| _modified_at | TIMESTAMP | Row last modified |

**SCD2 tracked columns:** credit_score, annual_income, employment_status, marital_status, city, state_code, country_code

### `gold.dim_card` (SCD Type 2)
| Column | Type | Description |
|---|---|---|
| card_sk | BIGINT | Surrogate key |
| card_id | STRING | Natural key |
| customer_sk | BIGINT | FK → dim_customer (current) |
| card_type | STRING | CREDIT/DEBIT/PREPAID |
| card_network | STRING | VISA/MASTERCARD/AMEX |
| issued_date | DATE | Issue date |
| card_expiry_date | DATE | Card expiry |
| credit_limit | DECIMAL(12,2) | Credit limit |
| cash_limit | DECIMAL(12,2) | Cash limit |
| interest_rate | DECIMAL(5,2) | Interest rate % |
| current_status | STRING | ACTIVE/BLOCKED/CLOSED/SUSPENDED |
| effective_date | DATE | SCD2 version start |
| scd_expiry_date | DATE | SCD2 version end |
| is_current | BOOLEAN | Active version flag |
| scd_hash | STRING | MD5 of tracked columns |

**SCD2 tracked columns:** credit_limit, cash_limit, interest_rate, current_status

### `gold.fact_transaction`
| Column | Type | Description |
|---|---|---|
| txn_sk | BIGINT | Surrogate key |
| transaction_id | STRING | Natural key |
| customer_sk | BIGINT | FK → dim_customer (-1 = unresolved) |
| card_sk | BIGINT | FK → dim_card (-1 = unresolved) |
| date_sk | INT | FK → dim_date |
| geo_sk | BIGINT | FK → dim_geography (merchant country) |
| transaction_datetime | TIMESTAMP | Full timestamp |
| amount | DECIMAL(12,2) | Transaction amount |
| currency_code | STRING | ISO currency |
| merchant_name | STRING | Merchant |
| merchant_category_code | STRING | MCC code |
| merchant_category_desc | STRING | MCC description |
| transaction_type | STRING | PURCHASE/WITHDRAWAL/REFUND/OTHER |
| pos_entry_mode | STRING | Entry method |

### `gold.fact_statement`
| Column | Type | Description |
|---|---|---|
| statement_sk | BIGINT | Surrogate key |
| statement_id | STRING | Natural key |
| customer_sk | BIGINT | FK → dim_customer |
| card_sk | BIGINT | FK → dim_card |
| statement_date_sk | INT | FK → dim_date |
| due_date_sk | INT | FK → dim_date |
| opening_balance | DECIMAL(12,2) | Opening balance |
| total_purchases | DECIMAL(12,2) | Purchases this cycle |
| total_payments | DECIMAL(12,2) | Payments received |
| total_credits | DECIMAL(12,2) | Credits/refunds |
| interest_charged | DECIMAL(12,2) | Interest charged |
| fees_charged | DECIMAL(12,2) | Fees charged |
| closing_balance | DECIMAL(12,2) | Closing balance |
| minimum_due | DECIMAL(12,2) | Minimum payment |
| payment_due_flag | BOOLEAN | Balance > minimum_due |
| days_to_due | INT | Days until payment due |
| utilization_ratio | DECIMAL(6,4) | Credit utilization ratio |
| payment_ratio | DECIMAL(6,4) | Payment / balance ratio |

### `gold.fact_default_analysis`
| Column | Type | Description |
|---|---|---|
| default_sk | BIGINT | Surrogate key |
| default_id | STRING | Natural key |
| customer_sk | BIGINT | FK → dim_customer |
| card_sk | BIGINT | FK → dim_card |
| default_date_sk | INT | FK → dim_date |
| days_past_due | INT | DPD at default |
| outstanding_amount | DECIMAL(12,2) | Balance at default |
| collection_stage | STRING | EARLY/MID/LATE/LEGAL |
| is_repeat_default | BOOLEAN | Prior default exists |
| dormancy_period_days | INT | Days since last default |
| default_sequence | INT | Count of defaults for customer |
| dpd_trend | STRING | FIRST/WORSENING/IMPROVING/STABLE |
| recovery_amount | DECIMAL(12,2) | Total recovered (rolled up) |
| recovery_count | INT | Recovery transactions count |
| recovery_status | STRING | FULL/PARTIAL/PENDING/NO_RECOVERY |
| recovery_rate_pct | DECIMAL(6,2) | `recovery_amount / outstanding_amount × 100` |

---

## Analytics Tables (`gold` schema)

### `gold.analytics_credit_utilization`
Per-card credit utilization snapshot. Updated daily.
Key columns: `utilization_ratio`, `utilization_bucket`, `is_over_limit`, `utilization_risk_flag`

### `gold.analytics_payment_behavior`
Per-card payment behavior aggregates. Key columns: `delinquency_score` (0–100), `payment_segment`, `max_consecutive_late_months`, `late_payment_pct`

### `gold.analytics_risk_scores`
Per-customer weighted risk score (0–100). Key columns: `risk_score`, `risk_tier` (VERY_LOW → VERY_HIGH), `primary_risk_driver`

### `gold.analytics_customer_segments`
RFM × risk segmentation. Key columns: `rfm_score`, `rfm_segment`, `combined_segment`, `is_top_spender`

### `gold.analytics_monthly_trends`
Monthly KPI rollup. Key columns: `total_spend`, `spend_mom_pct`, `default_rate_pct`, `recovery_rate_pct`, `avg_utilization_ratio`, `spend_3m_avg`
