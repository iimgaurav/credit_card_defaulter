# Banking KPI Glossary — Credit Card Defaulter Analysis

All metrics used in analytics tables, dashboards, and reports.

---

## Credit Utilization KPIs

### Credit Utilization Ratio
**Definition:** Percentage of available credit currently in use.  
**Formula:** `closing_balance / credit_limit`  
**Source:** `gold.analytics_credit_utilization.utilization_ratio`  
**Thresholds:**
- `ZERO`: 0%
- `LOW`: 0–30% — healthy range
- `MODERATE`: 30–70% — acceptable
- `HIGH`: 70–90% — risk signal
- `CRITICAL`: 90–100% — serious risk
- `OVER_LIMIT`: >100% — breach of credit agreement

**Business use:** High utilization predicts default risk. Customers at >70% with low credit scores are flagged `utilization_risk_flag = true`.

---

### Over-Limit Flag
**Definition:** TRUE if closing balance exceeds the card's credit limit.  
**Formula:** `closing_balance > credit_limit`  
**Source:** `gold.analytics_credit_utilization.is_over_limit`  

---

## Payment Behavior KPIs

### Payment Ratio
**Definition:** Proportion of the closing balance paid in a given cycle.  
**Formula:** `total_payments / closing_balance`  
**Source:** `silver.statement_clean.payment_ratio`  
**Interpretation:** 1.0 = full payment; <0.05 = near-zero payment (delinquency risk)

---

### Delinquency Score
**Definition:** Weighted composite score (0–100) measuring payment delinquency risk. Higher = worse.  
**Formula:**
```
delinquency_score =
    (late_payment_pct    × 0.40) +
    (min_only_pct        × 0.20) +
    min(no_payment_count × 10, 30) +
    min(max_consec_late  × 5,  10)
```
**Source:** `gold.analytics_payment_behavior.delinquency_score`  
**Thresholds:** 0–30 = LOW_RISK | 30–60 = MODERATE_RISK | ≥60 = HIGH_RISK

---

### Late Payment %
**Definition:** % of statement cycles where a full payment was not made by the due date.  
**Formula:** `(late_payment_count / total_statements) × 100`  
**Source:** `gold.analytics_payment_behavior.late_payment_pct`

---

### Consecutive Late Months
**Definition:** Longest streak of consecutive billing cycles without a full payment.  
**Source:** `gold.analytics_payment_behavior.max_consecutive_late_months`  
**Regulatory note:** 90+ consecutive DPD typically triggers charge-off classification.

---

### Payment Classification
| Class | Definition |
|---|---|
| FULL | `total_payments >= closing_balance` |
| PARTIAL | `total_payments > minimum_due AND < closing_balance` |
| MIN_ONLY | `0 < total_payments <= minimum_due` |
| NO_PAYMENT | `total_payments = 0` |

---

## Default Risk KPIs

### Days Past Due (DPD)
**Definition:** Number of calendar days a payment obligation remains unpaid after the due date.  
**Source:** `silver.default_clean.days_past_due`, `gold.fact_default_analysis.days_past_due`  
**Industry buckets:**
- DPD 1–30: Early delinquency
- DPD 31–60: Mid delinquency
- DPD 61–90: Late delinquency
- DPD 90+: Non-performing / charge-off territory

---

### Risk Score
**Definition:** Weighted composite score (0–100) predicting default probability. Higher = higher risk.  
**Formula:**
```
risk_score =
    score_credit_score  × 0.25   -- (850 - credit_score) / 5.5
    score_utilization   × 0.20   -- max utilization ratio scaled 0-100
    score_delinquency   × 0.20   -- delinquency_score
    score_defaults      × 0.20   -- 0→0, 1→40, 2→70, 3+→100
    score_consec_late   × 0.10   -- consecutive_months × 16.7, capped 100
    score_income        × 0.05   -- <100K→50, <200K→25, ≥200K→0
```
**Source:** `gold.analytics_risk_scores.risk_score`  
**Tiers:** VERY_LOW (<15) | LOW (15–35) | MEDIUM (35–55) | HIGH (55–75) | VERY_HIGH (≥75)

---

### Recovery Rate
**Definition:** % of defaulted outstanding balance recovered through collections.  
**Formula:** `recovery_amount / outstanding_amount × 100`  
**Source:** `gold.fact_default_analysis.recovery_rate_pct`  
**Benchmarks:** >60% = strong recovery; <20% = write-off candidate

---

### Default Rate
**Definition:** % of active customers with at least one default event in a period.  
**Formula:** `defaulted_customers / active_customers × 100`  
**Source:** `gold.analytics_monthly_trends.default_rate_pct`

---

### Repeat Default
**Definition:** A customer who has more than one default event on record.  
**Source:** `silver.default_clean.is_repeat_default`  
**Business use:** Repeat defaulters are weighted more heavily in risk scoring.

---

## Customer Segmentation KPIs

### RFM Score
**Definition:** Composite score from Recency (days since last purchase), Frequency (transaction count), Monetary (total spend). Each dimension scored 1–5 via quintile bucketing.  
**Formula:** `rfm_score = r_score + f_score + m_score` (range 3–15)  
**Source:** `gold.analytics_customer_segments`

### RFM Segments
| Segment | Criteria | Action |
|---|---|---|
| CHAMPIONS | R≥4, F≥4, M≥4 | Reward, upsell |
| LOYAL | F≥4, M≥3 | Nurture loyalty |
| POTENTIAL_LOYALIST | R≥3, F≥3 | Engage actively |
| AT_RISK | R≤2, F≥3, M≥3 | Win-back campaign |
| HIBERNATING | R≤2, F≤2 | Reactivation offer |
| NEW_CUSTOMER | R≥4, F≤2 | Onboard well |
| LOST | rfm_score ≤ 5 | Low priority |

### Combined Segment (RFM × Risk)
| Segment | Definition |
|---|---|
| HIGH_VALUE_LOW_RISK | Champions/Loyal + LOW/MEDIUM risk |
| HIGH_VALUE_HIGH_RISK | Champions/Loyal + HIGH/VERY_HIGH risk |
| AT_RISK | RFM At-Risk segment |
| DEFAULT_PRONE | Any segment + HIGH/VERY_HIGH risk tier |
| DORMANT | Hibernating segment |

---

## Billing & Statement KPIs

### Minimum Due
**Definition:** The minimum amount a cardholder must pay to avoid a late fee.  
**Typical formula (industry):** `max(minimum_floor, closing_balance × minimum_rate)`  
**Source:** `silver.statement_clean.minimum_due`

### Days to Due
**Definition:** Number of days between statement generation date and payment due date.  
**Formula:** `datediff(due_date, statement_date)`  
**Source:** `silver.statement_clean.days_to_due`  
**Typical value:** 20–25 days (regulatory minimum in most markets)

---

## Trend KPIs

### Month-over-Month (MoM) Spend Growth
**Definition:** % change in total spend vs prior month.  
**Formula:** `(current_month_spend - prior_month_spend) / prior_month_spend × 100`  
**Source:** `gold.analytics_monthly_trends.spend_mom_pct`

### 3-Month Rolling Average Spend
**Definition:** Smoothed spend trend using 3-month rolling window.  
**Formula:** `AVG(total_spend) OVER (ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`  
**Source:** `gold.analytics_monthly_trends.spend_3m_avg`
