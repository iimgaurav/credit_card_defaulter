# PySpark Transformation Guide — Credit Card Defaulter Analysis

Catalogue of every PySpark transformation pattern used in this project, with before/after examples.

---

## 1. Deduplication — Keep Latest Record (Row Number over Window)

**Used in:** `silver_crm_customer.py`, `silver_card.py`  
**Purpose:** Remove duplicate records, keeping the most recent version per key.

```python
from pyspark.sql import Window, functions as F

# BEFORE: crm_customer_master has duplicate customer_ids (injected data issues)
# customer_id | first_name | load_timestamp
# C001        | Alice       | 2026-01-01
# C001        | Alicia      | 2026-01-15   ← keep this (latest)

w = Window.partitionBy("customer_id").orderBy(F.col("load_timestamp").desc())

# AFTER: one row per customer_id (latest wins)
deduped = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
```

---

## 2. Categorical Standardization — create_map()

**Used in:** `silver_crm_customer.py`, `silver_card.py`, `silver_transactions.py`, `silver_billing.py`, `silver_collections.py`  
**Purpose:** Map raw source codes to standardized values.

```python
# BEFORE: gender column has raw codes M/F/O
# gender: "M", "F", "m", "MALE" (inconsistent)

GENDER_MAP = {"M": "MALE", "F": "FEMALE", "O": "OTHER"}
gender_expr = F.create_map([F.lit(k) for pair in GENDER_MAP.items() for k in pair])

# AFTER: standardized MALE/FEMALE/OTHER, unknown → "OTHER"
df = df.withColumn("gender", F.coalesce(gender_expr[F.upper(F.col("gender"))], F.lit("OTHER")))
# gender: "MALE", "FEMALE", "OTHER"
```

---

## 3. Date Parsing & Validation

**Used in:** `silver_crm_customer.py`, `silver_card.py`, `silver_billing.py`, `silver_collections.py`  
**Purpose:** Cast string dates and reject invalid/future values.

```python
# BEFORE: date_of_birth is a string "2026-15-99" or future date
# date_of_birth: "1985-03-22", "2030-01-01", "invalid-date"

df = df.withColumn("date_of_birth", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))

# Reject future dates (DOB must be in the past)
df = df.withColumn("date_of_birth",
    F.when(F.col("date_of_birth") < F.current_date(), F.col("date_of_birth"))
     .otherwise(F.lit(None))
)
# AFTER: "1985-03-22" kept | "2030-01-01" → null | "invalid-date" → null
```

---

## 4. Range Validation — Cap or Nullify Out-of-Range Values

**Used in:** `silver_crm_customer.py`, `silver_card.py`  
**Purpose:** Enforce business rules on numeric ranges.

```python
# BEFORE: credit_score can be 0, 999, -1 (injected DQ issues)
# credit_score: 720, 999, -1, 0, 650

# AFTER: only valid 300-850 range kept; others → null
df = df.withColumn("credit_score",
    F.when(F.col("credit_score").between(300, 850), F.col("credit_score"))
     .otherwise(F.lit(None))
)

# Cap approach (used for cash_limit ≤ credit_limit)
df = df.withColumn("cash_limit",
    F.when(
        (F.col("cash_limit") > 0) & (F.col("cash_limit") <= F.col("credit_limit")),
        F.round(F.col("cash_limit"), 2)
    ).otherwise(F.round(F.col("credit_limit") * 0.3, 2))  # default: 30% of credit_limit
)
```

---

## 5. String Standardization — Trim, Lower, Regex Clean

**Used in:** `silver_crm_customer.py`, `silver_transactions.py`  
**Purpose:** Normalize email, phone, merchant names.

```python
# BEFORE: email "  Alice@BANK.COM  ", phone "+1 (800) 555-1234"

# Email: lowercase + trim + regex validate
df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
df = df.withColumn("email",
    F.when(F.col("email").rlike(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$"), F.col("email"))
     .otherwise(F.lit(None))
)
# AFTER: "alice@bank.com"

# Phone: keep only digits and +
df = df.withColumn("phone_number", F.regexp_replace(F.col("phone_number"), r"[^\d+]", ""))
# AFTER: "+18005551234"
```

---

## 6. Incremental Merge (MERGE INTO / Delta Upsert)

**Used in:** All silver notebooks, gold SCD2 notebooks  
**Purpose:** Idempotent upsert — update existing rows, insert new ones.

```python
# Write temp view
df.createOrReplaceTempView("source_tmp")

# MERGE: update if exists, insert if new
spark.sql("""
    MERGE INTO silver.customer_clean AS t
    USING source_tmp AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
# BEFORE: table has 9,800 rows
# Source has 10,200 rows (200 new + 200 changed)
# AFTER: table has 10,000 rows (200 inserted, 200 updated)
```

---

## 7. Incremental Filter — is_incremental()

**Used in:** `silver_sales.sql` (pattern), all Silver incremental notebooks  
**Purpose:** Only process new/changed data on subsequent runs.

```python
from pyspark.sql import functions as F

# Get watermark: max date in target table
target_max_date = spark.sql("SELECT MAX(date_sk) FROM silver.transaction_clean").collect()[0][0]

# Filter bronze to only new records since last run
if target_max_date:
    df = df.filter(F.col("date_sk") >= target_max_date - 30)  # 30-day overlap for safety
# BEFORE (full): 500K rows
# AFTER (incremental): ~5K rows (last 30 days of activity)
```

---

## 8. Window Functions — Lead, Lag, Rank

**Used in:** `silver_collections.py`  
**Purpose:** Compute trend signals, sequences, and period-over-period comparisons.

```python
from pyspark.sql import Window, functions as F

w = Window.partitionBy("customer_id").orderBy("default_date")

df = df \
    .withColumn("prev_default_date", F.lag("default_date", 1).over(w)) \
    .withColumn("next_default_date", F.lead("default_date", 1).over(w)) \
    .withColumn("default_sequence",  F.rank().over(w)) \
    .withColumn("is_repeat_default", F.col("prev_default_date").isNotNull()) \
    .withColumn("dormancy_period_days", F.datediff("default_date", "prev_default_date"))

# DPD trend vs previous event
.withColumn("prev_dpd", F.lag("days_past_due", 1).over(w))
.withColumn("dpd_trend",
    F.when(F.col("prev_dpd").isNull(), "FIRST")
     .when(F.col("days_past_due") > F.col("prev_dpd"), "WORSENING")
     .when(F.col("days_past_due") < F.col("prev_dpd"), "IMPROVING")
     .otherwise("STABLE")
)
# customer C001, default 1: sequence=1, is_repeat=false, dpd_trend=FIRST
# customer C001, default 2: sequence=2, is_repeat=true, dpd_trend=WORSENING (DPD 45 vs 30)
```

---

## 9. Consecutive Streak Detection (Window + Group Trick)

**Used in:** `payment_behavior.py`  
**Purpose:** Count consecutive late payment months (streak length).

```python
w_time = Window.partitionBy("card_sk").orderBy("statement_date_sk")

# Assign group number: increments every time is_late = False
# (This breaks the streak counter on each non-late row)
df = df.withColumn("grp",
    F.sum((F.col("is_late") == False).cast("int")).over(w_time)
)

# Max streak per card = max count of True in any single group
streaks = (
    df.groupBy("card_sk", "grp")
      .agg(F.sum(F.col("is_late").cast("int")).alias("streak_len"))
      .groupBy("card_sk")
      .agg(F.max("streak_len").alias("max_consecutive_late_months"))
)
# BEFORE: is_late sequence for card_1: T, T, F, T, T, T, F, T
#   grp:                               0, 0, 1, 1, 1, 1, 2, 2
# AFTER: max_consecutive_late_months = 3 (group 1: T, T, T)
```

---

## 10. SCD Type 2 — Hash-based Change Detection

**Used in:** `gold_dim_customer.py`, `gold_dim_card.py`  
**Purpose:** Detect changes to tracked columns and create new history versions.

```python
# Columns to track for changes
SCD2_COLS = ["credit_score", "annual_income", "employment_status", "marital_status"]

# Compute hash of tracked columns
source = source.withColumn("scd_hash",
    F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in SCD2_COLS]))
)

# Compare with current version in Gold
dim_current = spark.read.table("gold.dim_customer").filter("is_current = true")

# Changed rows: hash mismatch or new customer
changed = source.alias("src").join(dim_current.alias("dim"), on="customer_id", how="left").filter(
    F.col("dim.customer_id").isNull() |            # new
    (F.col("src.scd_hash") != F.col("dim.scd_hash"))  # changed
).select("src.*")

# Step 1: Expire old current rows
dim.merge(changed.select("customer_id","scd_hash").alias("chg"),
    "dim.customer_id = chg.customer_id AND dim.is_current = true AND dim.scd_hash != chg.scd_hash"
).whenMatchedUpdate(set={"is_current": False, "expiry_date": F.date_sub(F.current_date(), 1)}).execute()

# Step 2: Insert new versions
changed.withColumn("effective_date", F.current_date()) \
       .withColumn("expiry_date",    F.lit("9999-12-31").cast("date")) \
       .withColumn("is_current",     F.lit(True)) \
       .write.mode("append").saveAsTable("gold.dim_customer")

# BEFORE: customer C001, credit_score=720, is_current=True, expiry=9999-12-31
# Source: customer C001, credit_score=680 (changed)
# AFTER:  customer C001, credit_score=720, is_current=False, expiry=2026-06-05  ← historical
#         customer C001, credit_score=680, is_current=True,  expiry=9999-12-31  ← current
```

---

## 11. FK Resolution — Join to Dimension with Sentinel Fallback

**Used in:** `gold_fact_transaction.py`, `gold_fact_statement.py`  
**Purpose:** Resolve natural keys to surrogate keys; use -1 for unresolved rows.

```python
dim_customer = spark.read.table("gold.dim_customer").filter("is_current = true") \
    .select(F.col("customer_sk").alias("dim_customer_sk"), "customer_id")

fact = txn.join(dim_customer, on="customer_id", how="left") \
    .withColumn("customer_sk", F.coalesce(F.col("dim_customer_sk"), F.lit(-1)))

# BEFORE: transaction with customer_id = "C_ORPHAN" (no match in dim_customer)
# AFTER:  customer_sk = -1 (sentinel, queryable as "unknown customer")
```

---

## 12. RFM Quintile Scoring — approxQuantile

**Used in:** `customer_segmentation.py`  
**Purpose:** Assign percentile-based scores without a full sort.

```python
# Compute quintile thresholds (20th, 40th, 60th, 80th percentile)
thresholds = df.approxQuantile("monetary", [0.2, 0.4, 0.6, 0.8], 0.01)
# thresholds = [1200.0, 3500.0, 7800.0, 15000.0]

m_score = (
    F.when(F.col("monetary") >= thresholds[3], F.lit(5))  # top 20%
     .when(F.col("monetary") >= thresholds[2], F.lit(4))
     .when(F.col("monetary") >= thresholds[1], F.lit(3))
     .when(F.col("monetary") >= thresholds[0], F.lit(2))
     .otherwise(F.lit(1))  # bottom 20%
)
# BEFORE: monetary = 20000 → score = 5 (top 20%)
# BEFORE: monetary =   500 → score = 1 (bottom 20%)
```

---

## 13. Multi-level Aggregation Join (customer_360_view)

**Used in:** `silver_enrichment.py`  
**Purpose:** Build a wide customer profile by joining aggregates from multiple fact tables.

```python
# Aggregate transactions per customer
txn_agg = txns.groupBy("customer_id").agg(
    F.count("transaction_id").alias("total_transactions"),
    F.sum(F.when(F.col("transaction_type") == "PURCHASE", F.col("amount")).otherwise(F.lit(0))).alias("total_spend"),
    F.max("transaction_datetime").alias("last_transaction_date"),
)

# Aggregate billing per customer (via card join)
billing_agg = stmts.join(cards.select("card_id","customer_id"), on="card_id") \
    .groupBy("customer_id").agg(
        F.avg("utilization_ratio").alias("avg_utilization_ratio"),
        F.avg("payment_ratio").alias("avg_payment_ratio"),
    )

# Join all aggregates to customer base
customer_360 = customers \
    .join(txn_agg,     on="customer_id", how="left") \
    .join(billing_agg, on="customer_id", how="left") \
    .join(default_agg, on="customer_id", how="left") \
    .fillna({"total_transactions": 0, "total_spend": 0.0})

# BEFORE: customers table, 10,000 rows, no behavioral columns
# AFTER:  customer_360_view, 10,000 rows, 25+ behavioral/risk columns per customer
```

---

## 14. Recovery Status Priority Rollup

**Used in:** `gold_fact_default.py`  
**Purpose:** Aggregate multiple recovery events into a single "worst/best" status per default.

```python
# Multiple recovery rows per default_id: PENDING, PARTIAL
# Business rule: FULL > PARTIAL > PENDING > NO_RECOVERY

status_priority = (
    F.when(F.col("recovery_status") == "FULL",    F.lit(3))
     .when(F.col("recovery_status") == "PARTIAL", F.lit(2))
     .when(F.col("recovery_status") == "PENDING", F.lit(1))
     .otherwise(F.lit(0))
)

recovery_agg = recoveries \
    .withColumn("status_rank", status_priority) \
    .groupBy("default_id") \
    .agg(
        F.sum("recovery_amount").alias("recovery_amount"),
        F.count("recovery_id").alias("recovery_count"),
        F.max("status_rank").alias("max_status_rank"),
    ) \
    .withColumn("recovery_status",
        F.when(F.col("max_status_rank") == 3, F.lit("FULL"))
         .when(F.col("max_status_rank") == 2, F.lit("PARTIAL"))
         .when(F.col("max_status_rank") == 1, F.lit("PENDING"))
         .otherwise(F.lit("NO_RECOVERY"))
    )
# BEFORE: default D001 → [PENDING $500, PARTIAL $1500, PARTIAL $2000]
# AFTER:  default D001 → recovery_amount=$4000, recovery_count=3, recovery_status=PARTIAL
```

---

## 15. Rolling Average — Window with Row Range

**Used in:** `monthly_trends.py`  
**Purpose:** Compute 3-month rolling average spend.

```python
w_time = Window.orderBy("year", "month_number")

df = df.withColumn("spend_3m_avg",
    F.round(F.avg("total_spend").over(w_time.rowsBetween(-2, 0)), 2)
)
# BEFORE: Jan=$1M, Feb=$1.2M, Mar=$0.9M
# AFTER:  Mar spend_3m_avg = (1.0 + 1.2 + 0.9) / 3 = $1.033M
```
