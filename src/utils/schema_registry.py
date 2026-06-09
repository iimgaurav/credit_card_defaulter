"""Bronze table schema definitions — single source of truth."""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DecimalType,
    DateType, TimestampType, BooleanType,
)


# ── CRM Customer Master ──────────────────────────────────────────────────────

CRM_CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address_id", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("annual_income", DoubleType(), True),
    StructField("employment_status", StringType(), True),
    StructField("years_employed", IntegerType(), True),
    StructField("housing_status", StringType(), True),
    StructField("dependents", IntegerType(), True),
])

# ── CRM Customer Address ─────────────────────────────────────────────────────

CRM_ADDRESS_SCHEMA = StructType([
    StructField("address_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("address_line1", StringType(), True),
    StructField("address_line2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_code", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("is_primary", BooleanType(), True),
])

# ── Card Details ─────────────────────────────────────────────────────────────

CARD_DETAILS_SCHEMA = StructType([
    StructField("card_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_brand", StringType(), True),
    StructField("credit_limit", DecimalType(12, 2), True),
    StructField("available_credit", DecimalType(12, 2), True),
    StructField("annual_fee", DecimalType(8, 2), True),
    StructField("interest_rate", DecimalType(5, 2), True),
    StructField("reward_program", StringType(), True),
    StructField("card_issue_date", DateType(), True),
    StructField("card_expiry_date", DateType(), True),
])

# ── Card Status ──────────────────────────────────────────────────────────────

CARD_STATUS_SCHEMA = StructType([
    StructField("card_id", StringType(), False),
    StructField("status", StringType(), True),
    StructField("status_reason", StringType(), True),
    StructField("status_date", DateType(), True),
    StructField("is_active", BooleanType(), True),
])

# ── Transactions ─────────────────────────────────────────────────────────────

TXN_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("card_id", StringType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("transaction_time", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_amount", DecimalType(12, 2), True),
    StructField("currency_code", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("is_international", BooleanType(), True),
])

# ── Billing Statements ───────────────────────────────────────────────────────

BILLING_STATEMENT_SCHEMA = StructType([
    StructField("statement_id", StringType(), False),
    StructField("card_id", StringType(), True),
    StructField("statement_date", DateType(), True),
    StructField("due_date", DateType(), True),
    StructField("opening_balance", DecimalType(12, 2), True),
    StructField("closing_balance", DecimalType(12, 2), True),
    StructField("minimum_due", DecimalType(12, 2), True),
    StructField("total_purchases", DecimalType(12, 2), True),
    StructField("payments_made", DecimalType(12, 2), True),
    StructField("credits_applied", DecimalType(12, 2), True),
    StructField("interest_charged", DecimalType(8, 2), True),
    StructField("fees_applied", DecimalType(8, 2), True),
])

# ── Billing Payments ─────────────────────────────────────────────────────────

BILLING_PAYMENT_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("card_id", StringType(), True),
    StructField("payment_date", DateType(), True),
    StructField("payment_amount", DecimalType(12, 2), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("statement_id", StringType(), True),
])

# ── Collections Defaults ─────────────────────────────────────────────────────

COLLECTIONS_DEFAULT_SCHEMA = StructType([
    StructField("default_id", StringType(), False),
    StructField("card_id", StringType(), True),
    StructField("default_date", DateType(), True),
    StructField("default_amount", DecimalType(12, 2), True),
    StructField("outstanding_balance", DecimalType(12, 2), True),
    StructField("days_past_due", IntegerType(), True),
    StructField("collection_stage", StringType(), True),
    StructField("assigned_collector", StringType(), True),
])

# ── Collections Recovery ─────────────────────────────────────────────────────

COLLECTIONS_RECOVERY_SCHEMA = StructType([
    StructField("recovery_id", StringType(), False),
    StructField("default_id", StringType(), True),
    StructField("recovery_date", DateType(), True),
    StructField("recovery_amount", DecimalType(12, 2), True),
    StructField("recovery_method", StringType(), True),
    StructField("recovery_status", StringType(), True),
])

# ── Reference Tables ─────────────────────────────────────────────────────────

REF_COUNTRY_SCHEMA = StructType([
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("currency_code", StringType(), True),
])

REF_STATE_SCHEMA = StructType([
    StructField("state_code", StringType(), False),
    StructField("state_name", StringType(), True),
    StructField("country_code", StringType(), True),
])

REF_CURRENCY_SCHEMA = StructType([
    StructField("currency_code", StringType(), False),
    StructField("currency_name", StringType(), True),
    StructField("currency_symbol", StringType(), True),
])

# ── Calendar ─────────────────────────────────────────────────────────────────

CALENDAR_SCHEMA = StructType([
    StructField("date", DateType(), False),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("month_name", StringType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField("is_holiday", BooleanType(), True),
])


# ── Metadata / Audit Columns (added to every bronze table) ──────────────────

METADATA_COLUMNS = [
    "ingestion_date",
    "ingestion_batch_id",
    "source_file",
    "load_timestamp",
    "_created_at",
    "_created_by",
    "_rescued_data",
]


# ── Schema Lookup ────────────────────────────────────────────────────────────

BRONZE_SCHEMAS = {
    "crm_customer_master": CRM_CUSTOMER_SCHEMA,
    "crm_customer_address": CRM_ADDRESS_SCHEMA,
    "card_details": CARD_DETAILS_SCHEMA,
    "card_status": CARD_STATUS_SCHEMA,
    "txn_transactions": TXN_TRANSACTIONS_SCHEMA,
    "billing_statements": BILLING_STATEMENT_SCHEMA,
    "billing_payments": BILLING_PAYMENT_SCHEMA,
    "collections_defaults": COLLECTIONS_DEFAULT_SCHEMA,
    "collections_recovery": COLLECTIONS_RECOVERY_SCHEMA,
    "ref_country": REF_COUNTRY_SCHEMA,
    "ref_state": REF_STATE_SCHEMA,
    "ref_currency": REF_CURRENCY_SCHEMA,
    "dim_calendar": CALENDAR_SCHEMA,
}


def get_bronze_schema(table_key):
    """Return StructType for the given bronze table key, or None."""
    return BRONZE_SCHEMAS.get(table_key)
