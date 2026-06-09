# Databricks notebook source
# MAGIC %md
# MAGIC # schema_registry — Schema Definitions

# COMMAND ----------

"""
Schema registry for all source and target tables.
Centralizes schema definitions to ensure consistency across layers.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DecimalType, DateType, TimestampType, BooleanType,
)

# ██  BRONZE LAYER SCHEMAS  ████████████████████████████████████████████████

CRM_CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("date_of_birth", StringType()),
    StructField("gender", StringType()),
    StructField("marital_status", StringType()),
    StructField("email", StringType()),
    StructField("phone_number", StringType()),
    StructField("employment_status", StringType()),
    StructField("annual_income", DoubleType()),
    StructField("credit_score", IntegerType()),
])

CRM_ADDRESS_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("address_line1", StringType()),
    StructField("city", StringType()),
    StructField("state_code", StringType()),
    StructField("country_code", StringType()),
    StructField("zip_code", StringType()),
    StructField("address_type", StringType()),
])

CARD_DETAILS_SCHEMA = StructType([
    StructField("card_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("card_type", StringType()),
    StructField("card_network", StringType()),
    StructField("issued_date", StringType()),
    StructField("expiry_date", StringType()),
    StructField("credit_limit", DoubleType()),
    StructField("cash_limit", DoubleType()),
    StructField("interest_rate", DoubleType()),
])

CARD_STATUS_SCHEMA = StructType([
    StructField("card_id", StringType()),
    StructField("status_code", StringType()),
    StructField("status_date", StringType()),
    StructField("reason_code", StringType()),
])

TXN_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("transaction_date", StringType()),
    StructField("transaction_time", StringType()),
    StructField("merchant_name", StringType()),
    StructField("merchant_category", StringType()),
    StructField("merchant_country", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency_code", StringType()),
    StructField("transaction_type", StringType()),
    StructField("pos_entry_mode", StringType()),
])

BILLING_STATEMENT_SCHEMA = StructType([
    StructField("statement_id", StringType()),
    StructField("card_id", StringType()),
    StructField("statement_date", StringType()),
    StructField("due_date", StringType()),
    StructField("opening_balance", DoubleType()),
    StructField("total_purchases", DoubleType()),
    StructField("total_payments", DoubleType()),
    StructField("total_credits", DoubleType()),
    StructField("interest_charged", DoubleType()),
    StructField("fees_charged", DoubleType()),
    StructField("closing_balance", DoubleType()),
    StructField("minimum_due", DoubleType()),
])

BILLING_PAYMENT_SCHEMA = StructType([
    StructField("payment_id", StringType()),
    StructField("statement_id", StringType()),
    StructField("payment_date", StringType()),
    StructField("payment_amount", DoubleType()),
    StructField("payment_method", StringType()),
])

COLLECTIONS_DEFAULT_SCHEMA = StructType([
    StructField("default_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("card_id", StringType()),
    StructField("default_date", StringType()),
    StructField("days_past_due", IntegerType()),
    StructField("outstanding_amount", DoubleType()),
    StructField("collection_stage", StringType()),
    StructField("last_contact_date", StringType()),
])

COLLECTIONS_RECOVERY_SCHEMA = StructType([
    StructField("recovery_id", StringType()),
    StructField("default_id", StringType()),
    StructField("recovery_date", StringType()),
    StructField("recovery_amount", DoubleType()),
    StructField("recovery_method", StringType()),
    StructField("recovery_status", StringType()),
])

# ██  REFERENCE SCHEMAS  ███████████████████████████████████████████████████

REF_COUNTRY_SCHEMA = StructType([
    StructField("country_code", StringType()),
    StructField("country_name", StringType()),
    StructField("region", StringType()),
    StructField("currency_code", StringType()),
])

REF_STATE_SCHEMA = StructType([
    StructField("state_code", StringType()),
    StructField("state_name", StringType()),
    StructField("country_code", StringType()),
    StructField("region", StringType()),
])

REF_CURRENCY_SCHEMA = StructType([
    StructField("currency_code", StringType()),
    StructField("currency_name", StringType()),
    StructField("currency_symbol", StringType()),
    StructField("country_code", StringType()),
])

# ██  CALENDAR SCHEMA  █████████████████████████████████████████████████████

CALENDAR_SCHEMA = StructType([
    StructField("date_key", StringType()),
    StructField("full_date", StringType()),
    StructField("day", IntegerType()),
    StructField("month", IntegerType()),
    StructField("year", IntegerType()),
    StructField("quarter", IntegerType()),
    StructField("day_of_week", IntegerType()),
    StructField("day_name", StringType()),
    StructField("month_name", StringType()),
    StructField("week_number", IntegerType()),
    StructField("is_weekend", BooleanType()),
    StructField("is_holiday", BooleanType()),
    StructField("fiscal_year", IntegerType()),
    StructField("fiscal_quarter", IntegerType()),
])

# ██  METADATA COLUMNS  ████████████████████████████████████████████████████

METADATA_COLUMNS = StructType([
    StructField("ingestion_date", DateType()),
    StructField("ingestion_batch_id", StringType()),
    StructField("source_file", StringType()),
    StructField("load_timestamp", TimestampType()),
    StructField("_created_at", TimestampType()),
    StructField("_created_by", StringType()),
])

# Schema lookup
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


def get_bronze_schema(table_name: str) -> StructType:
    """Return schema for a bronze table by name."""
    return BRONZE_SCHEMAS.get(table_name)
