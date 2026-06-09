"""Unit tests for schema_registry.py — pure Python, no SparkSession needed."""

import pytest
import sys
sys.path.insert(0, "src")
from src.utils.schema_registry import (
    BRONZE_SCHEMAS, get_bronze_schema, METADATA_COLUMNS,
    CRM_CUSTOMER_SCHEMA, CRM_ADDRESS_SCHEMA, CARD_DETAILS_SCHEMA,
    CARD_STATUS_SCHEMA, TXN_TRANSACTIONS_SCHEMA, BILLING_STATEMENT_SCHEMA,
    BILLING_PAYMENT_SCHEMA, COLLECTIONS_DEFAULT_SCHEMA, COLLECTIONS_RECOVERY_SCHEMA,
    REF_COUNTRY_SCHEMA, REF_STATE_SCHEMA, REF_CURRENCY_SCHEMA, CALENDAR_SCHEMA,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DecimalType, DateType, BooleanType,
)


def test_bronze_schemas_has_13_entries():
    assert len(BRONZE_SCHEMAS) == 13


def test_bronze_schemas_keys():
    expected = [
        "crm_customer_master", "crm_customer_address", "card_details",
        "card_status", "txn_transactions", "billing_statements",
        "billing_payments", "collections_defaults", "collections_recovery",
        "ref_country", "ref_state", "ref_currency", "dim_calendar",
    ]
    assert list(BRONZE_SCHEMAS.keys()) == expected


def test_get_bronze_schema_known():
    schema = get_bronze_schema("crm_customer_master")
    assert schema is CRM_CUSTOMER_SCHEMA
    assert isinstance(schema, StructType)


def test_get_bronze_schema_unknown():
    assert get_bronze_schema("nonexistent") is None


@pytest.mark.parametrize("key,schema", [
    ("crm_customer_master", CRM_CUSTOMER_SCHEMA),
    ("crm_customer_address", CRM_ADDRESS_SCHEMA),
    ("card_details", CARD_DETAILS_SCHEMA),
    ("card_status", CARD_STATUS_SCHEMA),
    ("txn_transactions", TXN_TRANSACTIONS_SCHEMA),
    ("billing_statements", BILLING_STATEMENT_SCHEMA),
    ("billing_payments", BILLING_PAYMENT_SCHEMA),
    ("collections_defaults", COLLECTIONS_DEFAULT_SCHEMA),
    ("collections_recovery", COLLECTIONS_RECOVERY_SCHEMA),
    ("ref_country", REF_COUNTRY_SCHEMA),
    ("ref_state", REF_STATE_SCHEMA),
    ("ref_currency", REF_CURRENCY_SCHEMA),
    ("dim_calendar", CALENDAR_SCHEMA),
])
def test_all_schemas_are_structtype(key, schema):
    assert isinstance(schema, StructType), f"{key} is not StructType"
    assert len(schema.fields) >= 1, f"{key} has no fields"


def test_required_field_present():
    fields = {f.name: f for f in CRM_CUSTOMER_SCHEMA.fields}
    assert fields["customer_id"].dataType == StringType()
    assert fields["customer_id"].nullable is False
    assert fields["credit_score"].dataType == IntegerType()


def test_decimal_types():
    fields = {f.name: f for f in CARD_DETAILS_SCHEMA.fields}
    assert isinstance(fields["credit_limit"].dataType, DecimalType)
    assert fields["credit_limit"].dataType.precision == 12
    assert fields["credit_limit"].dataType.scale == 2


def test_boolean_types():
    fields = {f.name: f for f in CRM_ADDRESS_SCHEMA.fields}
    assert isinstance(fields["is_primary"].dataType, BooleanType)


def test_date_types():
    fields = {f.name: f for f in CALENDAR_SCHEMA.fields}
    assert isinstance(fields["date"].dataType, DateType())
    assert fields["date"].nullable is False


def test_metadata_columns():
    expected = [
        "ingestion_date", "ingestion_batch_id", "source_file",
        "load_timestamp", "_created_at", "_created_by", "_rescued_data",
    ]
    assert METADATA_COLUMNS == expected


def test_all_schema_fields_have_names():
    for key, schema in BRONZE_SCHEMAS.items():
        for field in schema.fields:
            assert field.name, f"{key} has field with empty name"


def test_primary_keys_not_nullable():
    pk_checks = [
        (CRM_CUSTOMER_SCHEMA, "customer_id"),
        (CRM_ADDRESS_SCHEMA, "address_id"),
        (CARD_DETAILS_SCHEMA, "card_id"),
        (CARD_STATUS_SCHEMA, "card_id"),
        (TXN_TRANSACTIONS_SCHEMA, "transaction_id"),
        (BILLING_STATEMENT_SCHEMA, "statement_id"),
        (BILLING_PAYMENT_SCHEMA, "payment_id"),
        (COLLECTIONS_DEFAULT_SCHEMA, "default_id"),
        (COLLECTIONS_RECOVERY_SCHEMA, "recovery_id"),
        (REF_COUNTRY_SCHEMA, "country_code"),
        (REF_STATE_SCHEMA, "state_code"),
        (REF_CURRENCY_SCHEMA, "currency_code"),
        (CALENDAR_SCHEMA, "date"),
    ]
    for schema, pk in pk_checks:
        fields = {f.name: f for f in schema.fields}
        assert fields[pk].nullable is False, f"{pk} should be non-nullable"
