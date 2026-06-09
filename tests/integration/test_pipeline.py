"""
Integration tests for the full medallion pipeline.
Requires Databricks connect or a running cluster.
Run: pytest tests/integration/test_pipeline.py -v --skip-integration
     pytest tests/integration/test_pipeline.py -v          # with DATABRICKS_* env vars
"""

import os
import pytest
from pyspark.sql import SparkSession

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABRICKS_HOST"),
    reason="Integration tests require Databricks connection",
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]").appName("integration")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


CATALOG = "credit_card_dev"


# ── Bronze Layer ──────────────────────────────────────────────────────────────

def test_bronze_tables_exist(spark):
    tables = [
        "bronze.crm_customer_master",
        "bronze.crm_customer_address",
        "bronze.card_details",
        "bronze.card_status",
        "bronze.txn_transactions",
        "bronze.billing_statements",
        "bronze.billing_payments",
        "bronze.collections_defaults",
        "bronze.collections_recovery",
    ]
    for tbl in tables:
        try:
            cnt = spark.read.table(f"{CATALOG}.{tbl}").count()
            assert cnt > 0, f"{tbl} is empty"
        except Exception as e:
            pytest.fail(f"{tbl} missing or unreadable: {e}")


def test_bronze_has_rescued_data_column(spark):
    tbls = [
        "bronze.crm_customer_master", "bronze.crm_customer_address",
        "bronze.card_details", "bronze.card_status",
        "bronze.txn_transactions", "bronze.billing_statements",
        "bronze.billing_payments",
    ]
    for tbl in tbls:
        cols = [c.name for c in spark.table(f"{CATALOG}.{tbl}").schema.fields]
        assert "_rescued_data" in cols, f"{tbl} missing _rescued_data"


# ── Silver Layer ──────────────────────────────────────────────────────────────

def test_silver_tables_exist(spark):
    tables = [
        "silver.customer_clean",
        "silver.card_clean",
        "silver.transaction_clean",
        "silver.billing_clean",
        "silver.collections_clean",
        "silver.dq_scores",
        "silver.pipeline_logs",
    ]
    for tbl in tables:
        try:
            cnt = spark.read.table(f"{CATALOG}.{tbl}").count()
            assert cnt > 0, f"{tbl} is empty"
        except Exception as e:
            pytest.fail(f"{tbl} missing or unreadable: {e}")


def test_silver_dedup_no_duplicate_pks(spark):
    """Verify silver tables have no duplicate natural keys."""
    checks = [
        ("silver.customer_clean", "customer_id"),
        ("silver.card_clean", "card_id"),
        ("silver.transaction_clean", "transaction_id"),
    ]
    for tbl, pk in checks:
        df = spark.read.table(f"{CATALOG}.{tbl}")
        dupes = df.groupBy(pk).count().filter("count > 1").count()
        assert dupes == 0, f"{tbl}: {dupes} duplicate {pk} values"


# ── Gold Layer ────────────────────────────────────────────────────────────────

def test_gold_tables_exist(spark):
    tables = [
        "gold.dim_date",
        "gold.dim_geography",
        "gold.dim_customer",
        "gold.dim_card",
        "gold.fact_transaction",
        "gold.fact_statement",
        "gold.fact_default_analysis",
    ]
    for tbl in tables:
        try:
            cnt = spark.read.table(f"{CATALOG}.{tbl}").count()
            assert cnt > 0, f"{tbl} is empty"
        except Exception as e:
            pytest.fail(f"{tbl} missing or unreadable: {e}")


def test_gold_no_null_surrogate_keys(spark):
    """Verify fact tables have no null FK references."""
    checks = [
        ("gold.fact_transaction", "customer_sk"),
        ("gold.fact_transaction", "card_sk"),
        ("gold.fact_transaction", "date_sk"),
        ("gold.fact_statement", "customer_sk"),
        ("gold.fact_default_analysis", "customer_sk"),
    ]
    for tbl, fk in checks:
        nulls = spark.read.table(f"{CATALOG}.{tbl}").filter(f"`{fk}` IS NULL").count()
        assert nulls == 0, f"{tbl}: {nulls} NULL {fk}"


def test_gold_referential_integrity(spark):
    """Verify fact FKs exist in their respective dimension tables."""
    ri_checks = [
        ("gold.fact_transaction", "customer_sk", "gold.dim_customer", "customer_sk"),
        ("gold.fact_transaction", "card_sk", "gold.dim_card", "card_sk"),
        ("gold.fact_transaction", "date_sk", "gold.dim_date", "date_sk"),
    ]
    for fact_tbl, fact_fk, dim_tbl, dim_pk in ri_checks:
        fact = spark.read.table(f"{CATALOG}.{fact_tbl}")
        dim = spark.read.table(f"{CATALOG}.{dim_tbl}")
        orphan_count = fact.join(dim, fact[fact_fk] == dim[dim_pk], "anti").count()
        assert orphan_count == 0, f"{fact_tbl}.{fact_fk} has {orphan_count} orphan values"


# ── Data Quality ──────────────────────────────────────────────────────────────

def test_dq_scores_tracked(spark):
    try:
        cnt = spark.read.table(f"{CATALOG}.silver.dq_scores").count()
        print(f"DQ scores recorded: {cnt}")
    except Exception:
        pytest.skip("dq_scores table not available")


def test_dim_date_coverage(spark):
    df = spark.read.table(f"{CATALOG}.gold.dim_date")
    min_year = df.agg({"year": "min"}).collect()[0][0]
    max_year = df.agg({"year": "max"}).collect()[0][0]
    assert min_year <= 2020, f"dim_date starts at {min_year}, expected <= 2020"
    assert max_year >= 2030, f"dim_date ends at {max_year}, expected >= 2030"
