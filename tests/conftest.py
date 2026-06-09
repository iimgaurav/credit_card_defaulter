"""Shared fixtures and markers."""

import pytest
import sys
import os


def _spark_available():
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder.master("local[2]").appName("test")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.stop()
        return True
    except Exception:
        return False


SPARK_AVAILABLE = _spark_available()


@pytest.fixture(scope="session")
def spark():
    """PySpark SparkSession fixture. Skips all tests in the session if unavailable."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark SparkSession unavailable (PySpark 4.0.1 regression on this host)")
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder.master("local[2]").appName("test_session")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def sample_df(spark):
    """Simple DataFrame fixture for DQ tests."""
    from pyspark.sql import Row
    return spark.createDataFrame([
        Row(id="1", name="Alice", age=30, email="alice@co.com"),
        Row(id="2", name="Bob", age=25, email="bob@co.com"),
        Row(id="3", name=None, age=None, email="charlie@co.com"),
    ])
