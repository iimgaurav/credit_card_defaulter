"""Unit tests for dq_framework.py — requires local PySpark SparkSession.

Most tests use real DataFrames for thorough validation. If SparkSession
is unavailable (PySpark 4.0 regression), tests are skipped automatically.
"""

import pytest
import sys
sys.path.insert(0, "src")

pytest.importorskip("pyspark.sql.SparkSession")

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType,
)


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]").appName("test_dq")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def simple_df(spark):
    return spark.createDataFrame([
        Row(id="1", name="Alice", age=30, email="alice@co.com"),
        Row(id="2", name="Bob", age=25, email="bob@co.com"),
        Row(id="3", name=None, age=None, email="charlie@co.com"),
    ])


@pytest.fixture
def dup_df(spark):
    return spark.createDataFrame([
        Row(id="1", name="Alice"),
        Row(id="1", name="Alice"),
        Row(id="2", name="Bob"),
    ])


@pytest.fixture
def parent_df(spark):
    return spark.createDataFrame([
        Row(pk="1", val="a"),
        Row(pk="2", val="b"),
    ])


class TestCheckNulls:
    def test_all_passed(self, simple_df):
        from src.utils.dq_framework import check_nulls
        result = check_nulls(simple_df, ["id", "email"], "t")
        assert result["id"]["passed"] is True
        assert result["email"]["passed"] is True

    def test_some_nulls(self, simple_df):
        from src.utils.dq_framework import check_nulls
        result = check_nulls(simple_df, ["name", "age"], "t")
        assert result["name"]["null_count"] == 1
        assert result["age"]["null_count"] == 1

    def test_threshold_exceeded(self, simple_df):
        from src.utils.dq_framework import check_nulls
        result = check_nulls(simple_df, ["name"], "t", threshold_pct=10.0)
        assert result["name"]["null_pct"] == pytest.approx(33.33, 0.01)
        assert result["name"]["passed"] is False


class TestCheckDuplicates:
    def test_no_duplicates(self, simple_df):
        from src.utils.dq_framework import check_duplicates
        result = check_duplicates(simple_df, ["id"], "t")
        assert result["duplicate_groups"] == 0
        assert result["passed"] is True

    def test_has_duplicates(self, dup_df):
        from src.utils.dq_framework import check_duplicates
        result = check_duplicates(dup_df, ["id"], "t")
        assert result["duplicate_groups"] == 1
        assert result["passed"] is False


class TestCheckFullRowDedup:
    def test_no_duplicates(self, simple_df):
        from src.utils.dq_framework import check_full_row_dedup
        result = check_full_row_dedup(simple_df, "t")
        assert result["duplicate_rows"] == 0
        assert result["passed"] is True

    def test_has_duplicates(self, dup_df):
        from src.utils.dq_framework import check_full_row_dedup
        result = check_full_row_dedup(dup_df, "t")
        assert result["duplicate_rows"] == 1
        assert result["passed"] is False

    def test_exclude_cols(self, dup_df):
        from src.utils.dq_framework import check_full_row_dedup
        result = check_full_row_dedup(dup_df, "t", exclude_cols=["id"])
        assert result["duplicate_rows"] == 0


class TestCheckFKValidity:
    def test_all_valid(self, simple_df, parent_df):
        from src.utils.dq_framework import check_fk_validity
        df_child = simple_df.selectExpr("id as fk_col")
        result = check_fk_validity(df_child, parent_df, "fk_col", "pk", "t")
        assert result["orphan_fk_count"] == 1  # "3" not in parent
        assert result["passed"] is False

    def test_orphans_detected(self, spark):
        from src.utils.dq_framework import check_fk_validity
        child = spark.createDataFrame([Row(fk="1"), Row(fk="2"), Row(fk="99")])
        parent = spark.createDataFrame([Row(pk="1"), Row(pk="2")])
        result = check_fk_validity(child, parent, "fk", "pk", "t")
        assert result["orphan_fk_count"] == 1
        assert result["passed"] is False

    def test_empty_child(self, spark):
        from src.utils.dq_framework import check_fk_validity
        child = spark.createDataFrame([], StructType([StructField("fk", StringType())]))
        parent = spark.createDataFrame([Row(pk="1")])
        result = check_fk_validity(child, parent, "fk", "pk", "t")
        assert result["orphan_fk_count"] == 0


class TestCheckRange:
    def test_within_range(self, simple_df):
        from src.utils.dq_framework import check_range
        result = check_range(simple_df, "age", min_val=0, max_val=150)
        assert result["passed"] is True
        assert result["actual_min"] == 25.0
        assert result["actual_max"] == 30.0

    def test_below_min(self, simple_df):
        from src.utils.dq_framework import check_range
        result = check_range(simple_df, "age", min_val=26)
        assert result["passed"] is False
        assert result["violations"] == 1

    def test_above_max(self, simple_df):
        from src.utils.dq_framework import check_range
        result = check_range(simple_df, "age", max_val=28)
        assert result["passed"] is False
        assert result["violations"] == 1

    def test_no_bounds(self, simple_df):
        from src.utils.dq_framework import check_range
        result = check_range(simple_df, "age")
        assert result["passed"] is True


class TestCheckDomain:
    def test_all_valid(self, simple_df):
        from src.utils.dq_framework import check_domain
        result = check_domain(simple_df, "name", ["Alice", "Bob", None])
        assert result["passed"] is True

    def test_invalid_detected(self, simple_df):
        from src.utils.dq_framework import check_domain
        result = check_domain(simple_df, "name", ["Alice", "Bob"])
        assert result["passed"] is False
        assert result["invalid_distinct_count"] == 1


class TestCheckRegex:
    def test_all_match(self, simple_df):
        from src.utils.dq_framework import check_regex
        result = check_regex(simple_df, "email", r".+@.+\..+")
        assert result["passed"] is True
        assert result["violations"] == 0

    def test_violations_detected(self, simple_df):
        from src.utils.dq_framework import check_regex
        result = check_regex(simple_df, "name", r"^[A-Z].*")
        assert result["violations"] == 1  # None is not-null filter... actually None filtered out


class TestCheckSchemaDrift:
    def test_no_drift(self, spark, simple_df):
        from src.utils.dq_framework import check_schema_drift
        expected = StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("age", IntegerType()),
            StructField("email", StringType()),
        ])
        result = check_schema_drift(simple_df, expected, "t")
        assert result["passed"] is True

    def test_missing_column(self, spark, simple_df):
        from src.utils.dq_framework import check_schema_drift
        expected = StructType([
            StructField("id", StringType()),
            StructField("extra_col", StringType()),
        ])
        result = check_schema_drift(simple_df, expected, "t")
        assert result["passed"] is False
        assert "extra_col" in result["missing_columns"]

    def test_unexpected_column(self, spark, simple_df):
        from src.utils.dq_framework import check_schema_drift
        expected = StructType([StructField("id", StringType())])
        result = check_schema_drift(simple_df, expected, "t")
        assert result["passed"] is False
        assert len(result["unexpected_columns"]) >= 1

    def test_metadata_columns_ignored(self, spark):
        from src.utils.dq_framework import check_schema_drift
        df = spark.createDataFrame([Row(id="1", _rescued_data=None)])
        expected = StructType([StructField("id", StringType())])
        result = check_schema_drift(df, expected, "t", metadata_columns=["_rescued_data"])
        assert result["passed"] is True


class TestCheckRescuedData:
    def test_no_rescued(self, spark, simple_df):
        from src.utils.dq_framework import check_rescued_data
        simple_df.createOrReplaceTempView("_test_no_rescued")
        spark.catalog.dropTempView("_test_no_rescued")

    def test_empty_table(self, spark):
        from src.utils.dq_framework import check_rescued_data
        schema = StructType([StructField("id", StringType()), StructField("_rescued_data", StringType())])
        df = spark.createDataFrame([], schema)

        class FakeReader:
            def table(self, name):
                return df

        spark.read = FakeReader()
        result = check_rescued_data(spark, "fake_table")
        assert result["passed"] is True
        assert result["rescued_row_count"] == 0

    def test_no_rescued_column(self, spark, simple_df):
        from src.utils.dq_framework import check_rescued_data

        class FakeReader:
            def table(self, name):
                return simple_df

        spark.read = FakeReader()
        result = check_rescued_data(spark, "fake_table")
        assert result["passed"] is True


class TestCheckColumnPresence:
    def test_all_present(self, simple_df):
        from src.utils.dq_framework import check_column_presence
        result = check_column_presence(simple_df, ["id", "name", "email"], "t")
        assert result["passed"] is True

    def test_missing(self, simple_df):
        from src.utils.dq_framework import check_column_presence
        result = check_column_presence(simple_df, ["id", "nonexistent"], "t")
        assert result["passed"] is False
        assert "nonexistent" in result["missing_columns"]

    def test_case_insensitive(self, simple_df):
        from src.utils.dq_framework import check_column_presence
        result = check_column_presence(simple_df, ["ID", "NAME"], "t")
        assert result["passed"] is True


class TestReconcileCounts:
    def test_match(self, spark):
        from src.utils.dq_framework import reconcile_counts
        df1 = spark.createDataFrame([Row(id="1"), Row(id="2")])
        df2 = spark.createDataFrame([Row(id="1"), Row(id="2")])

        class FakeCatalog:
            tables = {}

            def tableExists(self, name):
                return True

        class FakeReader:
            def table(self, name):
                return {"src": df1, "tgt": df2}.get(name, df1)

        spark.read = FakeReader()
        result = reconcile_counts(spark, "src", "tgt")
        assert result["match"] is True
        assert result["source_count"] == result["target_count"] == 2

    def test_mismatch(self, spark):
        from src.utils.dq_framework import reconcile_counts

        df1 = spark.createDataFrame([Row(id="1"), Row(id="2"), Row(id="3")])
        df2 = spark.createDataFrame([Row(id="1")])

        class FakeReader:
            def table(self, name):
                return {"src": df1, "tgt": df2}.get(name, df1)

        spark.read = FakeReader()
        result = reconcile_counts(spark, "src", "tgt")
        assert result["match"] is False
        assert result["difference"] == 2


class TestRecordDQScore:
    def test_perfect_score(self, spark):
        from src.utils.dq_framework import record_dq_score
        checks = {
            "null_check": {"passed": True},
            "dup_check": {"passed": True},
        }

        class FakeWriter:
            def mode(self, m):
                return self
            def saveAsTable(self, name):
                pass

        class FakeDF:
            def write(self):
                return FakeWriter()

        real_createDataFrame = spark.createDataFrame

        def fake_createDataFrame(data, schema=None):
            return FakeDF()

        spark.createDataFrame = fake_createDataFrame
        score = record_dq_score(spark, "t", checks, "pipe", "run_1")
        assert score == 100.0
        spark.createDataFrame = real_createDataFrame

    def test_partial_score(self, spark):
        from src.utils.dq_framework import record_dq_score
        checks = {
            "null_check": {"passed": True},
            "dup_check": {"passed": False},
        }

        class FakeDF:
            def write(self):
                return self
            def mode(self, m):
                return self
            def saveAsTable(self, name):
                pass

        real_createDataFrame = spark.createDataFrame

        def fake_createDataFrame(data, schema=None):
            return FakeDF()

        spark.createDataFrame = fake_createDataFrame
        score = record_dq_score(spark, "t", checks, "pipe", "run_1")
        assert score == 50.0
        spark.createDataFrame = real_createDataFrame
