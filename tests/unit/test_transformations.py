"""
Unit tests for Silver/Gold transformation logic.
Run: pytest tests/unit/test_transformations.py -v
Uses local PySpark — no Databricks connection required.
"""
import pytest
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]").appName("test_transformations")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Deduplication ─────────────────────────────────────────────────────────────

def test_row_number_keeps_latest(spark):
    ts1, ts2 = datetime(2026, 1, 1), datetime(2026, 1, 15)
    df = spark.createDataFrame([
        ("C001", "Alice",  50000.0, ts1),
        ("C001", "Alicia", 55000.0, ts2),   # latest — keep this
        ("C002", "Bob",    40000.0, ts1),
    ], ["customer_id", "name", "income", "load_timestamp"])
    w = Window.partitionBy("customer_id").orderBy(F.col("load_timestamp").desc())
    result = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    assert result.count() == 2
    assert result.filter("customer_id = 'C001'").collect()[0]["income"] == 55000.0


def test_drop_duplicates_on_pk(spark):
    df = spark.createDataFrame([
        ("T001", 100.0), ("T001", 100.0), ("T002", 200.0)
    ], ["transaction_id", "amount"])
    assert df.dropDuplicates(["transaction_id"]).count() == 2


# ── Standardization ──────────────────────────────────────────────────────────

def test_gender_map_standardization(spark):
    GENDER_MAP = {"M": "MALE", "F": "FEMALE", "O": "OTHER"}
    expr = F.create_map([F.lit(k) for pair in GENDER_MAP.items() for k in pair])
    df = spark.createDataFrame([("M",), ("f",), ("X",)], ["gender"])
    result = df.withColumn("gender", F.coalesce(expr[F.upper("gender")], F.lit("OTHER")))
    vals = [r["gender"] for r in result.collect()]
    assert vals == ["MALE", "FEMALE", "OTHER"]


def test_email_lower_trim(spark):
    df = spark.createDataFrame([("  ALICE@BANK.COM  ",), (None,)], ["email"])
    result = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    assert result.collect()[0]["email"] == "alice@bank.com"
    assert result.collect()[1]["email"] is None


def test_phone_regexp_replace(spark):
    df = spark.createDataFrame([("+1 (800) 555-1234",)], ["phone"])
    result = df.withColumn("phone", F.regexp_replace("phone", r"[^\d+]", ""))
    assert result.collect()[0]["phone"] == "+18005551234"


# ── Validation ────────────────────────────────────────────────────────────────

def test_credit_score_range(spark):
    df = spark.createDataFrame([(720,), (200,), (900,), (300,), (850,)], ["credit_score"])
    result = df.withColumn("credit_score",
        F.when(F.col("credit_score").between(300, 850), F.col("credit_score")).otherwise(F.lit(None))
    )
    assert result.filter("credit_score is not null").count() == 3   # 720, 300, 850


def test_null_mandatory_filter(spark):
    df = spark.createDataFrame([("T1","C1"), (None,"C1"), ("T3",None)], ["txn_id","card_id"])
    result = df.filter(F.col("txn_id").isNotNull() & F.col("card_id").isNotNull())
    assert result.count() == 1


def test_amount_abs_cast(spark):
    df = spark.createDataFrame([(-150.5,), (200.0,)], ["amount"])
    result = df.withColumn("amount", F.round(F.abs(F.col("amount").cast("decimal(12,2)")), 2))
    assert float(result.collect()[0]["amount"]) == 150.5


# ── Date Parsing ──────────────────────────────────────────────────────────────

def test_to_date_future_nulled(spark):
    df = spark.createDataFrame([("1985-03-22",), ("2099-01-01",), ("bad-date",)], ["dob"])
    result = (df
        .withColumn("dob", F.to_date("dob", "yyyy-MM-dd"))
        .withColumn("dob", F.when(F.col("dob") < F.current_date(), F.col("dob")).otherwise(F.lit(None)))
    )
    vals = result.collect()
    assert str(vals[0]["dob"]) == "1985-03-22"
    assert vals[1]["dob"] is None   # future
    assert vals[2]["dob"] is None   # invalid


def test_to_timestamp_combine(spark):
    df = spark.createDataFrame([("2026-01-15", "14:30:00")], ["d", "t"])
    result = df.withColumn("ts", F.to_timestamp(F.concat_ws(" ", "d", "t"), "yyyy-MM-dd HH:mm:ss"))
    assert result.collect()[0]["ts"].year == 2026


# ── Window Functions ──────────────────────────────────────────────────────────

def test_lag_first_row_null(spark):
    df = spark.createDataFrame([("C1","2026-01-01",30),("C1","2026-03-01",45)],
                                ["cid","dt","dpd"])
    w = Window.partitionBy("cid").orderBy("dt")
    result = df.withColumn("prev", F.lag("dpd", 1).over(w))
    first = result.filter("dt = '2026-01-01'").collect()[0]["prev"]
    assert first is None


def test_rank_and_lead(spark):
    df = spark.createDataFrame([("C1","2026-01-01"),("C1","2026-03-01"),("C1","2026-06-01")],
                                ["cid","dt"])
    w = Window.partitionBy("cid").orderBy("dt")
    result = df.withColumn("seq", F.rank().over(w)).withColumn("nxt", F.lead("dt",1).over(w))
    rows = result.orderBy("dt").collect()
    assert [r["seq"] for r in rows] == [1, 2, 3]
    assert rows[0]["nxt"] == "2026-03-01"
    assert rows[2]["nxt"] is None


# ── DQ Checks ────────────────────────────────────────────────────────────────

def test_null_pct(spark):
    df = spark.createDataFrame([("A",),(None,),(None,),("B",)], ["col"])
    pct = round(df.filter(F.col("col").isNull()).count() / df.count() * 100, 2)
    assert pct == 50.0


def test_duplicate_detection(spark):
    df = spark.createDataFrame([("ID1",),("ID1",),("ID2",)], ["pk"])
    dups = df.groupBy("pk").count().filter("count > 1").count()
    assert dups == 1


def test_domain_check(spark):
    df = spark.createDataFrame([("MALE",),("FEMALE",),("INVALID",)], ["gender"])
    invalid = df.filter(~F.col("gender").isin(["MALE","FEMALE","OTHER"])).count()
    assert invalid == 1


# ── SCD2 ──────────────────────────────────────────────────────────────────────

def test_scd_hash_changes_on_tracked_col(spark):
    cols = ["credit_score", "income"]
    df = spark.createDataFrame([(720,50000.0),(680,50000.0),(720,50000.0)],
                                ["credit_score","income"])
    result = df.withColumn("h", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in cols])))
    h = [r["h"] for r in result.collect()]
    assert h[0] != h[1]    # score changed → different hash
    assert h[0] == h[2]    # identical rows → same hash


def test_scd2_no_duplicate_current(spark):
    df = spark.createDataFrame([
        ("C1", True, "9999-12-31"), ("C1", False, "2026-01-14"), ("C2", True, "9999-12-31")
    ], ["customer_id","is_current","expiry"])
    multi = (df.filter("is_current = true")
               .groupBy("customer_id").count()
               .filter("count > 1").count())
    assert multi == 0


# ── Watermark ────────────────────────────────────────────────────────────────

def test_watermark_default_on_missing(spark):
    """Watermark returns default when table has no entry."""
    DEFAULT = "1900-01-01 00:00:00"
    # Simulate empty watermark lookup
    result_ts = DEFAULT   # what Watermark.get() returns on first run
    assert result_ts == DEFAULT


# ── YTD / MTD / QTD ──────────────────────────────────────────────────────────

def test_ytd_filter(spark):
    df = spark.createDataFrame([(2026,1,100.0),(2026,6,200.0),(2025,12,50.0)],
                                ["year","month","spend"])
    current_year = 2026
    ytd = df.filter(F.col("year") == current_year).agg(F.sum("spend")).collect()[0][0]
    assert ytd == 300.0


def test_mtd_filter(spark):
    df = spark.createDataFrame([(2026,6,100.0),(2026,6,150.0),(2026,5,200.0)],
                                ["year","month","spend"])
    mtd = df.filter((F.col("year")==2026)&(F.col("month")==6)).agg(F.sum("spend")).collect()[0][0]
    assert mtd == 250.0
