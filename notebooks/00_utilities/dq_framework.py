# Databricks notebook source
# MAGIC %md
# MAGIC # dq_framework — Data Quality Framework

# COMMAND ----------

"""
Data quality framework for the Credit Card Defaulter pipeline.
Includes checks, quarantine, reconciliation, and DQ scoring.

Checks available:
  check_nulls          — null % per column vs threshold
  check_duplicates     — PK-based duplicate detection
  check_full_row_dedup — full-row exact duplicates
  check_fk_validity    — FK orphan detection
  check_range          — min/max value bounds
  check_domain         — accepted values list
  check_regex          — regex pattern validation
  move_to_quarantine      — write bad rows to quarantine table
  record_dq_score         — persist DQ score to silver.dq_scores
  run_dq_suite            — run a declarative list of checks
  reconcile_counts        — source vs target row count comparison
  check_schema_drift      — compare actual schema vs expected schema
  check_rescued_data      — detect rescued data (schema drift) in bronze
  check_column_presence   — verify required columns exist in DataFrame
"""
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StructType
from datetime import datetime
import json

# Config vars come from %run ../00_utilities/config

# Silver quarantine table (for silver-layer rejected rows)
SILVER_DQ_QUARANTINE = f"{CATALOG}.{SILVER_SCHEMA}.dq_quarantine"


# ── Null check ────────────────────────────────────────────────────────────────

def check_nulls(df: DataFrame, columns: list, table_name: str,
                threshold_pct: float = DQ_THRESHOLD_NULL_PERCENT) -> dict:
    """Check null percentages for specified columns."""
    total = df.count()
    results = {}
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        pct = (null_count / total * 100) if total > 0 else 0
        results[col] = {
            "null_count": null_count,
            "null_pct": round(pct, 2),
            "threshold_pct": threshold_pct,
            "passed": pct <= threshold_pct,
        }
    return results


# ── PK Duplicate check ────────────────────────────────────────────────────────

def check_duplicates(df: DataFrame, pk_columns: list, table_name: str,
                     threshold_pct: float = DQ_THRESHOLD_DUPLICATE_PERCENT) -> dict:
    """Check for duplicate primary key combinations."""
    total = df.count()
    dup_count = df.groupBy(*pk_columns).count().filter(F.col("count") > 1).count()
    pct = (dup_count / total * 100) if total > 0 else 0
    return {
        "duplicate_groups": dup_count,
        "duplicate_pct": round(pct, 2),
        "threshold_pct": threshold_pct,
        "passed": pct <= threshold_pct,
    }


# ── Full-row duplicate check ──────────────────────────────────────────────────

def check_full_row_dedup(df: DataFrame, table_name: str,
                         exclude_cols: list = None) -> dict:
    """
    Detect exact full-row duplicates (ignoring audit/metadata columns).
    exclude_cols: columns to ignore (e.g. surrogate keys, timestamps).
    """
    exclude = set(exclude_cols or [])
    compare_cols = [c for c in df.columns if c not in exclude]
    total = df.count()
    unique = df.select(*compare_cols).distinct().count()
    dup_rows = total - unique
    pct = (dup_rows / total * 100) if total > 0 else 0
    return {
        "total_rows": total,
        "unique_rows": unique,
        "duplicate_rows": dup_rows,
        "duplicate_pct": round(pct, 2),
        "passed": dup_rows == 0,
    }


# ── FK validity check ─────────────────────────────────────────────────────────

def check_fk_validity(df_child: DataFrame, df_parent: DataFrame,
                      child_fk: str, parent_pk: str, table_name: str) -> dict:
    """Check foreign key integrity — detect orphan FK values."""
    child_count = df_child.select(child_fk).distinct().count()
    orphan_count = (
        df_child
        .join(df_parent, df_child[child_fk] == df_parent[parent_pk], "left_anti")
        .select(child_fk).distinct().count()
    )
    pct = (orphan_count / child_count * 100) if child_count > 0 else 0
    return {
        "distinct_child_fk": child_count,
        "orphan_fk_count": orphan_count,
        "orphan_pct": round(pct, 2),
        "passed": orphan_count == 0,
    }


# ── Range check ───────────────────────────────────────────────────────────────

def check_range(df: DataFrame, column: str, min_val=None, max_val=None) -> dict:
    """Check that column values fall within [min_val, max_val]."""
    stats = df.agg(
        F.min(column).alias("min_val"),
        F.max(column).alias("max_val"),
    ).collect()[0]

    violations = 0
    passed = True
    if min_val is not None:
        v = df.filter(F.col(column) < min_val).count()
        violations += v
        if v > 0: passed = False
    if max_val is not None:
        v = df.filter(F.col(column) > max_val).count()
        violations += v
        if v > 0: passed = False

    return {
        "actual_min": float(stats["min_val"]) if stats["min_val"] is not None else None,
        "actual_max": float(stats["max_val"]) if stats["max_val"] is not None else None,
        "expected_min": min_val,
        "expected_max": max_val,
        "violations": violations,
        "passed": passed,
    }


# ── Domain check ──────────────────────────────────────────────────────────────

def check_domain(df: DataFrame, column: str, accepted_values: list) -> dict:
    """Check that all column values are in accepted_values."""
    invalid = (
        df.filter(F.col(column).isNotNull())
          .filter(~F.col(column).isin(accepted_values))
          .select(column).distinct().count()
    )
    return {
        "invalid_distinct_count": invalid,
        "accepted_values_count": len(accepted_values),
        "passed": invalid == 0,
    }


# ── Regex pattern check ───────────────────────────────────────────────────────

def check_regex(df: DataFrame, column: str, pattern: str,
                threshold_pct: float = 0.0) -> dict:
    """
    Check that column values match the given regex pattern.
    Non-null values that don't match are counted as violations.
    threshold_pct: allowable % of violations (default 0 = strict).
    """
    non_null = df.filter(F.col(column).isNotNull())
    total_non_null = non_null.count()
    violations = non_null.filter(~F.col(column).rlike(pattern)).count()
    pct = (violations / total_non_null * 100) if total_non_null > 0 else 0
    return {
        "pattern": pattern,
        "total_non_null": total_non_null,
        "violations": violations,
        "violation_pct": round(pct, 2),
        "threshold_pct": threshold_pct,
        "passed": pct <= threshold_pct,
    }


# ── Quarantine ────────────────────────────────────────────────────────────────

def move_to_quarantine(df: DataFrame, table_name: str, rule_name: str,
                       check_details: dict, spark,
                       layer: str = "bronze") -> None:
    """
    Write bad records to the appropriate layer quarantine table.
    layer: 'bronze' → bronze.dq_quarantine | 'silver' → silver.dq_quarantine
    """
    if not QUARANTINE_ENABLED:
        return
    target = SILVER_DQ_QUARANTINE if layer == "silver" else BRONZE_DQ_QUARANTINE
    (
        df.withColumns({
            "quarantine_table":     F.lit(table_name),
            "quarantine_rule":      F.lit(rule_name),
            "quarantine_details":   F.lit(json.dumps(check_details, default=str)),
            "quarantine_timestamp": F.lit(datetime.utcnow().isoformat()),
            "quarantine_layer":     F.lit(layer),
        })
        .write.mode("append").saveAsTable(target)
    )


# ── DQ Score recording ────────────────────────────────────────────────────────

def record_dq_score(spark, table_name: str, checks: dict,
                    pipeline_name: str, run_id: str) -> float:
    """Persist DQ score to silver.dq_scores and return the score."""
    failed = sum(1 for c in checks.values() if isinstance(c, dict) and not c.get("passed", True))
    total  = len(checks)
    score  = round((total - failed) / total * 100, 2) if total > 0 else 100.0

    row = spark.createDataFrame([{
        "run_id":        run_id,
        "pipeline_name": pipeline_name,
        "table_name":    table_name,
        "dq_score":      score,
        "total_checks":  total,
        "failed_checks": failed,
        "check_details": json.dumps(checks, default=str),
        "run_date":      datetime.utcnow().date().isoformat(),
        "recorded_at":   datetime.utcnow(),
    }])
    row.write.mode("append").saveAsTable(SILVER_DQ_SCORES)
    return score


# ── Full DQ suite runner ──────────────────────────────────────────────────────

def run_dq_suite(df: DataFrame, table_name: str, checks_config: list,
                 spark, pipeline_name: str, run_id: str,
                 layer: str = "silver") -> dict:
    """
    Run a declarative list of DQ checks against a DataFrame.

    checks_config items:
      {"type": "null",        "column": "col",    "threshold": 5.0}
      {"type": "duplicate",   "pk_columns": [...]}
      {"type": "full_row_dedup", "exclude_cols": [...]}
      {"type": "range",       "column": "col",    "min": 0, "max": 100}
      {"type": "domain",      "column": "col",    "accepted_values": [...]}
      {"type": "regex",       "column": "col",    "pattern": "^...$", "threshold": 0.0}
      {"type": "fk",          "parent_df": df2,   "child_fk": "c", "parent_pk": "p"}
    """
    results = {}

    for check in checks_config:
        check_type = check["type"]
        col = check.get("column")

        if check_type == "null":
            r = check_nulls(df, [col], table_name, check.get("threshold", DQ_THRESHOLD_NULL_PERCENT))
            results[f"null_{col}"] = r[col]

        elif check_type == "duplicate":
            results["pk_duplicate"] = check_duplicates(df, check["pk_columns"], table_name)

        elif check_type == "full_row_dedup":
            results["full_row_dedup"] = check_full_row_dedup(df, table_name, check.get("exclude_cols"))

        elif check_type == "range":
            results[f"range_{col}"] = check_range(df, col, check.get("min"), check.get("max"))

        elif check_type == "domain":
            results[f"domain_{col}"] = check_domain(df, col, check["accepted_values"])

        elif check_type == "regex":
            results[f"regex_{col}"] = check_regex(
                df, col, check["pattern"], check.get("threshold", 0.0)
            )

        elif check_type == "fk":
            results[f"fk_{col}"] = check_fk_validity(
                df, check["parent_df"], col, check["parent_pk"], table_name
            )

        # Quarantine failed records (non-null/non-dup checks)
        last_key = list(results.keys())[-1]
        if not results[last_key].get("passed", True) and QUARANTINE_ENABLED:
            move_to_quarantine(df, table_name, last_key, results[last_key], spark, layer)

    score = record_dq_score(spark, table_name, results, pipeline_name, run_id)
    return {"checks": results, "dq_score": score}


# ── Reconciliation ────────────────────────────────────────────────────────────

def reconcile_counts(spark, source_table: str, target_table: str,
                     source_col: str = None) -> dict:
    """Compare row counts between source and target tables."""
    source_count = spark.read.table(source_table).count()
    target_count = spark.read.table(target_table).count()
    diff = source_count - target_count
    return {
        "source_table":  source_table,
        "target_table":  target_table,
        "source_count":  source_count,
        "target_count":  target_count,
        "difference":    diff,
        "retention_pct": round(target_count / source_count * 100, 2) if source_count > 0 else 0,
        "match":         diff == 0,
    }


# ── Schema Drift Checks ─────────────────────────────────────────────────────────

def check_schema_drift(actual_df, expected_schema: StructType,
                       table_name: str,
                       metadata_columns: list = None) -> dict:
    """
    Compare actual DataFrame columns against expected schema.
    Detects missing columns, unexpected columns, and type mismatches.

    Returns:
    {
        "table": table_name,
        "missing_columns": [...],       # in expected but not in actual
        "unexpected_columns": [...],    # in actual but not in expected (excl metadata)
        "type_mismatches": [{"column": ..., "expected": ..., "actual": ...}],
        "passed": bool
    }
    """
    metadata_columns = metadata_columns or []
    expected_names = {f.name.lower(): f for f in expected_schema.fields}
    actual_names = {c.lower(): c for c in actual_df.columns}

    metadata_lower = {c.lower() for c in metadata_columns}

    missing = []
    for name_lower, field in expected_names.items():
        if name_lower not in actual_names and name_lower not in metadata_lower:
            missing.append(field.name)

    unexpected = []
    actual_type_map = dict(actual_df.dtypes)
    for name_lower, original_name in actual_names.items():
        if name_lower not in expected_names and name_lower not in metadata_lower:
            unexpected.append(original_name)

    type_mismatches = []
    for name_lower, expected_field in expected_names.items():
        if name_lower in actual_type_map and name_lower not in metadata_lower:
            actual_type = actual_type_map[name_lower]
            expected_type = expected_field.dataType.simpleString()
            if actual_type != expected_type:
                type_mismatches.append({
                    "column": expected_field.name,
                    "expected": expected_type,
                    "actual": actual_type,
                })

    passed = len(missing) == 0 and len(unexpected) == 0 and len(type_mismatches) == 0
    return {
        "table": table_name,
        "missing_columns": missing,
        "unexpected_columns": unexpected,
        "type_mismatches": type_mismatches,
        "passed": passed,
    }


def check_rescued_data(spark, table_name: str,
                       rescued_column: str = "_rescued_data") -> dict:
    """
    Check a Delta table for rescued data (schema drift captured by Auto Loader).
    Counts rows with non-null rescued_data and lists the distinct unexpected columns found.

    Returns:
    {
        "table": table_name,
        "rescued_row_count": int,
        "total_row_count": int,
        "rescued_pct": float,
        "distinct_unexpected_columns": [...],  # parsed from _rescued_data JSON
        "passed": bool                         # True if no rescued data
    }
    """
    df = spark.read.table(table_name)
    total = df.count()
    if total == 0:
        return {"table": table_name, "rescued_row_count": 0, "total_row_count": 0,
                "rescued_pct": 0.0, "distinct_unexpected_columns": [],
                "passed": True}

    if rescued_column not in df.columns:
        return {"table": table_name, "rescued_row_count": 0, "total_row_count": total,
                "rescued_pct": 0.0, "distinct_unexpected_columns": [],
                "passed": True}

    rescued_df = df.filter(F.col(rescued_column).isNotNull())
    rescued_count = rescued_df.count()
    rescued_pct = round(rescued_count / total * 100, 4)

    distinct_columns = []
    if rescued_count > 0:
        parsed = (
            rescued_df.select(F.json_keys(F.col(rescued_column)).alias("keys"))
            .select(F.explode("keys").alias("key"))
            .distinct()
            .collect()
        )
        distinct_columns = [row.key for row in parsed]

    return {
        "table": table_name,
        "rescued_row_count": rescued_count,
        "total_row_count": total,
        "rescued_pct": rescued_pct,
        "distinct_unexpected_columns": distinct_columns,
        "passed": rescued_count == 0,
    }


def check_column_presence(df, required_columns: list, table_name: str) -> dict:
    """Verify all required columns exist in the DataFrame."""
    actual = set(c.lower() for c in df.columns)
    required_lower = {c.lower(): c for c in required_columns}
    missing = []
    for name_lower, original_name in required_lower.items():
        if name_lower not in actual:
            missing.append(original_name)
    return {
        "table": table_name,
        "required_columns": required_columns,
        "missing_columns": missing,
        "passed": len(missing) == 0,
    }
