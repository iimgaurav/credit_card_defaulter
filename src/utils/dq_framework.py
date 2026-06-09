"""Data quality framework with checks, quarantine, scoring, and reconciliation."""

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StructType
from datetime import datetime
import json


# --- Null check ---

def check_nulls(df, columns, table_name, threshold_pct=5.0):
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


# --- PK Duplicate check ---

def check_duplicates(df, pk_columns, table_name, threshold_pct=0.0):
    total = df.count()
    dup_count = df.groupBy(*pk_columns).count().filter(F.col("count") > 1).count()
    pct = (dup_count / total * 100) if total > 0 else 0
    return {
        "duplicate_groups": dup_count,
        "duplicate_pct": round(pct, 2),
        "threshold_pct": threshold_pct,
        "passed": pct <= threshold_pct,
    }


# --- Full-row duplicate check ---

def check_full_row_dedup(df, table_name, exclude_cols=None):
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


# --- FK validity check ---

def check_fk_validity(df_child, df_parent, child_fk, parent_pk, table_name):
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


# --- Range check ---

def check_range(df, column, min_val=None, max_val=None):
    stats = df.agg(
        F.min(column).alias("min_val"),
        F.max(column).alias("max_val"),
    ).collect()[0]
    violations = 0
    passed = True
    if min_val is not None:
        v = df.filter(F.col(column) < min_val).count()
        violations += v
        if v > 0:
            passed = False
    if max_val is not None:
        v = df.filter(F.col(column) > max_val).count()
        violations += v
        if v > 0:
            passed = False
    return {
        "actual_min": float(stats["min_val"]) if stats["min_val"] is not None else None,
        "actual_max": float(stats["max_val"]) if stats["max_val"] is not None else None,
        "expected_min": min_val,
        "expected_max": max_val,
        "violations": violations,
        "passed": passed,
    }


# --- Domain check ---

def check_domain(df, column, accepted_values):
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


# --- Regex check ---

def check_regex(df, column, pattern, threshold_pct=0.0):
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


# --- Quarantine ---

def move_to_quarantine(df, table_name, rule_name, check_details, spark,
                       layer="bronze", bronze_quarantine_table=None, silver_quarantine_table=None):
    target = silver_quarantine_table if layer == "silver" else bronze_quarantine_table
    if not target:
        catalog = spark.conf.get("pipeline.target_catalog", "credit_card_dev")
        schema = "silver" if layer == "silver" else "bronze"
        target = f"{catalog}.{schema}.dq_quarantine"
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


# --- DQ Score ---

def record_dq_score(spark, table_name, checks, pipeline_name, run_id,
                    silver_dq_scores_table=None):
    if not silver_dq_scores_table:
        catalog = spark.conf.get("pipeline.target_catalog", "credit_card_dev")
        silver_dq_scores_table = f"{catalog}.silver.dq_scores"
    failed = sum(1 for c in checks.values() if isinstance(c, dict) and not c.get("passed", True))
    total = len(checks)
    score = round((total - failed) / total * 100, 2) if total > 0 else 100.0
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
    row.write.mode("append").saveAsTable(silver_dq_scores_table)
    return score


# --- Full DQ suite ---

def run_dq_suite(df, table_name, checks_config, spark, pipeline_name, run_id,
                 layer="silver", quarantine_enabled=True,
                 bronze_quarantine_table=None, silver_quarantine_table=None,
                 silver_dq_scores_table=None):
    results = {}
    for check in checks_config:
        check_type = check["type"]
        col = check.get("column")
        if check_type == "null":
            r = check_nulls(df, [col], table_name, check.get("threshold", 5.0))
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
            results[f"regex_{col}"] = check_regex(df, col, check["pattern"], check.get("threshold", 0.0))
        elif check_type == "fk":
            results[f"fk_{col}"] = check_fk_validity(df, check["parent_df"], col, check["parent_pk"], table_name)
        last_key = list(results.keys())[-1]
        if not results[last_key].get("passed", True) and quarantine_enabled:
            move_to_quarantine(df, table_name, last_key, results[last_key], spark, layer,
                               bronze_quarantine_table, silver_quarantine_table)
    score = record_dq_score(spark, table_name, results, pipeline_name, run_id, silver_dq_scores_table)
    return {"checks": results, "dq_score": score}


# --- Reconciliation ---

def reconcile_counts(spark, source_table, target_table, source_col=None):
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


# --- Schema Drift ---

def check_schema_drift(actual_df, expected_schema, table_name, metadata_columns=None):
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


def check_rescued_data(spark, table_name, rescued_column="_rescued_data"):
    df = spark.read.table(table_name)
    total = df.count()
    if total == 0:
        return {"table": table_name, "rescued_row_count": 0, "total_row_count": 0,
                "rescued_pct": 0.0, "distinct_unexpected_columns": [], "passed": True}
    if rescued_column not in df.columns:
        return {"table": table_name, "rescued_row_count": 0, "total_row_count": total,
                "rescued_pct": 0.0, "distinct_unexpected_columns": [], "passed": True}
    rescued_df = df.filter(F.col(rescued_column).isNotNull())
    rescued_count = rescued_df.count()
    rescued_pct = round(rescued_count / total * 100, 4) if total > 0 else 0
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


def check_column_presence(df, required_columns, table_name):
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
