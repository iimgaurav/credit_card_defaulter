"""
Cost guardrails for Databricks Community Edition.
- Tags: Sets cluster usage tags for cost tracking
- Runtime watchdog: Aborts notebooks if running beyond 2-hour CE limit
"""

from pyspark.sql import SparkSession
from datetime import datetime, timedelta


def set_cluster_tags(spark, project="credit_card_defaulter", purpose="pipeline"):
    """Set cluster usage tags for cost tracking."""
    tags = {
        "spark.databricks.clusterUsageTags.project": project,
        "spark.databricks.clusterUsageTags.clusterPurpose": purpose,
        "spark.databricks.clusterUsageTags.environment": "community",
    }
    for k, v in tags.items():
        spark.conf.set(k, v)
    print(f"Cluster tags set: {tags}")


def check_runtime_limit(spark, max_hours=2):
    """
    Check if the current notebook/job has exceeded the runtime limit.
    Aborts with a RuntimeError if the limit is exceeded.

    CE has a 2-hour max runtime per job. This guardrail checks the
    Spark start time and raises an error before the platform kills it,
    allowing for graceful cleanup.

    Usage:
        check_runtime_limit(spark, max_hours=2)
    """
    try:
        start_epoch = spark.conf.get("spark.sql.session.startTime")
    except Exception:
        start_epoch = None
    if start_epoch is None:
        print("Runtime check skipped: cannot determine start time.")
        return

    start_time = datetime.fromtimestamp(int(start_epoch) / 1000)
    elapsed = datetime.utcnow() - start_time
    limit = timedelta(hours=max_hours)

    if elapsed > limit:
        raise RuntimeError(
            f"Runtime guardrail TRIPPED: {elapsed} > {limit} ({max_hours}h max). "
            "Job would be killed by CE platform limit. "
            "Reduce data volume or break into smaller steps."
        )

    remaining = limit - elapsed
    print(f"Runtime check: {elapsed} elapsed, {remaining} remaining (limit {max_hours}h).")
