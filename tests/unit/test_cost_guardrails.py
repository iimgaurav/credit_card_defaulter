"""Unit tests for cost_guardrails.py — mocked SparkSession."""

import pytest
from unittest.mock import MagicMock, patch
import sys
sys.path.insert(0, "src")


class TestSetClusterTags:
    def test_sets_three_tags(self):
        from src.utils.cost_guardrails import set_cluster_tags
        spark = MagicMock()
        set_cluster_tags(spark)
        assert spark.conf.set.call_count == 3
        calls = [c[0] for c in spark.conf.set.call_args_list]
        assert any("spark.databricks.clusterUsageTags.project" in c for c in calls)
        assert any("spark.databricks.clusterUsageTags.clusterPurpose" in c for c in calls)
        assert any("spark.databricks.clusterUsageTags.environment" in c for c in calls)

    def test_custom_values(self):
        from src.utils.cost_guardrails import set_cluster_tags
        spark = MagicMock()
        set_cluster_tags(spark, project="my_proj", purpose="ml")
        tags = {c[0][0]: c[0][1] for c in spark.conf.set.call_args_list}
        assert "spark.databricks.clusterUsageTags.project" in tags


class TestCheckRuntimeLimit:
    def test_within_limit_prints_message(self):
        from src.utils.cost_guardrails import check_runtime_limit
        from datetime import datetime, timedelta
        spark = MagicMock()
        # Use a recent timestamp (1 hour ago)
        recent_ms = int((datetime.utcnow() - timedelta(hours=1)).timestamp() * 1000)
        spark.conf.get.return_value = str(recent_ms)
        check_runtime_limit(spark, max_hours=2)

    def test_exceeds_limit_raises(self):
        from src.utils.cost_guardrails import check_runtime_limit
        from datetime import datetime, timedelta
        spark = MagicMock()
        # Use a timestamp from 25 hours ago
        old_ms = int((datetime.utcnow() - timedelta(hours=25)).timestamp() * 1000)
        spark.conf.get.return_value = str(old_ms)
        with pytest.raises(RuntimeError, match="Runtime guardrail TRIPPED"):
            check_runtime_limit(spark, max_hours=2)

    def test_no_start_time_prints_message(self):
        from src.utils.cost_guardrails import check_runtime_limit
        spark = MagicMock()
        spark.conf.get.return_value = None
        check_runtime_limit(spark)
