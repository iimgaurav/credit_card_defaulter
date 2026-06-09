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
        spark = MagicMock()
        spark.conf.get.return_value = str(int(pytest.approx(60000)))
        check_runtime_limit(spark, max_hours=2)

    def test_exceeds_limit_raises(self):
        from src.utils.cost_guardrails import check_runtime_limit
        spark = MagicMock()
        epoch_24h_ago = int(pytest.approx(25 * 3600 * 1000))
        spark.conf.get.return_value = str(epoch_24h_ago)
        with pytest.raises(RuntimeError, match="Runtime guardrail TRIPPED"):
            check_runtime_limit(spark, max_hours=2)

    def test_no_start_time_prints_message(self):
        from src.utils.cost_guardrails import check_runtime_limit
        spark = MagicMock()
        spark.conf.get.return_value = None
        check_runtime_limit(spark)
