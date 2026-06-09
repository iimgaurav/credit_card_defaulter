"""Unit tests for config.py — mocked SparkSession."""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import sys
sys.path.insert(0, "src")


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.conf.get.return_value = None
    return spark


class TestDetectCatalog:
    def test_spark_conf_primary(self, mock_spark):
        mock_spark.conf.get.side_effect = lambda k, d=None: {
            "pipeline.target_catalog": "credit_card_uat"
        }.get(k, d)
        from src.utils.config import detect_catalog
        assert detect_catalog(mock_spark) == "credit_card_uat"

    def test_spark_conf_fallback(self, mock_spark):
        mock_spark.conf.get.side_effect = lambda k, d=None: {
            "TARGET_CATALOG": "credit_card_prod"
        }.get(k, d)
        from src.utils.config import detect_catalog
        assert detect_catalog(mock_spark) == "credit_card_prod"

    def test_hardcoded_fallback(self, mock_spark):
        from src.utils.config import detect_catalog
        assert detect_catalog(mock_spark) == "credit_card_dev"

    def test_widget_overrides(self, mock_spark):
        mock_spark.conf.get.return_value = "credit_card_dev"
        with patch("src.utils.config.DBUtils") as MockDBUtils:
            mock_dbutils = MagicMock()
            mock_dbutils.widgets.get.return_value = "credit_card_uat"
            MockDBUtils.return_value = mock_dbutils
            from src.utils.config import detect_catalog
            assert detect_catalog(mock_spark) == "credit_card_uat"


class TestMakeConfig:
    def test_returns_dict(self, mock_spark):
        mock_spark.conf.get.return_value = "credit_card_dev"
        from src.utils.config import make_config
        cfg = make_config(mock_spark)
        assert isinstance(cfg, dict)

    def test_expected_keys(self, mock_spark):
        mock_spark.conf.get.return_value = "credit_card_dev"
        from src.utils.config import make_config
        cfg = make_config(mock_spark)
        assert cfg["CATALOG"] == "credit_card_dev"
        assert cfg["BRONZE_SCHEMA"] == "bronze"
        assert cfg["SILVER_SCHEMA"] == "silver"
        assert cfg["GOLD_SCHEMA"] == "gold"
        assert "BRONZE_CRM_CUSTOMER" in cfg
        assert "SILVER_CUSTOMER_CLEAN" in cfg
        assert "GOLD_DIM_CUSTOMER" in cfg
        assert "SPARK_CONFIG" in cfg

    def test_catalog_propagates(self, mock_spark):
        mock_spark.conf.get.side_effect = lambda k, d=None: {
            "pipeline.target_catalog": "credit_card_prod"
        }.get(k, d)
        from src.utils.config import make_config
        cfg = make_config(mock_spark)
        assert "credit_card_prod" in cfg["BRONZE_CRM_CUSTOMER"]
        assert "credit_card_prod" in cfg["WATERMARK_TABLE"]
        assert "credit_card_prod" in cfg["LANDING_VOLUME"]


class TestUpsertTable:
    def test_merge_when_exists(self, mock_spark):
        mock_spark.catalog.tableExists.return_value = True
        from src.utils.config import upsert_table
        mock_df = MagicMock()
        upsert_table(mock_spark, mock_df, "catalog.silver.table", ["pk1"])
        mock_df.createOrReplaceTempView.assert_called_once_with("_upsert_src")
        mock_spark.sql.assert_called_once()
        sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql
        assert "t.pk1 = s.pk1" in sql

    def test_merge_composite_pk(self, mock_spark):
        mock_spark.catalog.tableExists.return_value = True
        from src.utils.config import upsert_table
        mock_df = MagicMock()
        upsert_table(mock_spark, mock_df, "catalog.silver.t", ["pk1", "pk2"])
        sql = mock_spark.sql.call_args[0][0]
        assert "t.pk1 = s.pk1 AND t.pk2 = s.pk2" in sql

    def test_create_when_not_exists(self, mock_spark):
        mock_spark.catalog.tableExists.return_value = False
        from src.utils.config import upsert_table
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = mock_writer
        upsert_table(mock_spark, mock_df, "catalog.silver.t", ["pk1"])
        mock_writer.format.assert_called_once_with("delta")
        mock_writer.mode.assert_called_once_with("overwrite")

    def test_create_with_partition(self, mock_spark):
        mock_spark.catalog.tableExists.return_value = False
        from src.utils.config import upsert_table
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = mock_writer
        upsert_table(mock_spark, mock_df, "catalog.silver.t", ["pk1"], partition_cols=["dt"])
        mock_writer.partitionBy.assert_called_once_with("dt")
