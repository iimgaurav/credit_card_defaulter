"""Unit tests for watermark.py — mocked SparkSession."""

import pytest
from unittest.mock import MagicMock, patch, call
import sys
sys.path.insert(0, "src")


class TestWatermark:
    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        spark.conf.get.return_value = "credit_card_dev"
        return spark

    def test_init_creates_schema_and_table(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="credit_card_dev")
        assert wm.CATALOG == "credit_card_dev"
        assert wm.WATERMARK_TABLE == "credit_card_dev.control.watermark"
        mock_spark.sql.assert_any_call("CREATE SCHEMA IF NOT EXISTS credit_card_dev.control")
        create_sql = mock_spark.sql.call_args_list[1][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in create_sql
        assert "credit_card_dev.control.watermark" in create_sql

    def test_get_returns_ts(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="c")
        mock_read = MagicMock()
        mock_spark.read.table.return_value = mock_read
        mock_read.filter.return_value = mock_read
        mock_read.select.return_value = mock_read
        mock_read.collect.return_value = [MagicMock(last_processed_ts="2026-01-15 10:00:00")]
        result = wm.get("customer_clean")
        assert result == "2026-01-15 10:00:00"

    def test_get_returns_default_when_no_rows(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="c")
        mock_read = MagicMock()
        mock_spark.read.table.return_value = mock_read
        mock_read.filter.return_value = mock_read
        mock_read.select.return_value = mock_read
        mock_read.collect.return_value = []
        result = wm.get("customer_clean")
        assert result == "1900-01-01 00:00:00"

    def test_get_returns_default_when_ts_is_none(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="c")
        mock_read = MagicMock()
        mock_spark.read.table.return_value = mock_read
        mock_read.filter.return_value = mock_read
        mock_read.select.return_value = mock_read
        mock_read.collect.return_value = [MagicMock(last_processed_ts=None)]
        result = wm.get("customer_clean")
        assert result == "1900-01-01 00:00:00"

    def test_get_date_returns_date(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="c")
        mock_read = MagicMock()
        mock_spark.read.table.return_value = mock_read
        mock_read.filter.return_value = mock_read
        mock_read.select.return_value = mock_read
        mock_read.collect.return_value = [MagicMock(last_processed_date="2026-01-15")]
        result = wm.get_date("customer_clean")
        assert result == "2026-01-15"

    def test_get_date_default(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="c")
        mock_read = MagicMock()
        mock_spark.read.table.return_value = mock_read
        mock_read.filter.return_value = mock_read
        mock_read.select.return_value = mock_read
        mock_read.collect.return_value = []
        result = wm.get_date("customer_clean")
        assert result == "1900-01-01"

    def test_update_merges(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="c")
        mock_spark.createDataFrame.return_value = MagicMock()
        wm.update("customer_clean", new_ts="2026-01-15", run_id="run_001")
        mock_spark.createDataFrame.assert_called_once()
        args = mock_spark.createDataFrame.call_args[0][0][0]
        assert args["table_name"] == "customer_clean"
        assert args["last_processed_ts"] == "2026-01-15"
        assert args["last_run_id"] == "run_001"
        mock_spark.sql.assert_any_call()
        sql_calls = [c[0][0] for c in mock_spark.sql.call_args_list if "MERGE INTO" in c[0][0]]
        assert len(sql_calls) >= 1

    def test_update_accepts_date(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="c")
        mock_spark.createDataFrame.return_value = MagicMock()
        wm.update("customer_clean", new_date="2026-01-15", run_id="run_002")
        args = mock_spark.createDataFrame.call_args[0][0][0]
        assert args["last_processed_date"] == "2026-01-15"
        assert args["last_processed_ts"] is None

    def test_list_all(self, mock_spark):
        from src.utils.watermark import Watermark
        wm = Watermark(mock_spark, catalog="c")
        mock_read = MagicMock()
        mock_spark.read.table.return_value = mock_read
        result = wm.list_all()
        assert result is mock_read
        mock_spark.read.table.assert_called_once_with("c.control.watermark")
