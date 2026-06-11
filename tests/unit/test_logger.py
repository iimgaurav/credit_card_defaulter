"""Unit tests for logger.py — mocked SparkSession."""

import pytest
from unittest.mock import MagicMock, patch
import sys
sys.path.insert(0, "src")


class TestPipelineLogger:
    @pytest.fixture
    def mock_spark(self):
        spark = MagicMock()
        spark.conf.get.return_value = "credit_card_dev"
        return spark

    def test_init_defaults(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline")
        assert log.pipeline_name == "test_pipeline"
        assert log.run_id is not None
        assert log.audit_enabled is True
        assert "credit_card_dev.bronze.pipeline_logs" in log.target_table

    def test_init_with_custom_run_id(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline", run_id="my_run")
        assert log.run_id == "my_run"

    def test_init_audit_disabled(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline", audit_enabled=False)
        assert log.audit_enabled is False

    def test_start_task_creates_entry(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline", audit_enabled=False)
        start = log.start_task("ingest")
        assert start is not None
        assert len(log.task_logs) == 1
        assert log.task_logs[0]["task_name"] == "ingest"
        assert log.task_logs[0]["status"] == "STARTED"

    def test_complete_task_creates_entry(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline", audit_enabled=False)
        start = log.start_task("ingest")
        log.complete_task("ingest", start, row_count=100)
        assert len(log.task_logs) == 2
        completed = log.task_logs[1]
        assert completed["task_name"] == "ingest"
        assert completed["status"] == "COMPLETED"
        assert completed["row_count"] == 100

    def test_fail_task_creates_entry(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline", audit_enabled=False)
        start = log.start_task("ingest")
        try:
            raise ValueError("bad data")
        except ValueError as e:
            log.fail_task("ingest", start, e)
        failed = log.task_logs[1]
        assert failed["status"] == "FAILED"
        assert "ValueError" in failed["error_message"]

    def test_summary_counts(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline", audit_enabled=False)
        s1 = log.start_task("task1")
        log.complete_task("task1", s1)
        s2 = log.start_task("task2")
        log.complete_task("task2", s2)
        s3 = log.start_task("task3")
        try:
            raise RuntimeError("fail")
        except RuntimeError as e:
            log.fail_task("task3", s3, e)
        summary = log.summary()
        assert summary["total_tasks"] == 6  # 3 start + 2 complete + 1 fail
        assert summary["completed"] == 2
        assert summary["failed"] == 1
        assert summary["started"] == 3  # 3 started, 2 completed, 1 failed

    def test_get_logs_df(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline", audit_enabled=False)
        log.start_task("ingest")
        df = log.get_logs_df()
        assert df is not None

    def test_audit_writes_to_table(self, mock_spark):
        from src.utils.logger import PipelineLogger
        log = PipelineLogger(mock_spark, "test_pipeline", audit_enabled=True)
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        log.start_task("ingest")
        mock_df.write.mode.assert_called_once_with("append")
        mock_df.write.mode.return_value.format.assert_called_once_with("delta")
