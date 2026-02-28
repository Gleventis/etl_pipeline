"""Tests for the step executor orchestration logic."""

from unittest.mock import MagicMock, patch

import pytest

from src.server.models import (
    AnalyzeRequest,
    AnalyzeResponse,
    StepName,
    StepResult,
    TaxiType,
)
from src.services.config import Settings
from src.services.step_executor import execute_step

_REQUEST = AnalyzeRequest(
    input_bucket="raw-data",
    input_object="yellow/2023/01/yellow_tripdata_2023-01.parquet",
    taxi_type=TaxiType.YELLOW,
    job_execution_id=42,
)


@pytest.fixture()
def settings():
    """Provide a Settings instance with test defaults."""
    return Settings(
        MINIO_ENDPOINT="minio:9000",
        MINIO_ACCESS_KEY="minioadmin",
        MINIO_SECRET_KEY="minioadmin",
        API_SERVER_URL="http://api-server:8000",
        OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS="desc-stats-bucket",
    )


@pytest.fixture()
def _mock_s3():
    """Patch all s3 utility functions used by step_executor."""
    with (
        patch("src.services.step_executor.create_s3_client") as mock_create,
        patch("src.services.step_executor.download_object") as mock_download,
        patch("src.services.step_executor.ensure_bucket") as mock_ensure,
        patch("src.services.step_executor.upload_object") as mock_upload,
    ):
        mock_create.return_value = MagicMock(name="s3_client")
        mock_download.return_value = b"fake-parquet-bytes"
        yield {
            "create": mock_create,
            "download": mock_download,
            "ensure": mock_ensure,
            "upload": mock_upload,
        }


@pytest.fixture()
def _mock_polars():
    """Patch polars.read_parquet to return a fake DataFrame."""
    with patch("src.services.step_executor.pl.read_parquet") as mock_read:
        mock_read.return_value = MagicMock(name="dataframe", columns=["a", "b"])
        mock_read.return_value.__len__ = lambda self: 100
        yield mock_read


@pytest.fixture()
def _mock_analyzer():
    """Patch get_analyzer to return a mock analyzer."""
    with patch("src.services.step_executor.get_analyzer") as mock_get:
        mock_instance = MagicMock(name="analyzer")
        mock_instance.analyze.return_value = StepResult(
            summary_data={"total_rows": 100},
            detail_bytes=b"detail-parquet",
            detail_s3_key="descriptive_statistics_detail.parquet",
        )
        mock_get.return_value = mock_instance
        yield {"get": mock_get, "instance": mock_instance}


@pytest.fixture()
def _mock_post():
    """Patch post_analytical_result."""
    with patch("src.services.step_executor.post_analytical_result") as mock_post:
        mock_post.return_value = True
        yield mock_post


class TestExecuteStepSuccess:
    """Tests for the happy path of execute_step."""

    @pytest.mark.usefixtures("_mock_s3", "_mock_polars", "_mock_analyzer", "_mock_post")
    def test_returns_success(self, settings):
        result = execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        assert result == AnalyzeResponse(success=True)

    def test_downloads_from_correct_bucket_and_key(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        _mock_s3["download"].assert_called_once_with(
            client=_mock_s3["create"].return_value,
            bucket="raw-data",
            key="yellow/2023/01/yellow_tripdata_2023-01.parquet",
        )

    def test_resolves_correct_analyzer(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        _mock_analyzer["get"].assert_called_once_with(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            taxi_type=TaxiType.YELLOW,
        )

    def test_calls_analyze_with_dataframe(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        _mock_analyzer["instance"].analyze.assert_called_once_with(
            df=_mock_polars.return_value,
        )

    def test_uploads_detail_to_output_bucket(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        _mock_s3["ensure"].assert_called_once_with(
            client=_mock_s3["create"].return_value,
            bucket="desc-stats-bucket",
        )
        _mock_s3["upload"].assert_called_once_with(
            client=_mock_s3["create"].return_value,
            bucket="desc-stats-bucket",
            key="yellow/42/descriptive_statistics_detail.parquet",
            data=b"detail-parquet",
        )

    def test_posts_summary_to_api_server(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        _mock_post.assert_called_once_with(
            api_server_url="http://api-server:8000",
            job_execution_id=42,
            result_type="descriptive_statistics",
            summary_data={"total_rows": 100},
            detail_s3_path="s3://desc-stats-bucket/yellow/42/descriptive_statistics_detail.parquet",
            computation_time_seconds=pytest.approx(0.0, abs=5.0),
        )


class TestExecuteStepApiPostFailure:
    """API post failure should NOT fail the step."""

    def test_returns_success_when_post_fails(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        _mock_post.return_value = False

        result = execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        assert result == AnalyzeResponse(success=True)


class TestExecuteStepErrors:
    """Tests for error handling in execute_step."""

    def test_download_failure_returns_error(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        _mock_s3["download"].side_effect = Exception("download failed")

        result = execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        assert result.success is False
        assert "download failed" in result.error

    def test_analysis_failure_returns_error(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        _mock_analyzer["instance"].analyze.side_effect = Exception("analysis crashed")

        result = execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        assert result.success is False
        assert "analysis crashed" in result.error

    def test_upload_failure_returns_error(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        _mock_s3["upload"].side_effect = Exception("upload failed")

        result = execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        assert result.success is False
        assert "upload failed" in result.error

    def test_analysis_failure_does_not_upload(
        self, settings, _mock_s3, _mock_polars, _mock_analyzer, _mock_post
    ):
        _mock_analyzer["instance"].analyze.side_effect = Exception("boom")

        execute_step(
            step_name=StepName.DESCRIPTIVE_STATISTICS,
            request=_REQUEST,
            settings=settings,
        )

        _mock_s3["upload"].assert_not_called()
        _mock_post.assert_not_called()
