"""Tests for the analyzer client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.services.analyzer_client import (
    AnalyzerRequest,
    AnalyzerResponse,
    _step_to_endpoint,
    send_job,
)


class TestAnalyzerRequest:
    """Tests for the AnalyzerRequest model."""

    def test_creates_valid_request(self):
        req = AnalyzerRequest(
            input_bucket="raw-data",
            input_object="yellow/2022/01/yellow_tripdata_2022-01.parquet",
            taxi_type="yellow",
            job_execution_id=42,
        )
        assert req.input_bucket == "raw-data"
        assert req.input_object == "yellow/2022/01/yellow_tripdata_2022-01.parquet"
        assert req.taxi_type == "yellow"
        assert req.job_execution_id == 42

    def test_is_frozen(self):
        req = AnalyzerRequest(
            input_bucket="raw-data",
            input_object="file.parquet",
            taxi_type="green",
            job_execution_id=1,
        )
        with pytest.raises(Exception):
            req.taxi_type = "other"

    def test_rejects_empty_input_bucket(self):
        with pytest.raises(Exception):
            AnalyzerRequest(
                input_bucket="",
                input_object="file.parquet",
                taxi_type="yellow",
                job_execution_id=1,
            )

    def test_rejects_zero_job_execution_id(self):
        with pytest.raises(Exception):
            AnalyzerRequest(
                input_bucket="raw-data",
                input_object="file.parquet",
                taxi_type="yellow",
                job_execution_id=0,
            )


class TestAnalyzerResponse:
    """Tests for the AnalyzerResponse model."""

    def test_success_response(self):
        resp = AnalyzerResponse(success=True)
        assert resp.success is True
        assert resp.error is None

    def test_failure_response(self):
        resp = AnalyzerResponse(success=False, error="something broke")
        assert resp.success is False
        assert resp.error == "something broke"

    def test_is_frozen(self):
        resp = AnalyzerResponse(success=True)
        with pytest.raises(Exception):
            resp.success = False


class TestStepToEndpoint:
    """Tests for the _step_to_endpoint helper."""

    @pytest.mark.parametrize(
        "step,expected",
        [
            ("descriptive_statistics", "/analyze/descriptive-statistics"),
            ("data_cleaning", "/analyze/data-cleaning"),
            ("temporal_analysis", "/analyze/temporal-analysis"),
            ("geospatial_analysis", "/analyze/geospatial-analysis"),
            ("fare_revenue_analysis", "/analyze/fare-revenue-analysis"),
        ],
    )
    def test_converts_step_to_endpoint(self, step: str, expected: str):
        assert _step_to_endpoint(step=step) == expected


class TestSendJob:
    """Tests for the send_job function."""

    @patch("src.services.analyzer_client.httpx.Client")
    def test_successful_job(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = send_job(
            analyzer_url="http://localhost:8002",
            step="descriptive_statistics",
            input_bucket="raw-data",
            input_object="yellow/2022/01/file.parquet",
            taxi_type="yellow",
            job_execution_id=42,
        )

        assert result.success is True
        assert result.error is None
        mock_client.post.assert_called_once_with(
            url="/analyze/descriptive-statistics",
            json={
                "input_bucket": "raw-data",
                "input_object": "yellow/2022/01/file.parquet",
                "taxi_type": "yellow",
                "job_execution_id": 42,
            },
            timeout=60.0,
        )

    @patch("src.services.analyzer_client.httpx.Client")
    def test_routes_to_correct_endpoint_per_step(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        send_job(
            analyzer_url="http://localhost:8002",
            step="fare_revenue_analysis",
            input_bucket="cleaned-data",
            input_object="file.parquet",
            taxi_type="fhvhv",
            job_execution_id=99,
        )

        mock_client.post.assert_called_once_with(
            url="/analyze/fare-revenue-analysis",
            json={
                "input_bucket": "cleaned-data",
                "input_object": "file.parquet",
                "taxi_type": "fhvhv",
                "job_execution_id": 99,
            },
            timeout=60.0,
        )

    @patch("src.services.analyzer_client.httpx.Client")
    def test_analyzer_returns_failure(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {"success": False, "error": "bad data"}
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = send_job(
            analyzer_url="http://localhost:8002",
            step="data_cleaning",
            input_bucket="raw-data",
            input_object="file.parquet",
            taxi_type="green",
            job_execution_id=10,
        )

        assert result.success is False
        assert result.error == "bad data"

    @patch("src.services.analyzer_client.httpx.Client")
    def test_http_status_error(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="server error",
            request=MagicMock(),
            response=mock_response,
        )
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = send_job(
            analyzer_url="http://localhost:8002",
            step="temporal_analysis",
            input_bucket="cleaned-data",
            input_object="file.parquet",
            taxi_type="fhv",
            job_execution_id=5,
        )

        assert result.success is False
        assert "500" in result.error

    @patch("src.services.analyzer_client.httpx.Client")
    def test_network_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.post.side_effect = httpx.ConnectError("connection refused")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = send_job(
            analyzer_url="http://localhost:8002",
            step="geospatial_analysis",
            input_bucket="cleaned-data",
            input_object="file.parquet",
            taxi_type="yellow",
            job_execution_id=7,
        )

        assert result.success is False
        assert "connection refused" in result.error

    @patch("src.services.analyzer_client.httpx.Client")
    def test_custom_timeout(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        send_job(
            analyzer_url="http://localhost:8002",
            step="fare_revenue_analysis",
            input_bucket="cleaned-data",
            input_object="file.parquet",
            taxi_type="yellow",
            job_execution_id=1,
            timeout=120.0,
        )

        mock_client.post.assert_called_once_with(
            url="/analyze/fare-revenue-analysis",
            json={
                "input_bucket": "cleaned-data",
                "input_object": "file.parquet",
                "taxi_type": "yellow",
                "job_execution_id": 1,
            },
            timeout=120.0,
        )

    @patch("src.services.analyzer_client.httpx.Client")
    def test_passes_base_url_and_verify_false(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        send_job(
            analyzer_url="http://analyzer:8002",
            step="descriptive_statistics",
            input_bucket="raw-data",
            input_object="file.parquet",
            taxi_type="yellow",
            job_execution_id=1,
        )

        mock_client_cls.assert_called_once_with(
            base_url="http://analyzer:8002",
            verify=False,
        )
