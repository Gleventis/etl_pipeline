"""Tests for the API Server client."""

from unittest.mock import MagicMock, patch

import httpx

from src.services.api_server_client import post_analytical_result

_COMMON_KWARGS = {
    "api_server_url": "http://fake-api-server:8000",
    "job_execution_id": 42,
    "result_type": "descriptive_statistics",
    "summary_data": {"total_rows": 1000},
    "detail_s3_path": "results/yellow/2023/01/descriptive_statistics.parquet",
    "computation_time_seconds": 63.2,
}


def _make_mock_client(mock_client_cls, *, response=None, side_effect=None):
    """Wire up a mock httpx.Client context manager."""
    mock_client = MagicMock()
    if side_effect:
        mock_client.post.side_effect = side_effect
    else:
        mock_client.post.return_value = response
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value = mock_client
    return mock_client


class TestPostAnalyticalResult:
    """Tests for post_analytical_result function."""

    @patch("src.services.api_server_client.httpx.Client")
    def test_success_returns_true(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.raise_for_status.return_value = None
        _make_mock_client(mock_client_cls, response=mock_response)

        result = post_analytical_result(**_COMMON_KWARGS)

        assert result is True

    @patch("src.services.api_server_client.httpx.Client")
    def test_sends_correct_payload(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_client = _make_mock_client(mock_client_cls, response=mock_response)

        post_analytical_result(**_COMMON_KWARGS)

        mock_client.post.assert_called_once_with(
            url="/analytical-results",
            json={
                "job_execution_id": 42,
                "result_type": "descriptive_statistics",
                "summary_data": {"total_rows": 1000},
                "detail_s3_path": "results/yellow/2023/01/descriptive_statistics.parquet",
                "computation_time_seconds": 63.2,
            },
            timeout=25.0,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_passes_base_url_and_verify_false(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        _make_mock_client(mock_client_cls, response=mock_response)

        post_analytical_result(**_COMMON_KWARGS)

        mock_client_cls.assert_called_once_with(
            base_url="http://fake-api-server:8000",
            verify=False,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_http_status_error_returns_false(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="not found",
            request=MagicMock(),
            response=mock_response,
        )
        _make_mock_client(mock_client_cls, response=mock_response)

        result = post_analytical_result(**_COMMON_KWARGS)

        assert result is False

    @patch("src.services.api_server_client.httpx.Client")
    def test_server_error_returns_false(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="server error",
            request=MagicMock(),
            response=mock_response,
        )
        _make_mock_client(mock_client_cls, response=mock_response)

        result = post_analytical_result(**_COMMON_KWARGS)

        assert result is False

    @patch("src.services.api_server_client.httpx.Client")
    def test_network_error_returns_false(self, mock_client_cls):
        _make_mock_client(
            mock_client_cls,
            side_effect=httpx.ConnectError("connection refused"),
        )

        result = post_analytical_result(**_COMMON_KWARGS)

        assert result is False
