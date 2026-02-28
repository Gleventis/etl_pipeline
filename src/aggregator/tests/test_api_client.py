"""Tests for the API Server HTTP client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.services.api_client import (
    PAGE_SIZE,
    fetch_analytical_results,
    fetch_pipeline_summary,
)


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    """Create a mock httpx response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="error",
            request=MagicMock(spec=httpx.Request),
            response=resp,
        )
    return resp


class TestFetchAnalyticalResults:
    """Tests for fetch_analytical_results."""

    @patch("src.services.api_client.httpx.Client")
    def test_single_page(self, mock_client_cls: MagicMock) -> None:
        """Returns all results when total fits in one page."""
        results = [{"id": 1}, {"id": 2}]
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            json_data={"results": results, "total": 2},
        )
        mock_client_cls.return_value = mock_client

        got = fetch_analytical_results(result_type="descriptive_statistics")

        assert got == results
        mock_client.get.assert_called_once()
        call_params = mock_client.get.call_args[1]["params"]
        assert call_params["result_type"] == "descriptive_statistics"
        assert call_params["limit"] == str(PAGE_SIZE)
        assert call_params["offset"] == "0"

    @patch("src.services.api_client.httpx.Client")
    def test_pagination_multiple_pages(self, mock_client_cls: MagicMock) -> None:
        """Fetches all pages when total exceeds PAGE_SIZE."""
        page1 = [{"id": i} for i in range(PAGE_SIZE)]
        page2 = [{"id": PAGE_SIZE}]
        total = PAGE_SIZE + 1

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = [
            _mock_response(json_data={"results": page1, "total": total}),
            _mock_response(json_data={"results": page2, "total": total}),
        ]
        mock_client_cls.return_value = mock_client

        got = fetch_analytical_results()

        assert len(got) == total
        assert mock_client.get.call_count == 2

    @patch("src.services.api_client.httpx.Client")
    def test_empty_results(self, mock_client_cls: MagicMock) -> None:
        """Returns empty list when no results match."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            json_data={"results": [], "total": 0},
        )
        mock_client_cls.return_value = mock_client

        got = fetch_analytical_results(taxi_type="yellow")

        assert got == []

    @patch("src.services.api_client.httpx.Client")
    def test_all_filters_passed(self, mock_client_cls: MagicMock) -> None:
        """All filter params are forwarded to the API Server."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            json_data={"results": [], "total": 0},
        )
        mock_client_cls.return_value = mock_client

        fetch_analytical_results(
            result_type="temporal_analysis",
            taxi_type="green",
            year="2024",
            month="03",
        )

        call_params = mock_client.get.call_args[1]["params"]
        assert call_params["result_type"] == "temporal_analysis"
        assert call_params["taxi_type"] == "green"
        assert call_params["year"] == "2024"
        assert call_params["month"] == "03"

    @patch("src.services.api_client.httpx.Client")
    def test_none_filters_omitted(self, mock_client_cls: MagicMock) -> None:
        """None-valued filters are not sent as query params."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            json_data={"results": [], "total": 0},
        )
        mock_client_cls.return_value = mock_client

        fetch_analytical_results()

        call_params = mock_client.get.call_args[1]["params"]
        assert "result_type" not in call_params
        assert "taxi_type" not in call_params
        assert "year" not in call_params
        assert "month" not in call_params

    @patch("src.services.api_client.httpx.Client")
    def test_api_server_error_raises(self, mock_client_cls: MagicMock) -> None:
        """Raises HTTPStatusError on non-2xx response."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            json_data={},
            status_code=500,
        )
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            fetch_analytical_results()


class TestFetchPipelineSummary:
    """Tests for fetch_pipeline_summary."""

    @patch("src.services.api_client.httpx.Client")
    def test_returns_summary(self, mock_client_cls: MagicMock) -> None:
        """Returns the pipeline summary dict."""
        summary = {"total_files": 10, "completed": 8}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(json_data=summary)
        mock_client_cls.return_value = mock_client

        got = fetch_pipeline_summary()

        assert got == summary
        mock_client.get.assert_called_once_with(url="/metrics/pipeline-summary")

    @patch("src.services.api_client.httpx.Client")
    def test_api_server_error_raises(self, mock_client_cls: MagicMock) -> None:
        """Raises HTTPStatusError on non-2xx response."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            json_data={},
            status_code=502,
        )
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            fetch_pipeline_summary()
