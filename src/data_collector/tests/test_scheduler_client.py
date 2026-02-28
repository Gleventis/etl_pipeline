"""Tests for the scheduler client."""

from unittest.mock import MagicMock, patch

import httpx

from src.services.scheduler_client import notify_scheduler


class TestNotifyScheduler:
    """Tests for notify_scheduler function."""

    @patch("src.services.scheduler_client.httpx.Client")
    def test_successful_notification(self, mock_client_cls: MagicMock) -> None:
        """Scheduler accepts the request and returns 200."""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        result = notify_scheduler(
            scheduler_url="http://localhost:8001",
            bucket="data-collector",
            objects=["yellow/2023/01/yellow_tripdata_2023-01.parquet"],
        )

        assert result is True
        mock_client.post.assert_called_once_with(
            url="http://localhost:8001/scheduler/schedule",
            json={
                "bucket": "data-collector",
                "objects": ["yellow/2023/01/yellow_tripdata_2023-01.parquet"],
            },
        )

    def test_empty_objects_skips_notification(self) -> None:
        """No HTTP call when objects list is empty."""
        result = notify_scheduler(
            scheduler_url="http://localhost:8001",
            bucket="data-collector",
            objects=[],
        )

        assert result is True

    @patch("src.services.scheduler_client.httpx.Client")
    def test_http_status_error_returns_false(self, mock_client_cls: MagicMock) -> None:
        """Scheduler returns a 4xx/5xx error."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="server error",
            request=MagicMock(),
            response=mock_response,
        )
        mock_client_cls.return_value = mock_client

        result = notify_scheduler(
            scheduler_url="http://localhost:8001",
            bucket="data-collector",
            objects=["file.parquet"],
        )

        assert result is False

    @patch("src.services.scheduler_client.httpx.Client")
    def test_connection_error_returns_false(self, mock_client_cls: MagicMock) -> None:
        """Scheduler is unreachable."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.side_effect = httpx.ConnectError(message="connection refused")
        mock_client_cls.return_value = mock_client

        result = notify_scheduler(
            scheduler_url="http://localhost:8001",
            bucket="data-collector",
            objects=["file.parquet"],
        )

        assert result is False

    @patch("src.services.scheduler_client.httpx.Client")
    def test_multiple_objects_sent(self, mock_client_cls: MagicMock) -> None:
        """Multiple objects are sent in a single request."""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        objects = [
            "yellow/2023/01/yellow_tripdata_2023-01.parquet",
            "yellow/2023/02/yellow_tripdata_2023-02.parquet",
        ]

        result = notify_scheduler(
            scheduler_url="http://localhost:8001",
            bucket="data-collector",
            objects=objects,
        )

        assert result is True
        call_args = mock_client.post.call_args
        assert call_args.kwargs["json"]["objects"] == objects
