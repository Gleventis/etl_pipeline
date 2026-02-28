"""Tests for the data collector /collect route."""

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.server.app import app
from src.server.models import TaxiType
from src.services.downloader import DownloadResult

client = TestClient(app=app)

COLLECT_URL = "/collector/collect"


def _make_success_result(
    taxi_type: str = "yellow", year: int = 2023, month: int = 1
) -> DownloadResult:
    """Create a successful DownloadResult for testing."""
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    return DownloadResult(
        url=f"https://example.com/{file_name}",
        file_name=file_name,
        taxi_type=TaxiType(taxi_type),
        year=year,
        month=month,
        success=True,
        file_bytes=b"fake-parquet-data",
    )


def _make_failure_result(
    taxi_type: str = "yellow", year: int = 2023, month: int = 1
) -> DownloadResult:
    """Create a failed DownloadResult for testing."""
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    return DownloadResult(
        url=f"https://example.com/{file_name}",
        file_name=file_name,
        taxi_type=TaxiType(taxi_type),
        year=year,
        month=month,
        success=False,
        error="HTTP 404",
    )


@patch("src.server.routes.notify_scheduler")
@patch("src.server.routes.upload_object")
@patch("src.server.routes.ensure_bucket")
@patch("src.server.routes.create_s3_client")
@patch("src.server.routes.download_batch")
class TestCollectRoute:
    """Tests for POST /collector/collect."""

    def test_successful_collection(
        self,
        mock_download_batch: MagicMock,
        mock_create_s3_client: MagicMock,
        mock_ensure_bucket: MagicMock,
        mock_upload_object: MagicMock,
        mock_notify_scheduler: MagicMock,
    ) -> None:
        """All files download and upload successfully."""
        mock_download_batch.return_value = [_make_success_result()]
        mock_create_s3_client.return_value = MagicMock()
        mock_upload_object.return_value = (
            "yellow/2023/01/yellow_tripdata_2023-01.parquet"
        )
        mock_notify_scheduler.return_value = True

        response = client.post(
            COLLECT_URL,
            json={
                "year": 2023,
                "month": 1,
                "taxi_type": "yellow",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 1
        assert len(body["failures"]) == 0
        assert body["successes"][0]["file_name"] == "yellow_tripdata_2023-01.parquet"
        mock_notify_scheduler.assert_called_once()

    def test_partial_failure(
        self,
        mock_download_batch: MagicMock,
        mock_create_s3_client: MagicMock,
        mock_ensure_bucket: MagicMock,
        mock_upload_object: MagicMock,
        mock_notify_scheduler: MagicMock,
    ) -> None:
        """Some files fail download, others succeed."""
        mock_download_batch.return_value = [
            _make_success_result(),
            _make_failure_result(month=2),
        ]
        mock_create_s3_client.return_value = MagicMock()
        mock_notify_scheduler.return_value = True

        response = client.post(
            COLLECT_URL,
            json={
                "year": 2023,
                "month": {"from": 1, "to": 2},
                "taxi_type": "yellow",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 1
        assert len(body["failures"]) == 1
        assert body["failures"][0]["reason"] == "HTTP 404"
        mock_notify_scheduler.assert_called_once()

    def test_all_failures(
        self,
        mock_download_batch: MagicMock,
        mock_create_s3_client: MagicMock,
        mock_ensure_bucket: MagicMock,
        mock_upload_object: MagicMock,
        mock_notify_scheduler: MagicMock,
    ) -> None:
        """All files fail download."""
        mock_download_batch.return_value = [_make_failure_result()]
        mock_create_s3_client.return_value = MagicMock()

        response = client.post(
            COLLECT_URL,
            json={
                "year": 2023,
                "month": 1,
                "taxi_type": "yellow",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 0
        assert len(body["failures"]) == 1
        mock_notify_scheduler.assert_not_called()

    def test_upload_failure_reported(
        self,
        mock_download_batch: MagicMock,
        mock_create_s3_client: MagicMock,
        mock_ensure_bucket: MagicMock,
        mock_upload_object: MagicMock,
        mock_notify_scheduler: MagicMock,
    ) -> None:
        """Download succeeds but upload fails — reported as failure."""
        mock_download_batch.return_value = [_make_success_result()]
        mock_create_s3_client.return_value = MagicMock()
        mock_upload_object.side_effect = Exception("connection refused")

        response = client.post(
            COLLECT_URL,
            json={
                "year": 2023,
                "month": 1,
                "taxi_type": "yellow",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 0
        assert len(body["failures"]) == 1
        assert "upload failed" in body["failures"][0]["reason"]
        mock_notify_scheduler.assert_not_called()

    def test_empty_url_list(
        self,
        mock_download_batch: MagicMock,
        mock_create_s3_client: MagicMock,
        mock_ensure_bucket: MagicMock,
        mock_upload_object: MagicMock,
        mock_notify_scheduler: MagicMock,
    ) -> None:
        """Request with valid params but download_batch returns empty."""
        mock_download_batch.return_value = []
        mock_create_s3_client.return_value = MagicMock()

        response = client.post(
            COLLECT_URL,
            json={
                "year": 2023,
                "month": 1,
                "taxi_type": "yellow",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 0
        assert len(body["failures"]) == 0
        mock_notify_scheduler.assert_not_called()

    def test_invalid_request_body(
        self,
        mock_download_batch: MagicMock,
        mock_create_s3_client: MagicMock,
        mock_ensure_bucket: MagicMock,
        mock_upload_object: MagicMock,
        mock_notify_scheduler: MagicMock,
    ) -> None:
        """Invalid request body returns 422."""
        response = client.post(
            COLLECT_URL,
            json={"year": "not-a-number", "month": 1, "taxi_type": "yellow"},
        )

        assert response.status_code == 422

    def test_year_range_request(
        self,
        mock_download_batch: MagicMock,
        mock_create_s3_client: MagicMock,
        mock_ensure_bucket: MagicMock,
        mock_upload_object: MagicMock,
        mock_notify_scheduler: MagicMock,
    ) -> None:
        """Year range generates correct number of URL calls."""
        mock_download_batch.return_value = [
            _make_success_result(year=2022),
            _make_success_result(year=2023),
        ]
        mock_create_s3_client.return_value = MagicMock()
        mock_notify_scheduler.return_value = True

        response = client.post(
            COLLECT_URL,
            json={
                "year": {"from": 2022, "to": 2023},
                "month": 1,
                "taxi_type": "yellow",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 2

        call_args = mock_download_batch.call_args
        assert len(call_args.kwargs["urls"]) == 2
        mock_notify_scheduler.assert_called_once()
