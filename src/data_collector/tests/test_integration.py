"""Integration tests for POST /collect with real MinIO."""

import io
import os
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from src.server.app import app
from src.server.models import TaxiType
from src.services.downloader import DownloadResult
from src.services.schemas import EXPECTED_COLUMNS
from utilities.s3 import create_s3_client, download_object, ensure_bucket

MINIO_ENDPOINT = f"http://{os.environ.get('MINIO_ENDPOINT', 'localhost:9000')}"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
TEST_BUCKET = "test-integration"

COLLECT_URL = "/collector/collect"

client = TestClient(app=app)


def _make_parquet_bytes(taxi_type: TaxiType) -> bytes:
    """Create a minimal valid parquet file for the given taxi type."""
    columns = list(EXPECTED_COLUMNS[taxi_type])
    arrays = [pa.array([1]) for _ in columns]
    table = pa.table(dict(zip(columns, arrays)))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.fixture()
def s3():
    """Create an S3 client and test bucket, clean up after."""
    s3_client = create_s3_client(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )
    ensure_bucket(client=s3_client, bucket=TEST_BUCKET)
    yield s3_client
    # Cleanup
    response = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    for obj in response.get("Contents", []):
        s3_client.delete_object(Bucket=TEST_BUCKET, Key=obj["Key"])
    s3_client.delete_bucket(Bucket=TEST_BUCKET)


class TestCollectIntegration:
    """Integration tests: mock downloads, real MinIO uploads."""

    @patch("src.services.config.SETTINGS.MINIO_BUCKET", TEST_BUCKET)
    @patch("src.server.routes.download_batch")
    def test_uploaded_file_exists_in_minio(
        self,
        mock_download_batch,
        s3,
    ) -> None:
        """Successful download result is uploaded and retrievable from MinIO."""
        parquet_bytes = _make_parquet_bytes(taxi_type=TaxiType.YELLOW)
        mock_download_batch.return_value = [
            DownloadResult(
                url="https://example.com/yellow_tripdata_2023-01.parquet",
                file_name="yellow_tripdata_2023-01.parquet",
                taxi_type=TaxiType.YELLOW,
                year=2023,
                month=1,
                success=True,
                file_bytes=parquet_bytes,
            ),
        ]

        response = client.post(
            COLLECT_URL,
            json={"year": 2023, "month": 1, "taxi_type": "yellow"},
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 1
        assert len(body["failures"]) == 0

        s3_key = body["successes"][0]["s3_key"]
        assert s3_key == "yellow/2023/01/yellow_tripdata_2023-01.parquet"

        downloaded = download_object(client=s3, bucket=TEST_BUCKET, key=s3_key)
        assert downloaded == parquet_bytes

    @patch("src.services.config.SETTINGS.MINIO_BUCKET", TEST_BUCKET)
    @patch("src.server.routes.download_batch")
    def test_partial_failure_uploads_only_successes(
        self,
        mock_download_batch,
        s3,
    ) -> None:
        """Only successful downloads are uploaded; failures are reported."""
        parquet_bytes = _make_parquet_bytes(taxi_type=TaxiType.GREEN)
        mock_download_batch.return_value = [
            DownloadResult(
                url="https://example.com/green_tripdata_2023-01.parquet",
                file_name="green_tripdata_2023-01.parquet",
                taxi_type=TaxiType.GREEN,
                year=2023,
                month=1,
                success=True,
                file_bytes=parquet_bytes,
            ),
            DownloadResult(
                url="https://example.com/green_tripdata_2023-02.parquet",
                file_name="green_tripdata_2023-02.parquet",
                taxi_type=TaxiType.GREEN,
                year=2023,
                month=2,
                success=False,
                error="HTTP 404",
            ),
        ]

        response = client.post(
            COLLECT_URL,
            json={"year": 2023, "month": {"from": 1, "to": 2}, "taxi_type": "green"},
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 1
        assert len(body["failures"]) == 1

        # Verify the successful file is in MinIO
        s3_key = body["successes"][0]["s3_key"]
        downloaded = download_object(client=s3, bucket=TEST_BUCKET, key=s3_key)
        assert downloaded == parquet_bytes

        # Verify the failed file is NOT in MinIO
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError):
            download_object(
                client=s3,
                bucket=TEST_BUCKET,
                key="green/2023/02/green_tripdata_2023-02.parquet",
            )

    @patch("src.services.config.SETTINGS.MINIO_BUCKET", TEST_BUCKET)
    @patch("src.server.routes.download_batch")
    def test_multiple_files_uploaded(
        self,
        mock_download_batch,
        s3,
    ) -> None:
        """Multiple successful downloads are all uploaded to MinIO."""
        yellow_bytes = _make_parquet_bytes(taxi_type=TaxiType.YELLOW)
        green_bytes = _make_parquet_bytes(taxi_type=TaxiType.GREEN)
        mock_download_batch.return_value = [
            DownloadResult(
                url="https://example.com/yellow_tripdata_2023-06.parquet",
                file_name="yellow_tripdata_2023-06.parquet",
                taxi_type=TaxiType.YELLOW,
                year=2023,
                month=6,
                success=True,
                file_bytes=yellow_bytes,
            ),
            DownloadResult(
                url="https://example.com/green_tripdata_2023-06.parquet",
                file_name="green_tripdata_2023-06.parquet",
                taxi_type=TaxiType.GREEN,
                year=2023,
                month=6,
                success=True,
                file_bytes=green_bytes,
            ),
        ]

        response = client.post(
            COLLECT_URL,
            json={"year": 2023, "month": 6, "taxi_type": "all"},
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["successes"]) == 2

        for success in body["successes"]:
            data = download_object(client=s3, bucket=TEST_BUCKET, key=success["s3_key"])
            assert len(data) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
