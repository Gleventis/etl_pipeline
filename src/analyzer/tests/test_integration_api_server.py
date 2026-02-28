"""Integration tests for API Server contract and failure handling."""

import io
import os
from datetime import datetime
from unittest.mock import patch

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from src.server.main import app

MINIO_ENDPOINT = f"http://{os.environ.get('MINIO_ENDPOINT', 'localhost:9000')}"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
INPUT_BUCKET = "test-api-contract-input"
OUTPUT_BUCKET = "test-api-contract-output"

client = TestClient(app=app)


def _make_yellow_parquet_bytes(*, num_rows: int = 50) -> bytes:
    """Create a Yellow taxi parquet file for integration testing."""
    rng = np.random.default_rng(seed=99)
    now = datetime(2023, 6, 15, 12, 0, 0)

    df = pl.DataFrame(
        {
            "vendorid": rng.integers(low=1, high=3, size=num_rows).tolist(),
            "tpep_pickup_datetime": [now] * num_rows,
            "tpep_dropoff_datetime": [now] * num_rows,
            "passenger_count": rng.integers(low=1, high=6, size=num_rows).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=20.0, size=num_rows).tolist(),
            "ratecodeid": rng.integers(low=1, high=6, size=num_rows).tolist(),
            "store_and_fwd_flag": ["N"] * num_rows,
            "pulocationid": rng.integers(low=1, high=265, size=num_rows).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=num_rows).tolist(),
            "payment_type": rng.integers(low=1, high=5, size=num_rows).tolist(),
            "fare_amount": rng.uniform(low=5.0, high=80.0, size=num_rows).tolist(),
            "extra": rng.uniform(low=0.0, high=5.0, size=num_rows).tolist(),
            "mta_tax": [0.5] * num_rows,
            "tip_amount": rng.uniform(low=0.0, high=15.0, size=num_rows).tolist(),
            "tolls_amount": rng.uniform(low=0.0, high=10.0, size=num_rows).tolist(),
            "improvement_surcharge": [0.3] * num_rows,
            "total_amount": rng.uniform(low=8.0, high=100.0, size=num_rows).tolist(),
            "congestion_surcharge": [2.5] * num_rows,
            "airport_fee": [0.0] * num_rows,
        }
    )
    buf = io.BytesIO()
    pq.write_table(df.to_arrow(), buf)
    return buf.getvalue()


@pytest.fixture()
def s3():
    """Create an S3 client with input and output buckets, clean up after."""
    from utilities.s3 import create_s3_client, ensure_bucket

    s3_client = create_s3_client(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )
    ensure_bucket(client=s3_client, bucket=INPUT_BUCKET)
    ensure_bucket(client=s3_client, bucket=OUTPUT_BUCKET)
    yield s3_client
    for bucket in (INPUT_BUCKET, OUTPUT_BUCKET):
        response = s3_client.list_objects_v2(Bucket=bucket)
        for obj in response.get("Contents", []):
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=bucket)


@pytest.fixture()
def _upload_yellow(s3):
    """Upload a Yellow parquet file to the input bucket."""
    from utilities.s3 import upload_object

    upload_object(
        client=s3,
        bucket=INPUT_BUCKET,
        key="yellow/2023/06/yellow_tripdata.parquet",
        data=_make_yellow_parquet_bytes(),
    )


_EXPECTED_PAYLOAD_KEYS = {
    "api_server_url",
    "job_execution_id",
    "result_type",
    "summary_data",
    "detail_s3_path",
    "computation_time_seconds",
}


class TestApiServerPayloadContract:
    """Verify POST payload shape matches POST /analytical-results contract."""

    @pytest.mark.usefixtures("_upload_yellow")
    @patch(
        "src.services.config.SETTINGS.OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS",
        OUTPUT_BUCKET,
    )
    @patch("src.services.step_executor.post_analytical_result", return_value=True)
    def test_payload_has_all_required_fields(self, mock_post, s3) -> None:
        """post_analytical_result is called with all fields from AnalyticalResultCreate."""
        client.post(
            "/analyze/descriptive-statistics",
            json={
                "input_bucket": INPUT_BUCKET,
                "input_object": "yellow/2023/06/yellow_tripdata.parquet",
                "taxi_type": "yellow",
                "job_execution_id": 100,
            },
        )

        mock_post.assert_called_once()
        kwargs = mock_post.call_args.kwargs
        assert set(kwargs.keys()) == _EXPECTED_PAYLOAD_KEYS

    @pytest.mark.usefixtures("_upload_yellow")
    @patch(
        "src.services.config.SETTINGS.OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS",
        OUTPUT_BUCKET,
    )
    @patch("src.services.step_executor.post_analytical_result", return_value=True)
    def test_payload_field_types_match_contract(self, mock_post, s3) -> None:
        """Each field has the type expected by AnalyticalResultCreate."""
        client.post(
            "/analyze/descriptive-statistics",
            json={
                "input_bucket": INPUT_BUCKET,
                "input_object": "yellow/2023/06/yellow_tripdata.parquet",
                "taxi_type": "yellow",
                "job_execution_id": 100,
            },
        )

        kwargs = mock_post.call_args.kwargs
        assert isinstance(kwargs["job_execution_id"], int)
        assert isinstance(kwargs["result_type"], str)
        assert len(kwargs["result_type"]) >= 1  # min_length=1
        assert isinstance(kwargs["summary_data"], dict)
        assert isinstance(kwargs["detail_s3_path"], str)
        assert isinstance(kwargs["computation_time_seconds"], float)
        assert kwargs["computation_time_seconds"] >= 0  # ge=0

    @pytest.mark.usefixtures("_upload_yellow")
    @patch(
        "src.services.config.SETTINGS.OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS",
        OUTPUT_BUCKET,
    )
    @patch("src.services.step_executor.post_analytical_result", return_value=True)
    def test_payload_values_are_correct(self, mock_post, s3) -> None:
        """Payload values match the request inputs."""
        client.post(
            "/analyze/descriptive-statistics",
            json={
                "input_bucket": INPUT_BUCKET,
                "input_object": "yellow/2023/06/yellow_tripdata.parquet",
                "taxi_type": "yellow",
                "job_execution_id": 100,
            },
        )

        kwargs = mock_post.call_args.kwargs
        assert kwargs["job_execution_id"] == 100
        assert kwargs["result_type"] == "descriptive_statistics"
        assert kwargs["detail_s3_path"].startswith(f"s3://{OUTPUT_BUCKET}/yellow/100/")
        assert kwargs["detail_s3_path"].endswith(".parquet")


class TestApiServerFailureDoesNotFailStep:
    """API Server failure must not cause the step to fail."""

    @pytest.mark.usefixtures("_upload_yellow")
    @patch(
        "src.services.config.SETTINGS.OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS",
        OUTPUT_BUCKET,
    )
    @patch("src.services.step_executor.post_analytical_result", return_value=False)
    def test_step_succeeds_when_api_post_returns_false(self, mock_post, s3) -> None:
        """Step returns success=True even when API Server post fails."""
        response = client.post(
            "/analyze/descriptive-statistics",
            json={
                "input_bucket": INPUT_BUCKET,
                "input_object": "yellow/2023/06/yellow_tripdata.parquet",
                "taxi_type": "yellow",
                "job_execution_id": 200,
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["error"] is None

        # Detail parquet was still uploaded to S3
        objects = s3.list_objects_v2(Bucket=OUTPUT_BUCKET)
        contents = objects.get("Contents", [])
        assert len(contents) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
