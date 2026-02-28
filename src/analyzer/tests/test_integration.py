"""Integration tests for analyzer endpoints with real MinIO."""

import io
import os
from datetime import datetime
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from src.server.main import app

MINIO_ENDPOINT = f"http://{os.environ.get('MINIO_ENDPOINT', 'localhost:9000')}"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
INPUT_BUCKET = "test-integration-input"
OUTPUT_BUCKET = "test-integration-output"

client = TestClient(app=app)


def _make_yellow_parquet_bytes(*, num_rows: int = 50) -> bytes:
    """Create a realistic Yellow taxi parquet file with numeric data."""
    import numpy as np

    rng = np.random.default_rng(seed=42)
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
    table = df.to_arrow()
    buf = io.BytesIO()
    pq.write_table(table, buf)
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
    # Cleanup both buckets
    for bucket in (INPUT_BUCKET, OUTPUT_BUCKET):
        response = s3_client.list_objects_v2(Bucket=bucket)
        for obj in response.get("Contents", []):
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=bucket)


class TestDescriptiveStatisticsIntegration:
    """End-to-end: upload parquet → call endpoint → verify output in MinIO."""

    @patch(
        "src.services.config.SETTINGS.OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS",
        OUTPUT_BUCKET,
    )
    @patch("src.services.step_executor.post_analytical_result", return_value=True)
    def test_yellow_descriptive_statistics_produces_output(
        self,
        mock_post,
        s3,
    ) -> None:
        """Yellow descriptive statistics writes result parquet to output bucket."""
        from utilities.s3 import download_object, upload_object

        parquet_bytes = _make_yellow_parquet_bytes()
        input_key = "yellow/2023/06/yellow_tripdata_2023-06.parquet"
        upload_object(
            client=s3,
            bucket=INPUT_BUCKET,
            key=input_key,
            data=parquet_bytes,
        )

        response = client.post(
            "/analyze/descriptive-statistics",
            json={
                "input_bucket": INPUT_BUCKET,
                "input_object": input_key,
                "taxi_type": "yellow",
                "job_execution_id": 1,
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["error"] is None

        # Verify output parquet exists in output bucket
        objects = s3.list_objects_v2(Bucket=OUTPUT_BUCKET)
        contents = objects.get("Contents", [])
        assert len(contents) == 1
        output_key = contents[0]["Key"]
        assert output_key.startswith("yellow/1/")

        # Verify output is valid parquet
        output_bytes = download_object(
            client=s3,
            bucket=OUTPUT_BUCKET,
            key=output_key,
        )
        table = pq.read_table(source=io.BytesIO(output_bytes))
        assert table.num_rows > 0

        # Verify API server was called with correct args
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["job_execution_id"] == 1
        assert call_kwargs["result_type"] == "descriptive_statistics"
        assert isinstance(call_kwargs["summary_data"], dict)
        assert call_kwargs["detail_s3_path"].startswith(f"s3://{OUTPUT_BUCKET}/")
        assert call_kwargs["computation_time_seconds"] > 0


def _make_green_parquet_bytes(*, num_rows: int = 50) -> bytes:
    """Create a realistic Green taxi parquet file."""
    import numpy as np

    rng = np.random.default_rng(seed=43)
    now = datetime(2023, 6, 15, 12, 0, 0)

    df = pl.DataFrame(
        {
            "vendorid": rng.integers(low=1, high=3, size=num_rows).tolist(),
            "lpep_pickup_datetime": [now] * num_rows,
            "lpep_dropoff_datetime": [now] * num_rows,
            "store_and_fwd_flag": ["N"] * num_rows,
            "ratecodeid": rng.integers(low=1, high=6, size=num_rows).tolist(),
            "pulocationid": rng.integers(low=1, high=265, size=num_rows).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=num_rows).tolist(),
            "passenger_count": rng.integers(low=1, high=6, size=num_rows).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=20.0, size=num_rows).tolist(),
            "fare_amount": rng.uniform(low=5.0, high=80.0, size=num_rows).tolist(),
            "extra": rng.uniform(low=0.0, high=5.0, size=num_rows).tolist(),
            "mta_tax": [0.5] * num_rows,
            "tip_amount": rng.uniform(low=0.0, high=15.0, size=num_rows).tolist(),
            "tolls_amount": rng.uniform(low=0.0, high=10.0, size=num_rows).tolist(),
            "ehail_fee": [None] * num_rows,
            "improvement_surcharge": [0.3] * num_rows,
            "total_amount": rng.uniform(low=8.0, high=100.0, size=num_rows).tolist(),
            "payment_type": rng.integers(low=1, high=5, size=num_rows).tolist(),
            "trip_type": rng.integers(low=1, high=3, size=num_rows).tolist(),
            "congestion_surcharge": [2.5] * num_rows,
        }
    )
    table = df.to_arrow()
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_fhv_parquet_bytes(*, num_rows: int = 50) -> bytes:
    """Create a realistic FHV taxi parquet file."""
    import numpy as np

    rng = np.random.default_rng(seed=44)
    now = datetime(2023, 6, 15, 12, 0, 0)

    df = pl.DataFrame(
        {
            "dispatching_base_num": ["B00001"] * num_rows,
            "pickup_datetime": [now] * num_rows,
            "dropoff_datetime": [now] * num_rows,
            "pulocationid": rng.integers(low=1, high=265, size=num_rows).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=num_rows).tolist(),
            "sr_flag": rng.choice(a=[0, 1], size=num_rows).tolist(),
            "affiliated_base_number": ["B00001"] * num_rows,
        }
    )
    table = df.to_arrow()
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_fhvhv_parquet_bytes(*, num_rows: int = 50) -> bytes:
    """Create a realistic FHVHV taxi parquet file."""
    import numpy as np

    rng = np.random.default_rng(seed=45)
    now = datetime(2023, 6, 15, 12, 0, 0)

    df = pl.DataFrame(
        {
            "hvfhs_license_num": ["HV0003"] * num_rows,
            "dispatching_base_num": ["B00001"] * num_rows,
            "originating_base_num": ["B00001"] * num_rows,
            "request_datetime": [now] * num_rows,
            "on_scene_datetime": [now] * num_rows,
            "pickup_datetime": [now] * num_rows,
            "dropoff_datetime": [now] * num_rows,
            "pulocationid": rng.integers(low=1, high=265, size=num_rows).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=num_rows).tolist(),
            "trip_miles": rng.uniform(low=0.5, high=20.0, size=num_rows).tolist(),
            "trip_time": rng.integers(low=60, high=3600, size=num_rows).tolist(),
            "base_passenger_fare": rng.uniform(
                low=5.0, high=80.0, size=num_rows
            ).tolist(),
            "tolls": rng.uniform(low=0.0, high=10.0, size=num_rows).tolist(),
            "bcf": rng.uniform(low=0.0, high=3.0, size=num_rows).tolist(),
            "sales_tax": rng.uniform(low=0.0, high=5.0, size=num_rows).tolist(),
            "congestion_surcharge": [2.75] * num_rows,
            "airport_fee": [0.0] * num_rows,
            "tips": rng.uniform(low=0.0, high=15.0, size=num_rows).tolist(),
            "driver_pay": rng.uniform(low=5.0, high=60.0, size=num_rows).tolist(),
            "shared_request_flag": ["N"] * num_rows,
            "shared_match_flag": ["N"] * num_rows,
            "access_a_ride_flag": ["N"] * num_rows,
            "wav_request_flag": ["N"] * num_rows,
            "wav_match_flag": ["N"] * num_rows,
        }
    )
    table = df.to_arrow()
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


class TestAllTaxiTypesIntegration:
    """Integration: each taxi type produces valid output for descriptive statistics."""

    @pytest.mark.parametrize(
        ("taxi_type", "parquet_factory", "input_key"),
        [
            (
                "green",
                _make_green_parquet_bytes,
                "green/2023/06/green_tripdata.parquet",
            ),
            ("fhv", _make_fhv_parquet_bytes, "fhv/2023/06/fhv_tripdata.parquet"),
            (
                "fhvhv",
                _make_fhvhv_parquet_bytes,
                "fhvhv/2023/06/fhvhv_tripdata.parquet",
            ),
        ],
    )
    @patch(
        "src.services.config.SETTINGS.OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS",
        OUTPUT_BUCKET,
    )
    @patch("src.services.step_executor.post_analytical_result", return_value=True)
    def test_taxi_type_descriptive_statistics_produces_output(
        self,
        mock_post,
        s3,
        taxi_type: str,
        parquet_factory,
        input_key: str,
    ) -> None:
        """Each taxi type produces valid parquet output for descriptive statistics."""
        from utilities.s3 import download_object, upload_object

        parquet_bytes = parquet_factory()
        upload_object(
            client=s3,
            bucket=INPUT_BUCKET,
            key=input_key,
            data=parquet_bytes,
        )

        response = client.post(
            "/analyze/descriptive-statistics",
            json={
                "input_bucket": INPUT_BUCKET,
                "input_object": input_key,
                "taxi_type": taxi_type,
                "job_execution_id": 2,
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["error"] is None

        objects = s3.list_objects_v2(Bucket=OUTPUT_BUCKET)
        contents = objects.get("Contents", [])
        assert len(contents) == 1
        output_key = contents[0]["Key"]
        assert output_key.startswith(f"{taxi_type}/2/")

        output_bytes = download_object(
            client=s3,
            bucket=OUTPUT_BUCKET,
            key=output_key,
        )
        table = pq.read_table(source=io.BytesIO(output_bytes))
        assert table.num_rows > 0

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["job_execution_id"] == 2
        assert call_kwargs["result_type"] == "descriptive_statistics"
        assert isinstance(call_kwargs["summary_data"], dict)


class TestFhvFareRevenueSkipIntegration:
    """Integration: FHV fare revenue analysis is skipped (no fare columns)."""

    @patch(
        "src.services.config.SETTINGS.OUTPUT_BUCKET_FARE_REVENUE_ANALYSIS",
        OUTPUT_BUCKET,
    )
    @patch("src.services.step_executor.post_analytical_result", return_value=True)
    def test_fhv_fare_revenue_returns_skipped(
        self,
        mock_post,
        s3,
    ) -> None:
        """FHV fare revenue analysis succeeds with skipped summary."""
        from utilities.s3 import upload_object

        parquet_bytes = _make_fhv_parquet_bytes()
        input_key = "fhv/2023/06/fhv_tripdata.parquet"
        upload_object(
            client=s3,
            bucket=INPUT_BUCKET,
            key=input_key,
            data=parquet_bytes,
        )

        response = client.post(
            "/analyze/fare-revenue-analysis",
            json={
                "input_bucket": INPUT_BUCKET,
                "input_object": input_key,
                "taxi_type": "fhv",
                "job_execution_id": 3,
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["error"] is None

        # Verify API server was called with skipped summary
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["job_execution_id"] == 3
        assert call_kwargs["result_type"] == "fare_revenue_analysis"
        assert call_kwargs["summary_data"]["skipped"] is True
        assert "reason" in call_kwargs["summary_data"]
        assert call_kwargs["computation_time_seconds"] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
