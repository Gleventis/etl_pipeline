"""Tests for the shared S3 utility."""

import os

import pytest

from s3 import (
    build_s3_key,
    create_s3_client,
    download_object,
    ensure_bucket,
    upload_object,
)

MINIO_ENDPOINT = f"http://{os.environ.get('MINIO_ENDPOINT', 'localhost:9000')}"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
TEST_BUCKET = "test-s3-utility"


@pytest.fixture()
def s3_client():
    """Create an S3 client connected to the MinIO instance."""
    return create_s3_client(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    )


@pytest.fixture()
def bucket(s3_client):
    """Ensure a clean test bucket exists."""
    ensure_bucket(client=s3_client, bucket=TEST_BUCKET)
    yield TEST_BUCKET
    # Cleanup: delete all objects then the bucket
    response = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    for obj in response.get("Contents", []):
        s3_client.delete_object(Bucket=TEST_BUCKET, Key=obj["Key"])
    s3_client.delete_bucket(Bucket=TEST_BUCKET)


class TestBuildS3Key:
    """Tests for build_s3_key."""

    def test_formats_key_correctly(self):
        key = build_s3_key(
            taxi_type="yellow",
            year=2023,
            month=1,
            file_name="yellow_tripdata_2023-01.parquet",
        )
        assert key == "yellow/2023/01/yellow_tripdata_2023-01.parquet"

    def test_pads_single_digit_month(self):
        key = build_s3_key(
            taxi_type="green",
            year=2020,
            month=3,
            file_name="green_tripdata_2020-03.parquet",
        )
        assert key == "green/2020/03/green_tripdata_2020-03.parquet"

    def test_double_digit_month(self):
        key = build_s3_key(
            taxi_type="fhv",
            year=2021,
            month=12,
            file_name="fhv_tripdata_2021-12.parquet",
        )
        assert key == "fhv/2021/12/fhv_tripdata_2021-12.parquet"


class TestEnsureBucket:
    """Tests for ensure_bucket with real MinIO."""

    def test_creates_bucket_if_missing(self, s3_client):
        bucket_name = "test-ensure-create"
        ensure_bucket(client=s3_client, bucket=bucket_name)

        response = s3_client.head_bucket(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Cleanup
        s3_client.delete_bucket(Bucket=bucket_name)

    def test_idempotent_on_existing_bucket(self, s3_client):
        bucket_name = "test-ensure-idempotent"
        s3_client.create_bucket(Bucket=bucket_name)

        # Should not raise
        ensure_bucket(client=s3_client, bucket=bucket_name)

        # Cleanup
        s3_client.delete_bucket(Bucket=bucket_name)


class TestUploadAndDownload:
    """Integration tests for upload_object and download_object."""

    def test_upload_and_download_roundtrip(self, s3_client, bucket):
        data = b"hello world parquet bytes"
        key = "yellow/2023/01/test_file.parquet"

        returned_key = upload_object(
            client=s3_client, bucket=bucket, key=key, data=data
        )
        assert returned_key == key

        downloaded = download_object(client=s3_client, bucket=bucket, key=key)
        assert downloaded == data

    def test_upload_empty_bytes(self, s3_client, bucket):
        key = "green/2020/06/empty.parquet"

        upload_object(client=s3_client, bucket=bucket, key=key, data=b"")

        downloaded = download_object(client=s3_client, bucket=bucket, key=key)
        assert downloaded == b""

    def test_download_nonexistent_key_raises(self, s3_client, bucket):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError):
            download_object(
                client=s3_client, bucket=bucket, key="does/not/exist.parquet"
            )
