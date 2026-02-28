"""Shared S3 client wrapper for MinIO/S3 operations."""

import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def create_s3_client(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> boto3.client:
    """Create a boto3 S3 client configured for MinIO.

    Args:
        endpoint_url: Full endpoint URL (e.g., 'http://localhost:9000').
        access_key: S3/MinIO access key.
        secret_key: S3/MinIO secret key.

    Returns:
        Configured boto3 S3 client.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def ensure_bucket(client: boto3.client, bucket: str) -> None:
    """Create the bucket if it does not exist.

    Args:
        client: boto3 S3 client.
        bucket: Bucket name.
    """
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)
        logger.info("created bucket: %s", bucket)


def upload_object(
    client: boto3.client,
    bucket: str,
    key: str,
    data: bytes,
) -> str:
    """Upload bytes to S3/MinIO.

    Args:
        client: boto3 S3 client.
        bucket: Target bucket name.
        key: Object key (path within bucket).
        data: Raw bytes to upload.

    Returns:
        The S3 key of the uploaded object.

    Raises:
        ClientError: If the upload fails.
    """
    client.put_object(Bucket=bucket, Key=key, Body=data)
    logger.info("uploaded object: s3://%s/%s (%d bytes)", bucket, key, len(data))
    return key


def download_object(
    client: boto3.client,
    bucket: str,
    key: str,
) -> bytes:
    """Download an object from S3/MinIO.

    Args:
        client: boto3 S3 client.
        bucket: Source bucket name.
        key: Object key.

    Returns:
        Raw bytes of the object.

    Raises:
        ClientError: If the download fails.
    """
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def build_s3_key(
    taxi_type: str,
    year: int,
    month: int,
    file_name: str,
) -> str:
    """Build the S3 object key for a TLC parquet file.

    Args:
        taxi_type: Taxi type string (e.g., 'yellow').
        year: Year of the data.
        month: Month of the data.
        file_name: Original file name.

    Returns:
        S3 key in format '<taxi_type>/<year>/<month>/<file_name>'.
    """
    return f"{taxi_type}/{year}/{month:02d}/{file_name}"


if __name__ == "__main__":
    key = build_s3_key(
        taxi_type="yellow",
        year=2023,
        month=1,
        file_name="yellow_tripdata_2023-01.parquet",
    )
    print(f"S3 key: {key}")
