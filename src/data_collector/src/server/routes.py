"""Routes for the data collector API."""

import logging

from fastapi import APIRouter, status

from src.server.models import (
    CollectRequest,
    CollectResponse,
    FileFailure,
    FileSuccess,
)
from src.services.config import SETTINGS
from src.services.downloader import download_batch
from src.services.scheduler_client import notify_scheduler
from src.services.url_generator import generate_urls
from utilities.s3 import build_s3_key, create_s3_client, ensure_bucket, upload_object

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/collector", tags=["Data Collector"])


@router.post(
    "/collect",
    status_code=status.HTTP_200_OK,
    response_model=CollectResponse,
)
def collect(request: CollectRequest) -> CollectResponse:
    """Download, validate, and upload NYC TLC parquet files to MinIO.

    Args:
        request: Collection parameters (year, month, taxi_type).

    Returns:
        CollectResponse with per-file success/failure details.
    """
    years = request.year.expand()
    months = request.month.expand()
    urls = generate_urls(years=years, months=months, taxi_type=request.taxi_type)

    logger.info("starting collection: %d urls", len(urls))

    results = download_batch(
        urls=urls,
        pool_size=SETTINGS.THREAD_POOL_SIZE,
    )

    s3_client = create_s3_client(
        endpoint_url=f"http://{SETTINGS.MINIO_ENDPOINT}",
        access_key=SETTINGS.MINIO_ACCESS_KEY,
        secret_key=SETTINGS.MINIO_SECRET_KEY,
    )
    ensure_bucket(client=s3_client, bucket=SETTINGS.MINIO_BUCKET)

    successes: list[FileSuccess] = []
    failures: list[FileFailure] = []

    for result in results:
        if not result.success:
            failures.append(
                FileFailure(
                    file_name=result.file_name, reason=result.error or "unknown"
                )
            )
            continue

        s3_key = build_s3_key(
            taxi_type=result.taxi_type,
            year=result.year,
            month=result.month,
            file_name=result.file_name,
        )

        try:
            upload_object(
                client=s3_client,
                bucket=SETTINGS.MINIO_BUCKET,
                key=s3_key,
                data=result.file_bytes,
            )
            successes.append(FileSuccess(file_name=result.file_name, s3_key=s3_key))
        except Exception as exc:
            logger.error("failed to upload %s: %s", result.file_name, exc)
            failures.append(
                FileFailure(file_name=result.file_name, reason=f"upload failed: {exc}")
            )

    logger.info(
        "collection complete: %d successes, %d failures",
        len(successes),
        len(failures),
    )

    if successes:
        uploaded_keys = [s.s3_key for s in successes]
        notify_scheduler(
            scheduler_url=SETTINGS.SCHEDULER_URL,
            bucket=SETTINGS.MINIO_BUCKET,
            objects=uploaded_keys,
        )

    return CollectResponse(successes=successes, failures=failures)


if __name__ == "__main__":
    print(f"Router prefix: {router.prefix}")
    print(f"Router routes: {[r.path for r in router.routes]}")
