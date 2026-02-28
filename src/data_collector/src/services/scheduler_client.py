"""HTTP client for notifying the scheduler after successful uploads."""

import logging

import httpx

logger = logging.getLogger(__name__)


def notify_scheduler(
    scheduler_url: str,
    bucket: str,
    objects: list[str],
    timeout: float = 30.0,
) -> bool:
    """Send uploaded object paths to the scheduler to start the pipeline.

    Args:
        scheduler_url: Base URL of the scheduler service.
        bucket: MinIO bucket where files were uploaded.
        objects: List of S3 keys for successfully uploaded files.
        timeout: HTTP request timeout in seconds.

    Returns:
        True if the scheduler accepted the request, False otherwise.
    """
    if not objects:
        logger.info("no objects to schedule, skipping scheduler notification")
        return True

    url = f"{scheduler_url}/scheduler/schedule"
    payload = {"bucket": bucket, "objects": objects}

    try:
        with httpx.Client(timeout=timeout, verify=False) as client:
            response = client.post(url=url, json=payload)
            response.raise_for_status()
        logger.info("scheduler notified: %d objects scheduled", len(objects))
        return True
    except httpx.HTTPStatusError as exc:
        logger.error(
            "scheduler returned error %s: %s",
            exc.response.status_code,
            exc,
        )
        return False
    except httpx.HTTPError as exc:
        logger.error("failed to reach scheduler at %s: %s", url, exc)
        return False


if __name__ == "__main__":
    result = notify_scheduler(
        scheduler_url="http://localhost:8001",
        bucket="data-collector",
        objects=["yellow/2023/01/yellow_tripdata_2023-01.parquet"],
    )
    print(f"Scheduler notified: {result}")
