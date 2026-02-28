"""Download and validate NYC TLC parquet files."""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import httpx

from src.server.models import TaxiType
from src.services.schemas import validate_parquet_schema

logger = logging.getLogger(__name__)

_URL_PATTERN = re.compile(
    r"/(?P<taxi_type>[a-z]+)_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.parquet$"
)


@dataclass(frozen=True, slots=True)
class DownloadResult:
    """Result of downloading and validating a single file."""

    url: str
    file_name: str
    taxi_type: TaxiType
    year: int
    month: int
    success: bool
    file_bytes: bytes | None = None
    error: str | None = None


def parse_url_metadata(url: str) -> tuple[str, TaxiType, int, int]:
    """Extract file_name, taxi_type, year, month from a TLC download URL.

    Args:
        url: TLC download URL.

    Returns:
        Tuple of (file_name, taxi_type, year, month).

    Raises:
        ValueError: If the URL does not match the expected pattern.
    """
    match = _URL_PATTERN.search(url)
    if not match:
        msg = f"url does not match TLC pattern: {url}"
        raise ValueError(msg)

    taxi_type = TaxiType(match.group("taxi_type"))
    year = int(match.group("year"))
    month = int(match.group("month"))
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    return file_name, taxi_type, year, month


def download_one(url: str, timeout: float = 60.0) -> DownloadResult:
    """Download a single TLC parquet file and validate it.

    Args:
        url: TLC download URL.
        timeout: HTTP request timeout in seconds.

    Returns:
        DownloadResult with success/failure status and validated file bytes.
    """
    try:
        file_name, taxi_type, year, month = parse_url_metadata(url=url)
    except ValueError as exc:
        return DownloadResult(
            url=url,
            file_name=url.rsplit("/", maxsplit=1)[-1],
            taxi_type=TaxiType.YELLOW,
            year=0,
            month=0,
            success=False,
            error=str(exc),
        )

    try:
        with httpx.Client(timeout=timeout, verify=False) as client:
            response = client.get(url=url)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        logger.error("http error downloading %s: %s", url, exc)
        return DownloadResult(
            url=url,
            file_name=file_name,
            taxi_type=taxi_type,
            year=year,
            month=month,
            success=False,
            error=f"HTTP {exc.response.status_code}",
        )
    except httpx.HTTPError as exc:
        logger.error("network error downloading %s: %s", url, exc)
        return DownloadResult(
            url=url,
            file_name=file_name,
            taxi_type=taxi_type,
            year=year,
            month=month,
            success=False,
            error=str(exc),
        )

    file_bytes = response.content
    if len(file_bytes) == 0:
        return DownloadResult(
            url=url,
            file_name=file_name,
            taxi_type=taxi_type,
            year=year,
            month=month,
            success=False,
            error="empty response body",
        )

    schema_errors = validate_parquet_schema(file_bytes=file_bytes, taxi_type=taxi_type)
    if schema_errors:
        return DownloadResult(
            url=url,
            file_name=file_name,
            taxi_type=taxi_type,
            year=year,
            month=month,
            success=False,
            error=f"schema validation failed: {'; '.join(schema_errors)}",
        )

    logger.info("downloaded and validated %s (%d bytes)", file_name, len(file_bytes))
    return DownloadResult(
        url=url,
        file_name=file_name,
        taxi_type=taxi_type,
        year=year,
        month=month,
        success=True,
        file_bytes=file_bytes,
    )


def download_batch(
    urls: list[str],
    pool_size: int = 4,
    timeout: float = 60.0,
) -> list[DownloadResult]:
    """Download multiple TLC parquet files concurrently.

    Args:
        urls: List of TLC download URLs.
        pool_size: Maximum number of concurrent download threads.
        timeout: HTTP request timeout per file in seconds.

    Returns:
        List of DownloadResult for each URL.
    """
    if not urls:
        return []

    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        futures = {
            executor.submit(download_one, url=url, timeout=timeout): url for url in urls
        }
        for future in as_completed(futures):
            results.append(future.result())

    return results


if __name__ == "__main__":
    result = download_one(
        url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )
    print(
        f"Result: success={result.success}, file={result.file_name}, error={result.error}"
    )
