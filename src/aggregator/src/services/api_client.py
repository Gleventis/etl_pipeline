"""HTTP client wrapper for fetching data from the API Server."""

import logging

import httpx

from src.services.config import SETTINGS

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000


def _build_params(
    result_type: str | None = None,
    taxi_type: str | None = None,
    year: str | None = None,
    month: str | None = None,
) -> dict[str, str]:
    """Build query parameters, omitting None values."""
    params: dict[str, str] = {}
    if result_type is not None:
        params["result_type"] = result_type
    if taxi_type is not None:
        params["taxi_type"] = taxi_type
    if year is not None:
        params["year"] = year
    if month is not None:
        params["month"] = month
    return params


def fetch_analytical_results(
    result_type: str | None = None,
    taxi_type: str | None = None,
    year: str | None = None,
    month: str | None = None,
) -> list[dict]:
    """Fetch all analytical results from the API Server with auto-pagination.

    Args:
        result_type: Filter by result type (e.g. 'descriptive_statistics').
        taxi_type: Filter by taxi type (e.g. 'yellow').
        year: Filter by year extracted from object name.
        month: Filter by month extracted from object name.

    Returns:
        List of analytical result dicts.

    Raises:
        httpx.HTTPStatusError: When the API Server returns a non-2xx response.
    """
    params = _build_params(
        result_type=result_type,
        taxi_type=taxi_type,
        year=year,
        month=month,
    )
    all_results: list[dict] = []
    offset = 0

    with httpx.Client(
        base_url=SETTINGS.API_SERVER_URL,
        timeout=SETTINGS.REQUEST_TIMEOUT,
    ) as client:
        while True:
            page_params = {**params, "limit": str(PAGE_SIZE), "offset": str(offset)}
            logger.debug("fetching analytical results: params=%s", page_params)

            response = client.get(url="/analytical-results", params=page_params)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            all_results.extend(results)

            total = data.get("total", 0)
            offset += PAGE_SIZE
            if offset >= total:
                break

    logger.info(
        "fetched %d analytical results: result_type=%s, taxi_type=%s",
        len(all_results),
        result_type,
        taxi_type,
    )
    return all_results


def fetch_pipeline_summary() -> dict:
    """Fetch the pipeline summary from the API Server.

    Returns:
        Pipeline summary dict.

    Raises:
        httpx.HTTPStatusError: When the API Server returns a non-2xx response.
    """
    with httpx.Client(
        base_url=SETTINGS.API_SERVER_URL,
        timeout=SETTINGS.REQUEST_TIMEOUT,
    ) as client:
        logger.debug("fetching pipeline summary")
        response = client.get(url="/metrics/pipeline-summary")
        response.raise_for_status()

    logger.info("fetched pipeline summary")
    return response.json()


if __name__ == "__main__":
    results = fetch_analytical_results(
        result_type="descriptive_statistics",
        taxi_type="yellow",
    )
    print(f"Fetched {len(results)} results")

    summary = fetch_pipeline_summary()
    print(f"Pipeline summary: {summary}")
