"""HTTP client for posting analytical results to the API Server."""

import logging

import httpx

logger = logging.getLogger(__name__)


def post_analytical_result(
    *,
    api_server_url: str,
    job_execution_id: int,
    result_type: str,
    summary_data: dict,
    detail_s3_path: str,
    computation_time_seconds: float,
) -> bool:
    """Post an analytical result to the API Server.

    Args:
        api_server_url: Base URL of the API Server.
        job_execution_id: ID of the job execution this result belongs to.
        result_type: Analytical step name (e.g. 'descriptive_statistics').
        summary_data: JSONB summary payload.
        detail_s3_path: S3 object path for detailed parquet output.
        computation_time_seconds: Wall-clock time for the computation.

    Returns:
        True if the result was stored successfully, False otherwise.
    """
    payload = {
        "job_execution_id": job_execution_id,
        "result_type": result_type,
        "summary_data": summary_data,
        "detail_s3_path": detail_s3_path,
        "computation_time_seconds": computation_time_seconds,
    }

    with httpx.Client(base_url=api_server_url, verify=False) as client:
        try:
            response = client.post(
                url="/analytical-results",
                json=payload,
                timeout=25.0,
            )
            response.raise_for_status()
            logger.info(
                "posted analytical result: job_execution_id=%s, result_type=%s",
                job_execution_id,
                result_type,
            )
            return True
        except httpx.HTTPStatusError as exc:
            logger.error(
                "api server returned error: status=%s, job_execution_id=%s, result_type=%s",
                exc.response.status_code,
                job_execution_id,
                result_type,
            )
            return False
        except httpx.HTTPError as exc:
            logger.error(
                "network error posting result: job_execution_id=%s, result_type=%s, error=%s",
                job_execution_id,
                result_type,
                exc,
            )
            return False


if __name__ == "__main__":
    result = post_analytical_result(
        api_server_url="http://localhost:8000",
        job_execution_id=42,
        result_type="descriptive_statistics",
        summary_data={"total_rows": 1000, "avg_fare": 12.5},
        detail_s3_path="results/yellow/2023/01/descriptive_statistics.parquet",
        computation_time_seconds=63.2,
    )
    print(f"Posted: {result}")
