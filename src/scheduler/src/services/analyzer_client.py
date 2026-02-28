"""HTTP client for dispatching analytical jobs to the analyzer service."""

import logging

import httpx
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class AnalyzerRequest(BaseModel):
    """Payload sent to the analyzer service for a single pipeline step."""

    model_config = ConfigDict(frozen=True)

    input_bucket: str = Field(min_length=1)
    input_object: str = Field(min_length=1)
    taxi_type: str = Field(min_length=1)
    job_execution_id: int = Field(ge=1)


class AnalyzerResponse(BaseModel):
    """Response from the analyzer service."""

    model_config = ConfigDict(frozen=True)

    success: bool
    error: str | None = None


def _step_to_endpoint(*, step: str) -> str:
    """Convert a pipeline step name to the analyzer endpoint path.

    Args:
        step: Pipeline step name (e.g. 'descriptive_statistics').

    Returns:
        Endpoint path (e.g. '/analyze/descriptive-statistics').
    """
    return f"/analyze/{step.replace('_', '-')}"


def send_job(
    *,
    analyzer_url: str,
    step: str,
    input_bucket: str,
    input_object: str,
    taxi_type: str,
    job_execution_id: int,
    timeout: float = 60.0,
) -> AnalyzerResponse:
    """Send an analytical job to the analyzer service.

    Args:
        analyzer_url: Base URL of the analyzer service.
        step: Pipeline step name (e.g. 'descriptive_statistics').
        input_bucket: MinIO bucket containing the input file.
        input_object: Object path within the bucket.
        taxi_type: Taxi type (e.g. 'yellow', 'green', 'fhv', 'fhvhv').
        job_execution_id: Job execution ID from the API Server.
        timeout: HTTP request timeout in seconds.

    Returns:
        AnalyzerResponse with success/failure status.
    """
    request = AnalyzerRequest(
        input_bucket=input_bucket,
        input_object=input_object,
        taxi_type=taxi_type,
        job_execution_id=job_execution_id,
    )
    endpoint = _step_to_endpoint(step=step)

    with httpx.Client(base_url=analyzer_url, verify=False) as client:
        try:
            response = client.post(
                url=endpoint,
                json=request.model_dump(),
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
            return AnalyzerResponse(
                success=data.get("success", True),
                error=data.get("error"),
            )
        except httpx.HTTPStatusError as exc:
            logger.error(
                "analyzer returned error: status=%s, object=%s, step=%s",
                exc.response.status_code,
                input_object,
                step,
            )
            return AnalyzerResponse(
                success=False,
                error=f"HTTP {exc.response.status_code}",
            )
        except httpx.HTTPError as exc:
            logger.error(
                "network error calling analyzer: object=%s, step=%s, error=%s",
                input_object,
                step,
                exc,
            )
            return AnalyzerResponse(
                success=False,
                error=str(exc),
            )


if __name__ == "__main__":
    result = send_job(
        analyzer_url="http://localhost:8002",
        step="descriptive_statistics",
        input_bucket="raw-data",
        input_object="yellow/2022/01/yellow_tripdata_2022-01.parquet",
        taxi_type="yellow",
        job_execution_id=1,
    )
    print(f"Result: success={result.success}, error={result.error}")
