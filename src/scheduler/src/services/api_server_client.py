"""HTTP client for interacting with the API Server service."""

import logging

import httpx

logger = logging.getLogger(__name__)


def create_file_record(
    *,
    api_server_url: str,
    bucket: str,
    object_name: str,
) -> int:
    """Create or update a file record in the API Server.

    Args:
        api_server_url: Base URL of the API Server.
        bucket: MinIO bucket name.
        object_name: Object path within the bucket.

    Returns:
        The file_id of the created/updated record.

    Raises:
        httpx.HTTPStatusError: If the API Server returns an error status.
        httpx.HTTPError: If a network error occurs.
    """
    with httpx.Client(base_url=api_server_url, verify=False) as client:
        response = client.post(
            url="/files",
            json={
                "bucket": bucket,
                "object_name": object_name,
                "overall_status": "pending",
            },
            timeout=25.0,
        )
        response.raise_for_status()
        data = response.json()
        file_id: int = data["file_id"]
        logger.info(
            "created file record: file_id=%d, object=%s",
            file_id,
            object_name,
        )
        return file_id


def create_job_execution(
    *,
    api_server_url: str,
    file_id: int,
    pipeline_run_id: str,
    step_name: str,
) -> int:
    """Create a job execution record in the API Server.

    Args:
        api_server_url: Base URL of the API Server.
        file_id: ID of the file record.
        pipeline_run_id: Unique identifier for the pipeline run.
        step_name: Pipeline step name (e.g. 'descriptive_statistics').

    Returns:
        The job_execution_id of the created record.

    Raises:
        httpx.HTTPStatusError: If the API Server returns an error status.
        httpx.HTTPError: If a network error occurs.
    """
    with httpx.Client(base_url=api_server_url, verify=False) as client:
        response = client.post(
            url="/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": pipeline_run_id,
                "step_name": step_name,
                "status": "pending",
                "retry_count": 0,
            },
            timeout=25.0,
        )
        response.raise_for_status()
        data = response.json()
        job_execution_id: int = data["job_execution_id"]
        logger.info(
            "created job execution: job_execution_id=%d, file_id=%d, step=%s",
            job_execution_id,
            file_id,
            step_name,
        )
        return job_execution_id


def update_job_execution(
    *,
    api_server_url: str,
    job_execution_id: int,
    status: str | None = None,
    started_at: str | None = None,
    completed_at: str | None = None,
    computation_time_seconds: float | None = None,
    error_message: str | None = None,
) -> None:
    """Update a job execution record in the API Server.

    Args:
        api_server_url: Base URL of the API Server.
        job_execution_id: ID of the job execution to update.
        status: New status value.
        started_at: ISO timestamp when step started.
        completed_at: ISO timestamp when step completed.
        computation_time_seconds: Wall-clock computation time.
        error_message: Error message if step failed.

    Raises:
        httpx.HTTPStatusError: If the API Server returns an error status.
        httpx.HTTPError: If a network error occurs.
    """
    payload = {}
    if status is not None:
        payload["status"] = status
    if started_at is not None:
        payload["started_at"] = started_at
    if completed_at is not None:
        payload["completed_at"] = completed_at
    if computation_time_seconds is not None:
        payload["computation_time_seconds"] = computation_time_seconds
    if error_message is not None:
        payload["error_message"] = error_message

    with httpx.Client(base_url=api_server_url, verify=False) as client:
        response = client.patch(
            url=f"/job-executions/{job_execution_id}",
            json=payload,
            timeout=25.0,
        )
        response.raise_for_status()
        logger.info(
            "updated job execution: job_execution_id=%d, status=%s",
            job_execution_id,
            status,
        )


def update_file(
    *,
    api_server_url: str,
    file_id: int,
    overall_status: str | None = None,
    total_computation_seconds: float | None = None,
    total_elapsed_seconds: float | None = None,
    retry_count: int | None = None,
) -> None:
    """Update a file record in the API Server.

    Args:
        api_server_url: Base URL of the API Server.
        file_id: ID of the file to update.
        overall_status: New overall status.
        total_computation_seconds: Cumulative computation time.
        total_elapsed_seconds: Wall-clock elapsed time.
        retry_count: Number of retries.

    Raises:
        httpx.HTTPStatusError: If the API Server returns an error status.
        httpx.HTTPError: If a network error occurs.
    """
    payload = {}
    if overall_status is not None:
        payload["overall_status"] = overall_status
    if total_computation_seconds is not None:
        payload["total_computation_seconds"] = total_computation_seconds
    if total_elapsed_seconds is not None:
        payload["total_elapsed_seconds"] = total_elapsed_seconds
    if retry_count is not None:
        payload["retry_count"] = retry_count

    with httpx.Client(base_url=api_server_url, verify=False) as client:
        response = client.patch(
            url=f"/files/{file_id}",
            json=payload,
            timeout=25.0,
        )
        response.raise_for_status()
        logger.info(
            "updated file: file_id=%d, overall_status=%s",
            file_id,
            overall_status,
        )


def persist_step_dependencies(
    *,
    api_server_url: str,
    pipeline_run_id: str,
    edges: list[tuple[str, str]],
) -> None:
    """Persist DAG edges to the API Server.

    Args:
        api_server_url: Base URL of the API Server.
        pipeline_run_id: Unique identifier for the pipeline run.
        edges: Dependency edges as (step_name, depends_on_step_name) tuples.

    Raises:
        httpx.HTTPStatusError: If the API Server returns an error status.
        httpx.HTTPError: If a network error occurs.
    """
    with httpx.Client(base_url=api_server_url, verify=False) as client:
        response = client.post(
            url="/step-dependencies",
            json={
                "pipeline_run_id": pipeline_run_id,
                "edges": [
                    {
                        "step_name": step_name,
                        "depends_on_step_name": depends_on,
                    }
                    for step_name, depends_on in edges
                ],
            },
            timeout=25.0,
        )
        response.raise_for_status()
        logger.info(
            "persisted step dependencies: pipeline_run_id=%s, edge_count=%d",
            pipeline_run_id,
            len(edges),
        )


if __name__ == "__main__":
    fid = create_file_record(
        api_server_url="http://localhost:8000",
        bucket="raw-data",
        object_name="yellow/2022/01/yellow_tripdata_2022-01.parquet",
    )
    print(f"File ID: {fid}")

    jid = create_job_execution(
        api_server_url="http://localhost:8000",
        file_id=fid,
        pipeline_run_id="test-run-001",
        step_name="descriptive_statistics",
    )
    print(f"Job Execution ID: {jid}")

    update_job_execution(
        api_server_url="http://localhost:8000",
        job_execution_id=jid,
        status="completed",
        computation_time_seconds=42.5,
    )
    print("Job execution updated")

    update_file(
        api_server_url="http://localhost:8000",
        file_id=fid,
        overall_status="completed",
        total_computation_seconds=42.5,
    )
    print("File updated")
