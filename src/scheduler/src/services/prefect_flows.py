"""Prefect flow and task definitions for pipeline orchestration."""

import logging
import time
from datetime import datetime, timezone

from prefect import flow, task

from src.services.analyzer_client import AnalyzerResponse, send_job
from src.services.api_server_client import (
    create_file_record,
    create_job_execution,
    update_file,
    update_job_execution,
)
from src.services.config import Settings
from src.services.database import get_connection, save_job_state
from src.services.pipeline import STEPS, get_input_bucket, get_next_step
from src.services.taxi_type import extract_taxi_type

logger = logging.getLogger(__name__)


@task
def execute_step(
    *,
    step: str,
    input_bucket: str,
    object_name: str,
    analyzer_url: str,
    taxi_type: str,
    job_execution_id: int,
    timeout: float,
) -> AnalyzerResponse:
    """Execute a single pipeline step by dispatching to the analyzer.

    Args:
        step: Pipeline step name (e.g. 'descriptive_statistics').
        input_bucket: MinIO bucket containing the input file.
        object_name: Object path within the bucket.
        analyzer_url: Base URL of the analyzer service.
        taxi_type: Taxi type (e.g. 'yellow', 'green', 'fhv', 'fhvhv').
        job_execution_id: Job execution ID from the API Server.
        timeout: HTTP request timeout in seconds.

    Returns:
        AnalyzerResponse with success/failure status.
    """
    logger.info(
        "executing step: step=%s, object=%s, bucket=%s, taxi_type=%s, job_execution_id=%d",
        step,
        object_name,
        input_bucket,
        taxi_type,
        job_execution_id,
    )
    return send_job(
        analyzer_url=analyzer_url,
        step=step,
        input_bucket=input_bucket,
        input_object=object_name,
        taxi_type=taxi_type,
        job_execution_id=job_execution_id,
        timeout=timeout,
    )


@flow
def process_file_flow(
    *,
    object_name: str,
    bucket: str,
    settings: Settings,
    db_url: str,
    pipeline_run_id: str,
    start_step: str | None = None,
    skip_checkpoints: list[str] | None = None,
) -> None:
    """Walk a single file through the pipeline steps sequentially.

    Opens its own Postgres connection, persists state at each transition,
    and calls execute_step for each remaining step.

    Args:
        object_name: S3 object path to process.
        bucket: S3 bucket name.
        settings: Scheduler configuration.
        db_url: Postgres connection string.
        pipeline_run_id: Unique identifier for the pipeline run.
        start_step: Step to resume from. If None, starts from the first step.
        skip_checkpoints: Step names for which checkpoint persistence is skipped.
    """
    taxi_type = extract_taxi_type(object_name=object_name)
    file_id = create_file_record(
        api_server_url=settings.API_SERVER_URL,
        bucket=bucket,
        object_name=object_name,
    )

    completed_steps: list[str] = []
    is_resume = start_step is not None
    if is_resume:
        start_index = STEPS.index(start_step)
        completed_steps = list(STEPS[:start_index])

    flow_start = time.monotonic()
    total_computation_seconds = 0.0

    update_file(
        api_server_url=settings.API_SERVER_URL,
        file_id=file_id,
        overall_status="in_progress",
        retry_count=1 if is_resume else None,
    )

    with get_connection(database_url=db_url) as conn:
        next_step = get_next_step(completed_steps=completed_steps)
        save_job_state(
            conn=conn,
            object_name=object_name,
            bucket=bucket,
            current_step=next_step,
            status="in_progress",
            completed_steps=list(completed_steps),
            failed_step=None,
        )

        while next_step is not None:
            job_execution_id = create_job_execution(
                api_server_url=settings.API_SERVER_URL,
                file_id=file_id,
                pipeline_run_id=pipeline_run_id,
                step_name=next_step,
            )

            started_at = datetime.now(tz=timezone.utc).isoformat()
            update_job_execution(
                api_server_url=settings.API_SERVER_URL,
                job_execution_id=job_execution_id,
                status="running",
                started_at=started_at,
            )

            step_start = time.monotonic()
            input_bucket = get_input_bucket(step=next_step, settings=settings)
            response = execute_step(
                step=next_step,
                input_bucket=input_bucket,
                object_name=object_name,
                analyzer_url=settings.ANALYZER_URL,
                taxi_type=taxi_type,
                job_execution_id=job_execution_id,
                timeout=settings.ANALYZER_TIMEOUT,
            )
            computation_time = time.monotonic() - step_start
            total_computation_seconds += computation_time

            if not response.success:
                completed_at = datetime.now(tz=timezone.utc).isoformat()
                update_job_execution(
                    api_server_url=settings.API_SERVER_URL,
                    job_execution_id=job_execution_id,
                    status="failed",
                    completed_at=completed_at,
                    error_message=response.error,
                )
                total_elapsed_seconds = time.monotonic() - flow_start
                update_file(
                    api_server_url=settings.API_SERVER_URL,
                    file_id=file_id,
                    overall_status="failed",
                    total_computation_seconds=total_computation_seconds,
                    total_elapsed_seconds=total_elapsed_seconds,
                )
                save_job_state(
                    conn=conn,
                    object_name=object_name,
                    bucket=bucket,
                    current_step=next_step,
                    status="failed",
                    completed_steps=list(completed_steps),
                    failed_step=next_step,
                )
                logger.error(
                    "step failed: object=%s, step=%s, error=%s",
                    object_name,
                    next_step,
                    response.error,
                )
                return

            completed_at = datetime.now(tz=timezone.utc).isoformat()
            update_job_execution(
                api_server_url=settings.API_SERVER_URL,
                job_execution_id=job_execution_id,
                status="completed",
                completed_at=completed_at,
                computation_time_seconds=computation_time,
            )

            update_file(
                api_server_url=settings.API_SERVER_URL,
                file_id=file_id,
                total_computation_seconds=total_computation_seconds,
            )

            just_completed = next_step
            completed_steps.append(just_completed)
            next_step = get_next_step(completed_steps=completed_steps)
            if just_completed not in (skip_checkpoints or []):
                save_job_state(
                    conn=conn,
                    object_name=object_name,
                    bucket=bucket,
                    current_step=next_step,
                    status="in_progress" if next_step else "completed",
                    completed_steps=list(completed_steps),
                    failed_step=None,
                )

        total_elapsed_seconds = time.monotonic() - flow_start
        update_file(
            api_server_url=settings.API_SERVER_URL,
            file_id=file_id,
            overall_status="completed",
            total_computation_seconds=total_computation_seconds,
            total_elapsed_seconds=total_elapsed_seconds,
        )
        logger.info("pipeline completed: object=%s", object_name)


if __name__ == "__main__":
    result = execute_step(
        step="descriptive_statistics",
        input_bucket="raw-data",
        object_name="yellow/2022/01/yellow_tripdata_2022-01.parquet",
        analyzer_url="http://localhost:8002",
        taxi_type="yellow",
        job_execution_id=1,
        timeout=300.0,
    )
    print(f"Result: success={result.success}, error={result.error}")
