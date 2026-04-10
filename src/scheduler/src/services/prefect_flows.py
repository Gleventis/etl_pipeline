"""Prefect flow and task definitions for pipeline orchestration."""

import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone

from prefect import flow, task

from src.server.models import StepDefinition
from src.services.analyzer_client import AnalyzerResponse, send_job
from src.services.api_server_client import (
    create_file_record,
    create_job_execution,
    persist_step_dependencies,
    update_file,
    update_job_execution,
)
from src.services.config import Settings
from src.services.dag import get_ready_steps
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


def _run_dag(
    *,
    dag_step_names: list[str],
    dag_edges: list[tuple[str, str]],
    name_to_action: dict[str, str],
    name_to_checkpoint: dict[str, bool],
    dag_steps_json: list[dict[str, object]] | None,
    completed_steps: list[str],
    conn: object,
    object_name: str,
    bucket: str,
    file_id: int,
    pipeline_run_id: str,
    settings: Settings,
    taxi_type: str,
    flow_start: float,
    total_computation_seconds: float,
) -> None:
    """Execute pipeline steps using DAG-aware parallel batches.

    Submits all ready steps concurrently via Prefect task submission,
    waits for the batch to complete, then proceeds to the next batch.
    Stops on first failure.

    Args:
        dag_step_names: All step names in the DAG.
        dag_edges: Dependency edges as (step, depends_on) tuples.
        name_to_action: Mapping from step name to analyzer action.
        name_to_checkpoint: Mapping from step name to checkpoint flag.
        dag_steps_json: Serialized DAG step definitions for persistence, or None.
        completed_steps: Steps already completed (mutated in place).
        conn: Database connection for job state persistence.
        object_name: S3 object path.
        bucket: S3 bucket name.
        file_id: File record ID from the API server.
        pipeline_run_id: Unique pipeline run identifier.
        settings: Scheduler configuration.
        taxi_type: Taxi type string.
        flow_start: Monotonic timestamp of flow start.
        total_computation_seconds: Accumulated computation time.
    """
    ready = get_ready_steps(
        all_steps=dag_step_names,
        edges=dag_edges,
        completed_steps=set(completed_steps),
    )
    save_job_state(
        conn=conn,
        object_name=object_name,
        bucket=bucket,
        current_step=ready[0] if ready else None,
        status="in_progress",
        completed_steps=list(completed_steps),
        failed_step=None,
        dag_steps=dag_steps_json,
    )

    while ready:
        # --- prepare and submit batch concurrently --------------------------
        batch_meta: list[tuple[str, int, float]] = []
        futures: list[object] = []

        for step_name in ready:
            action = name_to_action[step_name]
            job_execution_id = create_job_execution(
                api_server_url=settings.API_SERVER_URL,
                file_id=file_id,
                pipeline_run_id=pipeline_run_id,
                step_name=step_name,
            )
            started_at = datetime.now(tz=timezone.utc).isoformat()
            update_job_execution(
                api_server_url=settings.API_SERVER_URL,
                job_execution_id=job_execution_id,
                status="running",
                started_at=started_at,
            )
            step_start = time.monotonic()
            input_bucket = get_input_bucket(step=action, settings=settings)

            future = execute_step.submit(
                step=action,
                input_bucket=input_bucket,
                object_name=object_name,
                analyzer_url=settings.ANALYZER_URL,
                taxi_type=taxi_type,
                job_execution_id=job_execution_id,
                timeout=settings.ANALYZER_TIMEOUT,
            )
            batch_meta.append((step_name, job_execution_id, step_start))
            futures.append(future)

        # --- collect results ------------------------------------------------
        failed_step_name: str | None = None
        failed_error: str | None = None

        for i, (step_name, job_execution_id, step_start) in enumerate(batch_meta):
            response = futures[i].result()
            computation_time = time.monotonic() - step_start
            total_computation_seconds += computation_time

            if response.success:
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
                completed_steps.append(step_name)
            else:
                completed_at = datetime.now(tz=timezone.utc).isoformat()
                update_job_execution(
                    api_server_url=settings.API_SERVER_URL,
                    job_execution_id=job_execution_id,
                    status="failed",
                    completed_at=completed_at,
                    error_message=response.error,
                )
                if failed_step_name is None:
                    failed_step_name = step_name
                    failed_error = response.error

        # --- handle batch failure -------------------------------------------
        if failed_step_name is not None:
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
                current_step=failed_step_name,
                status="failed",
                completed_steps=list(completed_steps),
                failed_step=failed_step_name,
                dag_steps=dag_steps_json,
            )
            logger.error(
                "step failed: object=%s, step=%s, error=%s",
                object_name,
                failed_step_name,
                failed_error,
            )
            return

        # --- checkpoint batch ---------------------------------------------------
        batch_has_checkpoint = any(name_to_checkpoint[s] for s in ready)
        if batch_has_checkpoint:
            next_ready = get_ready_steps(
                all_steps=dag_step_names,
                edges=dag_edges,
                completed_steps=set(completed_steps),
            )
            save_job_state(
                conn=conn,
                object_name=object_name,
                bucket=bucket,
                current_step=next_ready[0] if next_ready else None,
                status="in_progress" if next_ready else "completed",
                completed_steps=list(completed_steps),
                failed_step=None,
                dag_steps=dag_steps_json,
            )

        ready = get_ready_steps(
            all_steps=dag_step_names,
            edges=dag_edges,
            completed_steps=set(completed_steps),
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


def _run_linear(
    *,
    resolve_next: Callable[[list[str]], str | None],
    action_for: Callable[[str], str],
    should_checkpoint: Callable[[str], bool],
    completed_steps: list[str],
    conn: object,
    object_name: str,
    bucket: str,
    file_id: int,
    pipeline_run_id: str,
    settings: Settings,
    taxi_type: str,
    flow_start: float,
    total_computation_seconds: float,
) -> None:
    """Execute pipeline steps sequentially using the linear STEPS list.

    Args:
        resolve_next: Callable returning the next step name or None.
        action_for: Callable mapping step name to analyzer action.
        should_checkpoint: Callable returning whether to checkpoint a step.
        completed_steps: Steps already completed (mutated in place).
        conn: Database connection for job state persistence.
        object_name: S3 object path.
        bucket: S3 bucket name.
        file_id: File record ID from the API server.
        pipeline_run_id: Unique pipeline run identifier.
        settings: Scheduler configuration.
        taxi_type: Taxi type string.
        flow_start: Monotonic timestamp of flow start.
        total_computation_seconds: Accumulated computation time.
    """
    next_step = resolve_next(completed_steps)
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
        action = action_for(next_step)

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
        input_bucket = get_input_bucket(step=action, settings=settings)
        response = execute_step(
            step=action,
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

        completed_steps.append(next_step)
        next_step = resolve_next(completed_steps)
        if should_checkpoint(completed_steps[-1]):
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
    steps: list[StepDefinition] | None = None,
    initial_completed_steps: list[str] | None = None,
) -> None:
    """Walk a single file through the pipeline steps sequentially.

    Opens its own Postgres connection, persists state at each transition,
    and calls execute_step for each remaining step.

    When ``steps`` is provided the flow uses DAG-aware ordering via
    ``get_ready_steps``; otherwise it falls back to the linear ``STEPS``
    list.

    Args:
        object_name: S3 object path to process.
        bucket: S3 bucket name.
        settings: Scheduler configuration.
        db_url: Postgres connection string.
        pipeline_run_id: Unique identifier for the pipeline run.
        start_step: Step to resume from (linear mode only). If None, starts
            from the first step.
        skip_checkpoints: Step names for which checkpoint persistence is
            skipped (linear mode only).
        steps: Optional DAG step definitions. When provided, step ordering
            and checkpoint behaviour are derived from the DAG structure.
        initial_completed_steps: Pre-populated completed steps for DAG
            resume. When provided, used directly instead of reconstructing
            from start_step.
    """
    # --- DAG pre-computation ------------------------------------------------
    dag_step_names: list[str] | None = None
    dag_edges: list[tuple[str, str]] | None = None
    name_to_action: dict[str, str] | None = None
    name_to_checkpoint: dict[str, bool] | None = None
    dag_steps_json: list[dict[str, object]] | None = None

    if steps is not None:
        dag_step_names = [s.name for s in steps]
        dag_edges = [(s.name, dep) for s in steps for dep in s.after]
        name_to_action = {s.name: s.action.lower() for s in steps}
        name_to_checkpoint = {s.name: s.checkpoint for s in steps}
        dag_steps_json = [s.model_dump() for s in steps]

    def _resolve_next(completed: list[str]) -> str | None:
        if dag_step_names is not None and dag_edges is not None:
            ready = get_ready_steps(
                all_steps=dag_step_names,
                edges=dag_edges,
                completed_steps=set(completed),
            )
            return ready[0] if ready else None
        return get_next_step(completed_steps=completed)

    def _action_for(step_name: str) -> str:
        if name_to_action is not None:
            return name_to_action[step_name]
        return step_name

    def _should_checkpoint(step_name: str) -> bool:
        if name_to_checkpoint is not None:
            return name_to_checkpoint[step_name]
        return step_name not in (skip_checkpoints or [])

    # --- flow body ----------------------------------------------------------
    taxi_type = extract_taxi_type(object_name=object_name)
    file_id = create_file_record(
        api_server_url=settings.API_SERVER_URL,
        bucket=bucket,
        object_name=object_name,
    )

    completed_steps: list[str] = []
    is_resume = start_step is not None or initial_completed_steps is not None
    if initial_completed_steps is not None:
        completed_steps = list(initial_completed_steps)
    elif start_step is not None:
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
        if steps is not None:
            if dag_edges:
                try:
                    persist_step_dependencies(
                        api_server_url=settings.API_SERVER_URL,
                        pipeline_run_id=pipeline_run_id,
                        edges=dag_edges,
                    )
                except Exception:
                    logger.exception(
                        "failed to persist step dependencies: pipeline_run_id=%s",
                        pipeline_run_id,
                    )

            _run_dag(
                dag_step_names=dag_step_names,
                dag_edges=dag_edges,
                name_to_action=name_to_action,
                name_to_checkpoint=name_to_checkpoint,
                dag_steps_json=dag_steps_json,
                completed_steps=completed_steps,
                conn=conn,
                object_name=object_name,
                bucket=bucket,
                file_id=file_id,
                pipeline_run_id=pipeline_run_id,
                settings=settings,
                taxi_type=taxi_type,
                flow_start=flow_start,
                total_computation_seconds=total_computation_seconds,
            )
        else:
            _run_linear(
                resolve_next=_resolve_next,
                action_for=_action_for,
                should_checkpoint=_should_checkpoint,
                completed_steps=completed_steps,
                conn=conn,
                object_name=object_name,
                bucket=bucket,
                file_id=file_id,
                pipeline_run_id=pipeline_run_id,
                settings=settings,
                taxi_type=taxi_type,
                flow_start=flow_start,
                total_computation_seconds=total_computation_seconds,
            )


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
