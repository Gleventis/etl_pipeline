"""API routes for the API server."""

import logging
from datetime import datetime
from typing import Generator

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from src.server.models import (
    AnalyticalResultCreate,
    AnalyticalResultListResponse,
    AnalyticalResultResponse,
    CheckpointSavingsAggregateResponse,
    CheckpointSavingsFileResponse,
    FailureStatisticsResponse,
    FileCreate,
    FileInfo,
    FileListResponse,
    FileResponse,
    FileUpdate,
    JobExecutionBatchCreate,
    JobExecutionBatchResponse,
    JobExecutionCreate,
    JobExecutionListResponse,
    JobExecutionResponse,
    JobExecutionUpdate,
    PipelineEfficiencyResponse,
    PipelineEfficiencyStatistic,
    PipelineSummaryResponse,
    RecoveryTimeResponse,
    StepFailureStatistic,
    StepPerformanceResponse,
    StepPerformanceStatistic,
)
from src.services.crud import (
    create_analytical_result,
    create_job_execution,
    create_job_executions_batch,
    create_or_get_file,
    get_analytical_result_by_id,
    get_file_by_id,
    get_job_execution_by_id,
    list_analytical_results,
    list_files,
    list_job_executions,
    update_file,
    update_job_execution,
)
from src.services.database import AnalyticalResults, Files, get_session_factory
from src.services.metrics import (
    calculate_checkpoint_savings,
    calculate_failure_statistics,
    calculate_pipeline_efficiency,
    calculate_pipeline_summary,
    calculate_recovery_time_improvement,
    calculate_step_performance,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["API Server"])


def get_db() -> Generator[Session, None, None]:
    """Provide a database session via FastAPI dependency injection.

    Yields:
        Active SQLAlchemy Session.
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        session.close()


def _file_to_response(file) -> FileResponse:
    """Convert a Files ORM object to a FileResponse.

    Args:
        file: Files ORM instance.

    Returns:
        FileResponse pydantic model.
    """
    return FileResponse(
        file_id=file.id,
        bucket=file.bucket,
        object_name=file.object_name,
        overall_status=file.overall_status,
        total_computation_seconds=file.total_computation_seconds,
        total_elapsed_seconds=file.total_elapsed_seconds,
        retry_count=file.retry_count,
        created_at=file.created_at,
        updated_at=file.updated_at,
    )


@router.post(
    "/files",
    status_code=status.HTTP_201_CREATED,
    response_model=FileResponse,
)
def post_file(
    body: FileCreate,
    session: Session = Depends(get_db),
) -> FileResponse:
    """Create or get a file record (idempotent).

    Args:
        body: File creation request.
        session: Database session.

    Returns:
        The created or existing file.
    """
    file = create_or_get_file(
        session=session,
        bucket=body.bucket,
        object_name=body.object_name,
        overall_status=body.overall_status,
    )
    return _file_to_response(file=file)


@router.get(
    "/files/{file_id}",
    status_code=status.HTTP_200_OK,
    response_model=FileResponse,
)
def get_file(
    file_id: int,
    session: Session = Depends(get_db),
) -> FileResponse:
    """Get a file by ID.

    Args:
        file_id: Primary key of the file.
        session: Database session.

    Returns:
        The file record.

    Raises:
        HTTPException: 404 if file not found.
    """
    file = get_file_by_id(session=session, file_id=file_id)
    if file is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"file with id {file_id} does not exist",
        )
    return _file_to_response(file=file)


@router.get(
    "/files",
    status_code=status.HTTP_200_OK,
    response_model=FileListResponse,
)
def get_files(
    session: Session = Depends(get_db),
    status_filter: str | None = Query(default=None, alias="status"),
    bucket: str | None = Query(default=None),
    object_name_pattern: str | None = Query(default=None),
    retry_count_min: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> FileListResponse:
    """List files with optional filtering and pagination.

    Args:
        session: Database session.
        status_filter: Filter by overall_status.
        bucket: Filter by bucket.
        object_name_pattern: SQL LIKE pattern for object_name.
        retry_count_min: Minimum retry_count.
        limit: Max results.
        offset: Results to skip.

    Returns:
        Paginated file list.
    """
    files, total = list_files(
        session=session,
        status=status_filter,
        bucket=bucket,
        object_name_pattern=object_name_pattern,
        retry_count_min=retry_count_min,
        limit=limit,
        offset=offset,
    )
    return FileListResponse(
        files=[_file_to_response(file=f) for f in files],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.patch(
    "/files/{file_id}",
    status_code=status.HTTP_200_OK,
    response_model=FileResponse,
)
def patch_file(
    file_id: int,
    body: FileUpdate,
    session: Session = Depends(get_db),
) -> FileResponse:
    """Partially update a file record.

    Args:
        file_id: Primary key of the file.
        body: Fields to update.
        session: Database session.

    Returns:
        The updated file.

    Raises:
        HTTPException: 404 if file not found.
    """
    updates = body.model_dump(exclude_none=True)
    file = update_file(session=session, file_id=file_id, updates=updates)
    if file is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"file with id {file_id} does not exist",
        )
    return _file_to_response(file=file)


def _job_execution_to_response(job) -> JobExecutionResponse:
    """Convert a JobExecutions ORM object to a JobExecutionResponse.

    Args:
        job: JobExecutions ORM instance.

    Returns:
        JobExecutionResponse pydantic model.
    """
    return JobExecutionResponse(
        job_execution_id=job.id,
        file_id=job.file_id,
        pipeline_run_id=job.pipeline_run_id,
        step_name=job.step_name,
        status=job.status,
        started_at=job.started_at,
        completed_at=job.completed_at,
        computation_time_seconds=job.computation_time_seconds,
        retry_count=job.retry_count,
        error_message=job.error_message,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


@router.post(
    "/job-executions",
    status_code=status.HTTP_201_CREATED,
    response_model=JobExecutionResponse,
)
def post_job_execution(
    body: JobExecutionCreate,
    session: Session = Depends(get_db),
) -> JobExecutionResponse:
    """Create a single job execution.

    Args:
        body: Job execution creation request.
        session: Database session.

    Returns:
        The created job execution.

    Raises:
        HTTPException: 404 if file_id does not exist.
    """
    try:
        job = create_job_execution(
            session=session,
            file_id=body.file_id,
            pipeline_run_id=body.pipeline_run_id,
            step_name=body.step_name,
            status=body.status,
            retry_count=body.retry_count,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    return _job_execution_to_response(job=job)


@router.post(
    "/job-executions/batch",
    status_code=status.HTTP_201_CREATED,
    response_model=JobExecutionBatchResponse,
)
def post_job_executions_batch(
    body: JobExecutionBatchCreate,
    session: Session = Depends(get_db),
) -> JobExecutionBatchResponse:
    """Create multiple job executions atomically.

    Args:
        body: Batch creation request.
        session: Database session.

    Returns:
        IDs and count of created executions.

    Raises:
        HTTPException: 404 if file_id does not exist.
    """
    try:
        ids = create_job_executions_batch(
            session=session,
            file_id=body.file_id,
            pipeline_run_id=body.pipeline_run_id,
            executions=[e.model_dump() for e in body.executions],
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    return JobExecutionBatchResponse(
        job_execution_ids=ids,
        created_count=len(ids),
    )


@router.get(
    "/job-executions/{job_execution_id}",
    status_code=status.HTTP_200_OK,
    response_model=JobExecutionResponse,
)
def get_job_execution(
    job_execution_id: int,
    session: Session = Depends(get_db),
) -> JobExecutionResponse:
    """Get a job execution by ID.

    Args:
        job_execution_id: Primary key.
        session: Database session.

    Returns:
        The job execution record.

    Raises:
        HTTPException: 404 if not found.
    """
    job = get_job_execution_by_id(
        session=session,
        job_execution_id=job_execution_id,
    )
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"job execution with id {job_execution_id} does not exist",
        )
    return _job_execution_to_response(job=job)


@router.get(
    "/job-executions",
    status_code=status.HTTP_200_OK,
    response_model=JobExecutionListResponse,
)
def get_job_executions(
    session: Session = Depends(get_db),
    file_id: int | None = Query(default=None),
    pipeline_run_id: str | None = Query(default=None),
    step_name: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    retry_count_min: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> JobExecutionListResponse:
    """List job executions with optional filtering and pagination.

    Args:
        session: Database session.
        file_id: Filter by file.
        pipeline_run_id: Filter by pipeline run.
        step_name: Filter by step.
        status_filter: Filter by status.
        retry_count_min: Minimum retry_count.
        limit: Max results.
        offset: Results to skip.

    Returns:
        Paginated job execution list.
    """
    jobs, total = list_job_executions(
        session=session,
        file_id=file_id,
        pipeline_run_id=pipeline_run_id,
        step_name=step_name,
        status=status_filter,
        retry_count_min=retry_count_min,
        limit=limit,
        offset=offset,
    )
    return JobExecutionListResponse(
        job_executions=[_job_execution_to_response(job=j) for j in jobs],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.patch(
    "/job-executions/{job_execution_id}",
    status_code=status.HTTP_200_OK,
    response_model=JobExecutionResponse,
)
def patch_job_execution(
    job_execution_id: int,
    body: JobExecutionUpdate,
    session: Session = Depends(get_db),
) -> JobExecutionResponse:
    """Partially update a job execution.

    Args:
        job_execution_id: Primary key.
        body: Fields to update.
        session: Database session.

    Returns:
        The updated job execution.

    Raises:
        HTTPException: 404 if not found.
    """
    updates = body.model_dump(exclude_none=True)
    job = update_job_execution(
        session=session,
        job_execution_id=job_execution_id,
        updates=updates,
    )
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"job execution with id {job_execution_id} does not exist",
        )
    return _job_execution_to_response(job=job)


def _analytical_result_to_response(
    result: AnalyticalResults,
    file: Files | None = None,
) -> AnalyticalResultResponse:
    """Convert an AnalyticalResults ORM object to a response model.

    Args:
        result: AnalyticalResults ORM instance.
        file: Optional Files ORM instance for nested file_info.

    Returns:
        AnalyticalResultResponse pydantic model.
    """
    file_info = None
    if file is not None:
        file_info = FileInfo(
            file_id=file.id,
            bucket=file.bucket,
            object_name=file.object_name,
        )
    return AnalyticalResultResponse(
        result_id=result.id,
        job_execution_id=result.job_execution_id,
        result_type=result.result_type,
        summary_data=result.summary_data,
        detail_s3_path=result.detail_s3_path,
        computation_time_seconds=result.computation_time_seconds,
        created_at=result.created_at,
        file_info=file_info,
    )


@router.post(
    "/analytical-results",
    status_code=status.HTTP_201_CREATED,
    response_model=AnalyticalResultResponse,
)
def post_analytical_result(
    body: AnalyticalResultCreate,
    session: Session = Depends(get_db),
) -> AnalyticalResultResponse:
    """Create an analytical result.

    Args:
        body: Analytical result creation request.
        session: Database session.

    Returns:
        The created analytical result.

    Raises:
        HTTPException: 404 if job_execution_id does not exist.
    """
    try:
        result = create_analytical_result(
            session=session,
            job_execution_id=body.job_execution_id,
            result_type=body.result_type,
            summary_data=body.summary_data,
            computation_time_seconds=body.computation_time_seconds,
            detail_s3_path=body.detail_s3_path,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    return _analytical_result_to_response(result=result)


@router.get(
    "/analytical-results/{result_id}",
    status_code=status.HTTP_200_OK,
    response_model=AnalyticalResultResponse,
)
def get_analytical_result(
    result_id: int,
    session: Session = Depends(get_db),
) -> AnalyticalResultResponse:
    """Get an analytical result by ID.

    Args:
        result_id: Primary key.
        session: Database session.

    Returns:
        The analytical result record.

    Raises:
        HTTPException: 404 if not found.
    """
    result = get_analytical_result_by_id(session=session, result_id=result_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"analytical result with id {result_id} does not exist",
        )
    return _analytical_result_to_response(result=result)


@router.get(
    "/analytical-results",
    status_code=status.HTTP_200_OK,
    response_model=AnalyticalResultListResponse,
)
def get_analytical_results(
    session: Session = Depends(get_db),
    result_type: str | None = Query(default=None),
    file_id: int | None = Query(default=None),
    taxi_type: str | None = Query(default=None),
    year: str | None = Query(default=None),
    month: str | None = Query(default=None),
    created_at_from: datetime | None = Query(default=None),
    created_at_to: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> AnalyticalResultListResponse:
    """List analytical results with complex filtering.

    Args:
        session: Database session.
        result_type: Filter by result type.
        file_id: Filter by file ID.
        taxi_type: Filter by taxi type from object_name.
        year: Filter by year from object_name.
        month: Filter by month from object_name.
        created_at_from: Results after this timestamp.
        created_at_to: Results before this timestamp.
        limit: Max results.
        offset: Results to skip.

    Returns:
        Paginated analytical results list with file info.
    """
    rows, total = list_analytical_results(
        session=session,
        result_type=result_type,
        file_id=file_id,
        taxi_type=taxi_type,
        year=year,
        month=month,
        created_at_from=created_at_from,
        created_at_to=created_at_to,
        limit=limit,
        offset=offset,
    )
    return AnalyticalResultListResponse(
        results=[_analytical_result_to_response(result=ar, file=f) for ar, f in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/metrics/checkpoint-savings",
    status_code=status.HTTP_200_OK,
    response_model=CheckpointSavingsFileResponse | CheckpointSavingsAggregateResponse,
)
def get_checkpoint_savings(
    session: Session = Depends(get_db),
    file_id: int | None = Query(default=None),
) -> CheckpointSavingsFileResponse | CheckpointSavingsAggregateResponse:
    """Calculate time saved by checkpointing.

    Args:
        session: Database session.
        file_id: If provided, calculate for a specific file. Otherwise aggregate.

    Returns:
        Per-file or aggregate checkpoint savings.

    Raises:
        HTTPException: 404 if file_id provided but not found.
    """
    result = calculate_checkpoint_savings(session=session, file_id=file_id)
    if file_id is not None and not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"file with id {file_id} does not exist",
        )
    if file_id is not None:
        return CheckpointSavingsFileResponse(**result)
    return CheckpointSavingsAggregateResponse(**result)


@router.get(
    "/metrics/failure-statistics",
    status_code=status.HTTP_200_OK,
    response_model=FailureStatisticsResponse,
)
def get_failure_statistics(
    session: Session = Depends(get_db),
) -> FailureStatisticsResponse:
    """Calculate failure rates per analytical step.

    Args:
        session: Database session.

    Returns:
        Failure statistics across all steps.
    """
    stats = calculate_failure_statistics(session=session)
    return FailureStatisticsResponse(
        statistics=[StepFailureStatistic(**s) for s in stats],
    )


@router.get(
    "/metrics/pipeline-summary",
    status_code=status.HTTP_200_OK,
    response_model=PipelineSummaryResponse,
)
def get_pipeline_summary(
    session: Session = Depends(get_db),
) -> PipelineSummaryResponse:
    """Calculate comprehensive pipeline summary for thesis reporting.

    Args:
        session: Database session.

    Returns:
        Pipeline summary metrics.
    """
    result = calculate_pipeline_summary(session=session)
    return PipelineSummaryResponse(**result)


@router.get(
    "/metrics/step-performance",
    status_code=status.HTTP_200_OK,
    response_model=StepPerformanceResponse,
)
def get_step_performance(
    session: Session = Depends(get_db),
) -> StepPerformanceResponse:
    """Calculate average computation time per analytical step.

    Args:
        session: Database session.

    Returns:
        Step-level performance statistics.
    """
    stats = calculate_step_performance(session=session)
    return StepPerformanceResponse(
        statistics=[StepPerformanceStatistic(**s) for s in stats],
    )


@router.get(
    "/metrics/pipeline-efficiency",
    status_code=status.HTTP_200_OK,
    response_model=PipelineEfficiencyResponse,
)
def get_pipeline_efficiency(
    session: Session = Depends(get_db),
) -> PipelineEfficiencyResponse:
    """Calculate pipeline efficiency ratio grouped by overall status.

    Args:
        session: Database session.

    Returns:
        Pipeline efficiency statistics per status group.
    """
    stats = calculate_pipeline_efficiency(session=session)
    return PipelineEfficiencyResponse(
        statistics=[PipelineEfficiencyStatistic(**s) for s in stats],
    )


@router.get(
    "/metrics/recovery-time",
    status_code=status.HTTP_200_OK,
    response_model=RecoveryTimeResponse,
)
def get_recovery_time(
    session: Session = Depends(get_db),
) -> RecoveryTimeResponse:
    """Calculate average recovery time improvement from checkpointing.

    Args:
        session: Database session.

    Returns:
        Recovery time improvement metrics.
    """
    result = calculate_recovery_time_improvement(session=session)
    return RecoveryTimeResponse(**result)


if __name__ == "__main__":
    print(f"Router routes: {[r.path for r in router.routes]}")
