"""CRUD operations for the API server database."""

import logging
import re
from datetime import datetime

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from src.services.database import (
    AnalyticalResults,
    Files,
    JobExecutions,
    StepDependencies,
)

logger = logging.getLogger(__name__)


def create_or_get_file(
    session: Session,
    bucket: str,
    object_name: str,
    overall_status: str = "pending",
) -> Files:
    """Create a file record or return existing one if (bucket, object_name) exists.

    Args:
        session: Active SQLAlchemy session.
        bucket: S3 bucket name.
        object_name: S3 object key.
        overall_status: Initial status for new files.

    Returns:
        The existing or newly created Files record.
    """
    existing = session.execute(
        select(Files).where(
            Files.bucket == bucket,
            Files.object_name == object_name,
        )
    ).scalar_one_or_none()

    if existing is not None:
        return existing

    file = Files(
        bucket=bucket,
        object_name=object_name,
        overall_status=overall_status,
    )
    session.add(file)
    session.commit()
    session.refresh(file)
    return file


def get_file_by_id(session: Session, file_id: int) -> Files | None:
    """Get a file by its ID.

    Args:
        session: Active SQLAlchemy session.
        file_id: Primary key of the file.

    Returns:
        Files record or None if not found.
    """
    return session.get(Files, file_id)


def list_files(
    session: Session,
    status: str | None = None,
    bucket: str | None = None,
    object_name_pattern: str | None = None,
    retry_count_min: int | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[Files], int]:
    """List files with optional filtering and pagination.

    Args:
        session: Active SQLAlchemy session.
        status: Filter by overall_status.
        bucket: Filter by bucket.
        object_name_pattern: SQL LIKE pattern for object_name.
        retry_count_min: Minimum retry_count filter.
        limit: Max results to return.
        offset: Number of results to skip.

    Returns:
        Tuple of (list of Files, total count matching filters).
    """
    query = select(Files)
    count_query = select(func.count()).select_from(Files)

    if status is not None:
        query = query.where(Files.overall_status == status)
        count_query = count_query.where(Files.overall_status == status)
    if bucket is not None:
        query = query.where(Files.bucket == bucket)
        count_query = count_query.where(Files.bucket == bucket)
    if object_name_pattern is not None:
        query = query.where(Files.object_name.like(object_name_pattern))
        count_query = count_query.where(Files.object_name.like(object_name_pattern))
    if retry_count_min is not None:
        query = query.where(Files.retry_count >= retry_count_min)
        count_query = count_query.where(Files.retry_count >= retry_count_min)

    total = session.execute(count_query).scalar_one()
    files = session.execute(query.offset(offset).limit(limit)).scalars().all()
    return files, total


def update_file(
    session: Session,
    file_id: int,
    updates: dict,
) -> Files | None:
    """Partially update a file record.

    Args:
        session: Active SQLAlchemy session.
        file_id: Primary key of the file.
        updates: Dict of field names to new values (None values excluded).

    Returns:
        Updated Files record or None if not found.
    """
    file = session.get(Files, file_id)
    if file is None:
        return None

    for key, value in updates.items():
        if value is not None:
            setattr(file, key, value)

    session.commit()
    session.refresh(file)
    return file


def create_job_execution(
    session: Session,
    file_id: int,
    pipeline_run_id: str,
    step_name: str,
    status: str = "pending",
    retry_count: int = 0,
) -> JobExecutions:
    """Create a single job execution record.

    Args:
        session: Active SQLAlchemy session.
        file_id: FK to files table.
        pipeline_run_id: Groups steps in one pipeline attempt.
        step_name: Analytical step name.
        status: Initial status.
        retry_count: Retry attempt number.

    Returns:
        The newly created JobExecutions record.

    Raises:
        ValueError: If file_id does not exist.
    """
    file = session.get(Files, file_id)
    if file is None:
        raise ValueError(f"file with id {file_id} does not exist")

    job = JobExecutions(
        file_id=file_id,
        pipeline_run_id=pipeline_run_id,
        step_name=step_name,
        status=status,
        retry_count=retry_count,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def create_job_executions_batch(
    session: Session,
    file_id: int,
    pipeline_run_id: str,
    executions: list[dict],
) -> list[int]:
    """Create multiple job executions atomically.

    Args:
        session: Active SQLAlchemy session.
        file_id: FK to files table (shared by all executions).
        pipeline_run_id: Groups steps in one pipeline attempt.
        executions: List of dicts with keys: step_name, status, retry_count.

    Returns:
        List of created job execution IDs.

    Raises:
        ValueError: If file_id does not exist.
    """
    file = session.get(Files, file_id)
    if file is None:
        raise ValueError(f"file with id {file_id} does not exist")

    jobs = []
    for item in executions:
        job = JobExecutions(
            file_id=file_id,
            pipeline_run_id=pipeline_run_id,
            step_name=item["step_name"],
            status=item.get("status", "pending"),
            retry_count=item.get("retry_count", 0),
        )
        session.add(job)
        jobs.append(job)

    session.commit()
    for job in jobs:
        session.refresh(job)
    return [job.id for job in jobs]


def get_job_execution_by_id(
    session: Session,
    job_execution_id: int,
) -> JobExecutions | None:
    """Get a job execution by its ID.

    Args:
        session: Active SQLAlchemy session.
        job_execution_id: Primary key.

    Returns:
        JobExecutions record or None if not found.
    """
    return session.get(JobExecutions, job_execution_id)


def list_job_executions(
    session: Session,
    file_id: int | None = None,
    pipeline_run_id: str | None = None,
    step_name: str | None = None,
    status: str | None = None,
    retry_count_min: int | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[JobExecutions], int]:
    """List job executions with optional filtering and pagination.

    Args:
        session: Active SQLAlchemy session.
        file_id: Filter by file.
        pipeline_run_id: Filter by pipeline run.
        step_name: Filter by step.
        status: Filter by status.
        retry_count_min: Minimum retry_count filter.
        limit: Max results to return.
        offset: Number of results to skip.

    Returns:
        Tuple of (list of JobExecutions, total count matching filters).
    """
    query = select(JobExecutions)
    count_query = select(func.count()).select_from(JobExecutions)

    if file_id is not None:
        query = query.where(JobExecutions.file_id == file_id)
        count_query = count_query.where(JobExecutions.file_id == file_id)
    if pipeline_run_id is not None:
        query = query.where(JobExecutions.pipeline_run_id == pipeline_run_id)
        count_query = count_query.where(
            JobExecutions.pipeline_run_id == pipeline_run_id
        )
    if step_name is not None:
        query = query.where(JobExecutions.step_name == step_name)
        count_query = count_query.where(JobExecutions.step_name == step_name)
    if status is not None:
        query = query.where(JobExecutions.status == status)
        count_query = count_query.where(JobExecutions.status == status)
    if retry_count_min is not None:
        query = query.where(JobExecutions.retry_count >= retry_count_min)
        count_query = count_query.where(JobExecutions.retry_count >= retry_count_min)

    total = session.execute(count_query).scalar_one()
    jobs = session.execute(query.offset(offset).limit(limit)).scalars().all()
    return jobs, total


def update_job_execution(
    session: Session,
    job_execution_id: int,
    updates: dict,
) -> JobExecutions | None:
    """Partially update a job execution record.

    Args:
        session: Active SQLAlchemy session.
        job_execution_id: Primary key.
        updates: Dict of field names to new values (None values excluded).

    Returns:
        Updated JobExecutions record or None if not found.
    """
    job = session.get(JobExecutions, job_execution_id)
    if job is None:
        return None

    for key, value in updates.items():
        if value is not None:
            setattr(job, key, value)

    session.commit()
    session.refresh(job)
    return job


_OBJECT_NAME_PATTERN = re.compile(
    r"^(?P<taxi_type>[^/]+)/(?P<year>\d{4})/(?P<month>\d{2})/"
)


def extract_metadata_from_object_name(
    object_name: str,
) -> dict[str, str | None]:
    """Extract taxi_type, year, and month from an S3 object name.

    Args:
        object_name: S3 object key (e.g. "yellow/2022/01/file.parquet").

    Returns:
        Dict with keys taxi_type, year, month — values are None if not matched.
    """
    match = _OBJECT_NAME_PATTERN.match(object_name)
    if match is None:
        return {"taxi_type": None, "year": None, "month": None}
    return {
        "taxi_type": match.group("taxi_type"),
        "year": match.group("year"),
        "month": match.group("month"),
    }


def create_analytical_result(
    session: Session,
    job_execution_id: int,
    result_type: str,
    summary_data: dict,
    computation_time_seconds: float,
    detail_s3_path: str | None = None,
) -> AnalyticalResults:
    """Create an analytical result record.

    Args:
        session: Active SQLAlchemy session.
        job_execution_id: FK to job_executions table.
        result_type: Type of analytical result.
        summary_data: JSONB summary data.
        computation_time_seconds: Time taken for computation.
        detail_s3_path: Optional S3 path for detailed results.

    Returns:
        The newly created AnalyticalResults record.

    Raises:
        ValueError: If job_execution_id does not exist.
    """
    job = session.get(JobExecutions, job_execution_id)
    if job is None:
        raise ValueError(f"job execution with id {job_execution_id} does not exist")

    result = AnalyticalResults(
        job_execution_id=job_execution_id,
        result_type=result_type,
        summary_data=summary_data,
        detail_s3_path=detail_s3_path,
        computation_time_seconds=computation_time_seconds,
    )
    session.add(result)
    session.commit()
    session.refresh(result)
    return result


def get_analytical_result_by_id(
    session: Session,
    result_id: int,
) -> AnalyticalResults | None:
    """Get an analytical result by its ID.

    Args:
        session: Active SQLAlchemy session.
        result_id: Primary key.

    Returns:
        AnalyticalResults record or None if not found.
    """
    return session.get(AnalyticalResults, result_id)


def list_analytical_results(
    session: Session,
    result_type: str | None = None,
    file_id: int | None = None,
    taxi_type: str | None = None,
    year: str | None = None,
    month: str | None = None,
    created_at_from: datetime | None = None,
    created_at_to: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[tuple[AnalyticalResults, Files]], int]:
    """List analytical results with complex filtering via JOIN.

    Joins analytical_results → job_executions → files to support filtering
    by file metadata extracted from object_name.

    Args:
        session: Active SQLAlchemy session.
        result_type: Filter by result type.
        file_id: Filter by file ID.
        taxi_type: Filter by taxi type extracted from object_name.
        year: Filter by year extracted from object_name.
        month: Filter by month extracted from object_name.
        created_at_from: Results created after this timestamp.
        created_at_to: Results created before this timestamp.
        limit: Max results to return.
        offset: Number of results to skip.

    Returns:
        Tuple of (list of (AnalyticalResults, Files) tuples, total count).
    """
    query = (
        select(AnalyticalResults, Files)
        .join(JobExecutions, AnalyticalResults.job_execution_id == JobExecutions.id)
        .join(Files, JobExecutions.file_id == Files.id)
    )
    count_query = (
        select(func.count())
        .select_from(AnalyticalResults)
        .join(JobExecutions, AnalyticalResults.job_execution_id == JobExecutions.id)
        .join(Files, JobExecutions.file_id == Files.id)
    )

    if result_type is not None:
        query = query.where(AnalyticalResults.result_type == result_type)
        count_query = count_query.where(AnalyticalResults.result_type == result_type)
    if file_id is not None:
        query = query.where(Files.id == file_id)
        count_query = count_query.where(Files.id == file_id)
    if taxi_type is not None:
        pattern = f"{taxi_type}/%"
        query = query.where(Files.object_name.like(pattern))
        count_query = count_query.where(Files.object_name.like(pattern))
    if year is not None:
        pattern = f"%/{year}/%"
        query = query.where(Files.object_name.like(pattern))
        count_query = count_query.where(Files.object_name.like(pattern))
    if month is not None:
        pattern = f"%/{month}/%"
        query = query.where(Files.object_name.like(pattern))
        count_query = count_query.where(Files.object_name.like(pattern))
    if created_at_from is not None:
        query = query.where(AnalyticalResults.created_at >= created_at_from)
        count_query = count_query.where(AnalyticalResults.created_at >= created_at_from)
    if created_at_to is not None:
        query = query.where(AnalyticalResults.created_at <= created_at_to)
        count_query = count_query.where(AnalyticalResults.created_at <= created_at_to)

    total = session.execute(count_query).scalar_one()
    rows = session.execute(query.offset(offset).limit(limit)).all()
    return [(row[0], row[1]) for row in rows], total


def create_step_dependencies_batch(
    session: Session,
    pipeline_run_id: str,
    edges: list[dict[str, str]],
) -> int:
    """Batch-insert DAG edges for a pipeline run.

    Args:
        session: Active SQLAlchemy session.
        pipeline_run_id: Pipeline run identifier.
        edges: List of dicts with keys: step_name, depends_on_step_name.

    Returns:
        Number of inserted rows.
    """
    rows = [
        StepDependencies(
            pipeline_run_id=pipeline_run_id,
            step_name=edge["step_name"],
            depends_on_step_name=edge["depends_on_step_name"],
        )
        for edge in edges
    ]
    session.add_all(rows)
    session.commit()
    return len(rows)


def list_step_dependencies(
    session: Session,
    pipeline_run_id: str,
) -> list[StepDependencies]:
    """List DAG edges for a pipeline run.

    Args:
        session: Active SQLAlchemy session.
        pipeline_run_id: Pipeline run identifier.

    Returns:
        List of StepDependencies records for the given pipeline run.
    """
    return (
        session.execute(
            select(StepDependencies).where(
                StepDependencies.pipeline_run_id == pipeline_run_id
            )
        )
        .scalars()
        .all()
    )


if __name__ == "__main__":
    from src.services.database import get_session, init_schema

    init_schema()
    with get_session() as session:
        f = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="yellow/2022/01/test.parquet",
        )
        print(f"Created/got file: id={f.id}, status={f.overall_status}")

        job = create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="test-run-001",
            step_name="descriptive_statistics",
        )
        print(f"Created job: id={job.id}, step={job.step_name}")

        meta = extract_metadata_from_object_name(object_name=f.object_name)
        print(f"Metadata: {meta}")
