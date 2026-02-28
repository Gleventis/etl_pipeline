"""Metrics calculations for thesis checkpoint evaluation."""

import logging

from sqlalchemy import case, distinct, func, select
from sqlalchemy.orm import Session

from src.services.database import Files, JobExecutions

logger = logging.getLogger(__name__)


def _time_saved_subquery(session: Session, file_id: int) -> float:
    """Sum of computation_time_seconds for completed first-attempt steps of a file.

    Args:
        session: Active SQLAlchemy session.
        file_id: File to calculate for.

    Returns:
        Total seconds saved (completed steps that didn't need re-running).
    """
    result = session.execute(
        select(
            func.coalesce(func.sum(JobExecutions.computation_time_seconds), 0.0)
        ).where(
            JobExecutions.file_id == file_id,
            JobExecutions.status == "completed",
            JobExecutions.retry_count == 0,
        )
    ).scalar_one()
    return float(result)


def calculate_checkpoint_savings(
    session: Session,
    file_id: int | None = None,
) -> dict:
    """Calculate time saved by checkpointing.

    Args:
        session: Active SQLAlchemy session.
        file_id: If provided, calculate for a specific file. Otherwise aggregate.

    Returns:
        Dict matching CheckpointSavingsFileResponse or CheckpointSavingsAggregateResponse.
    """
    if file_id is not None:
        file = session.get(Files, file_id)
        if file is None:
            return {}

        time_saved = _time_saved_subquery(session=session, file_id=file_id)
        actual = file.total_computation_seconds
        percent = round(100.0 * time_saved / actual, 2) if actual > 0 else 0.0

        return {
            "file_id": file.id,
            "object_name": file.object_name,
            "time_saved_seconds": time_saved,
            "actual_computation_seconds": actual,
            "percent_saved": percent,
            "retry_count": file.retry_count,
        }

    # Aggregate across all files with retries
    files_with_retries = (
        session.execute(
            select(Files).where(
                Files.retry_count > 0,
                Files.overall_status == "completed",
            )
        )
        .scalars()
        .all()
    )

    if not files_with_retries:
        return {
            "files_with_retries": 0,
            "total_time_saved_seconds": 0.0,
            "total_time_saved_hours": 0.0,
            "avg_time_saved_per_file_seconds": 0.0,
            "total_computation_seconds": 0.0,
            "percent_saved": 0.0,
        }

    total_saved = 0.0
    total_computation = 0.0
    for f in files_with_retries:
        total_saved += _time_saved_subquery(session=session, file_id=f.id)
        total_computation += f.total_computation_seconds

    count = len(files_with_retries)
    percent = (
        round(100.0 * total_saved / total_computation, 2)
        if total_computation > 0
        else 0.0
    )

    return {
        "files_with_retries": count,
        "total_time_saved_seconds": total_saved,
        "total_time_saved_hours": round(total_saved / 3600.0, 2),
        "avg_time_saved_per_file_seconds": round(total_saved / count, 2),
        "total_computation_seconds": total_computation,
        "percent_saved": percent,
    }


def calculate_failure_statistics(session: Session) -> list[dict]:
    """Calculate failure rates per analytical step.

    Args:
        session: Active SQLAlchemy session.

    Returns:
        List of dicts matching StepFailureStatistic.
    """
    rows = session.execute(
        select(
            JobExecutions.step_name,
            func.count(distinct(JobExecutions.file_id)).label("total_files"),
            func.count(
                distinct(
                    case(
                        (JobExecutions.status == "failed", JobExecutions.file_id),
                    )
                )
            ).label("files_failed"),
            func.avg(
                case(
                    (JobExecutions.retry_count > 0, JobExecutions.retry_count),
                )
            ).label("avg_retries"),
            func.avg(JobExecutions.computation_time_seconds).label("avg_computation"),
        ).group_by(JobExecutions.step_name)
    ).all()

    results = []
    for row in rows:
        total = row.total_files
        failed = row.files_failed
        rate = round(100.0 * failed / total, 2) if total > 0 else 0.0
        results.append(
            {
                "step_name": row.step_name,
                "total_files_processed": total,
                "files_that_failed": failed,
                "failure_rate_percent": rate,
                "avg_retries_when_failed": round(float(row.avg_retries), 2)
                if row.avg_retries is not None
                else None,
                "avg_computation_seconds": round(float(row.avg_computation), 2)
                if row.avg_computation is not None
                else None,
            }
        )

    return results


def calculate_pipeline_summary(session: Session) -> dict:
    """Calculate comprehensive pipeline summary for thesis reporting.

    Args:
        session: Active SQLAlchemy session.

    Returns:
        Dict matching PipelineSummaryResponse.
    """
    file_stats = session.execute(
        select(
            func.count().label("total_files"),
            func.count(case((Files.retry_count > 0, 1))).label("files_with_retries"),
            func.avg(Files.total_computation_seconds).label("avg_computation"),
            func.sum(Files.total_computation_seconds).label("total_computation"),
        ).where(Files.overall_status == "completed")
    ).one()

    total_files = file_stats.total_files or 0
    files_with_retries = file_stats.files_with_retries or 0
    avg_computation = float(file_stats.avg_computation or 0.0)
    total_computation = float(file_stats.total_computation or 0.0)

    retry_rate = (
        round(100.0 * files_with_retries / total_files, 2) if total_files > 0 else 0.0
    )

    # Calculate savings from checkpoint
    savings = calculate_checkpoint_savings(session=session)
    total_hours_saved = savings.get("total_time_saved_hours", 0.0)
    avg_saved = savings.get("avg_time_saved_per_file_seconds", 0.0)
    total_computation_hours = total_computation / 3600.0
    percent_saved = (
        round(100.0 * total_hours_saved / total_computation_hours, 2)
        if total_computation_hours > 0
        else 0.0
    )

    return {
        "total_files": total_files,
        "files_with_retries": files_with_retries,
        "retry_rate_percent": retry_rate,
        "avg_computation_minutes_per_file": round(avg_computation / 60.0, 2),
        "total_computation_hours": round(total_computation_hours, 2),
        "total_hours_saved_by_checkpointing": total_hours_saved,
        "avg_minutes_saved_per_retry": round(avg_saved / 60.0, 2),
        "percent_time_saved": percent_saved,
    }


def calculate_step_performance(session: Session) -> list[dict]:
    """Calculate average computation time per analytical step.

    Args:
        session: Active SQLAlchemy session.

    Returns:
        List of dicts matching StepPerformanceStatistic.
    """
    rows = session.execute(
        select(
            JobExecutions.step_name,
            func.count().label("executions"),
            func.avg(JobExecutions.computation_time_seconds).label("avg_seconds"),
            func.min(JobExecutions.computation_time_seconds).label("min_seconds"),
            func.max(JobExecutions.computation_time_seconds).label("max_seconds"),
            func.stddev(JobExecutions.computation_time_seconds).label("stddev_seconds"),
        )
        .where(
            JobExecutions.status == "completed",
            JobExecutions.computation_time_seconds.is_not(None),
        )
        .group_by(JobExecutions.step_name)
        .order_by(func.avg(JobExecutions.computation_time_seconds).desc())
    ).all()

    return [
        {
            "step_name": row.step_name,
            "executions": row.executions,
            "avg_seconds": round(float(row.avg_seconds), 2),
            "min_seconds": round(float(row.min_seconds), 2),
            "max_seconds": round(float(row.max_seconds), 2),
            "stddev_seconds": round(float(row.stddev_seconds), 2)
            if row.stddev_seconds is not None
            else None,
        }
        for row in rows
    ]


def calculate_pipeline_efficiency(session: Session) -> list[dict]:
    """Calculate pipeline efficiency ratio grouped by overall_status.

    Efficiency is the ratio of computation time to wall-clock elapsed time.

    Args:
        session: Active SQLAlchemy session.

    Returns:
        List of dicts matching PipelineEfficiencyStatistic.
    """
    rows = session.execute(
        select(
            Files.overall_status,
            func.count().label("file_count"),
            func.avg(
                Files.total_computation_seconds
                / func.nullif(Files.total_elapsed_seconds, 0)
            ).label("avg_efficiency_ratio"),
            func.avg(Files.total_computation_seconds).label("avg_computation"),
            func.avg(Files.total_elapsed_seconds).label("avg_elapsed"),
        )
        .where(Files.total_elapsed_seconds > 0)
        .group_by(Files.overall_status)
    ).all()

    return [
        {
            "overall_status": row.overall_status,
            "file_count": row.file_count,
            "avg_efficiency_ratio": round(float(row.avg_efficiency_ratio), 4)
            if row.avg_efficiency_ratio is not None
            else 0.0,
            "avg_computation_minutes": round(float(row.avg_computation) / 60.0, 2)
            if row.avg_computation is not None
            else 0.0,
            "avg_elapsed_minutes": round(float(row.avg_elapsed) / 60.0, 2)
            if row.avg_elapsed is not None
            else 0.0,
        }
        for row in rows
    ]


def calculate_recovery_time_improvement(session: Session) -> dict:
    """Calculate average recovery time improvement from checkpointing.

    Compares actual recovery time (only retried steps) against hypothetical
    full restart (total_computation_seconds) for files that experienced failures.

    Args:
        session: Active SQLAlchemy session.

    Returns:
        Dict matching RecoveryTimeResponse fields.
    """
    files_with_retries = (
        session.execute(
            select(Files).where(
                Files.retry_count > 0,
                Files.overall_status == "completed",
            )
        )
        .scalars()
        .all()
    )

    if not files_with_retries:
        return {
            "avg_recovery_with_checkpoint_seconds": 0.0,
            "avg_recovery_without_checkpoint_seconds": 0.0,
            "avg_time_saved_seconds": 0.0,
            "percent_improvement": 0.0,
        }

    with_checkpoint_values = []
    without_checkpoint_values = []

    for f in files_with_retries:
        time_with = session.execute(
            select(
                func.coalesce(func.sum(JobExecutions.computation_time_seconds), 0.0)
            ).where(
                JobExecutions.file_id == f.id,
                JobExecutions.retry_count > 0,
            )
        ).scalar_one()

        with_checkpoint_values.append(float(time_with))
        without_checkpoint_values.append(f.total_computation_seconds)

    count = len(files_with_retries)
    avg_with = sum(with_checkpoint_values) / count
    avg_without = sum(without_checkpoint_values) / count
    avg_saved = avg_without - avg_with
    percent = round(100.0 * avg_saved / avg_without, 2) if avg_without > 0 else 0.0

    return {
        "avg_recovery_with_checkpoint_seconds": round(avg_with, 2),
        "avg_recovery_without_checkpoint_seconds": round(avg_without, 2),
        "avg_time_saved_seconds": round(avg_saved, 2),
        "percent_improvement": percent,
    }


if __name__ == "__main__":
    from src.services.database import get_session, init_schema

    init_schema()
    with get_session() as session:
        savings = calculate_checkpoint_savings(session=session)
        print(f"Checkpoint savings: {savings}")

        failures = calculate_failure_statistics(session=session)
        print(f"Failure statistics: {failures}")

        summary = calculate_pipeline_summary(session=session)
        print(f"Pipeline summary: {summary}")

        step_perf = calculate_step_performance(session=session)
        print(f"Step performance: {step_perf}")

        efficiency = calculate_pipeline_efficiency(session=session)
        print(f"Pipeline efficiency: {efficiency}")

        recovery = calculate_recovery_time_improvement(session=session)
        print(f"Recovery time improvement: {recovery}")
