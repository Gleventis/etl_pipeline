"""Tests for metrics calculations."""

import pytest
from sqlalchemy import text

from src.services.config import Settings
from src.services.crud import (
    create_or_get_file,
    create_job_execution,
    update_file,
    update_job_execution,
)
from src.services.database import get_session, init_schema, reset_globals
from src.services.metrics import (
    calculate_checkpoint_savings,
    calculate_failure_statistics,
    calculate_pipeline_efficiency,
    calculate_pipeline_summary,
    calculate_recovery_time_improvement,
    calculate_step_performance,
)


@pytest.fixture(scope="module")
def database_url() -> str:
    """Return the Postgres URL from the docker-compose environment."""
    settings = Settings()
    return settings.DATABASE_URL


@pytest.fixture(scope="module", autouse=True)
def _setup_schema(database_url: str):
    """Initialize schema once for the module and reset globals."""
    reset_globals()
    init_schema(database_url=database_url)
    yield
    reset_globals()


@pytest.fixture()
def session(database_url: str):
    """Provide a session with clean tables per test."""
    with get_session(database_url=database_url) as s:
        s.execute(text("DELETE FROM analytical_results"))
        s.execute(text("DELETE FROM job_executions"))
        s.execute(text("DELETE FROM files"))
        s.commit()
        yield s


def _create_completed_file_with_retry(
    session, object_name: str, computation: float, retry_count: int
):
    """Helper: create a completed file with retries and job executions."""
    f = create_or_get_file(
        session=session,
        bucket="raw-data",
        object_name=object_name,
    )
    update_file(
        session=session,
        file_id=f.id,
        updates={
            "overall_status": "completed",
            "total_computation_seconds": computation,
            "retry_count": retry_count,
        },
    )
    return f


class TestCalculateCheckpointSavingsPerFile:
    """Tests for calculate_checkpoint_savings with file_id."""

    def test_returns_empty_for_missing_file(self, session) -> None:
        result = calculate_checkpoint_savings(session=session, file_id=99999)
        assert result == {}

    def test_calculates_savings_for_file_with_retries(self, session) -> None:
        f = _create_completed_file_with_retry(
            session=session,
            object_name="yellow/2022/01/f.parquet",
            computation=200.0,
            retry_count=1,
        )
        # First attempt: step1 completed, step2 failed
        create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=0,
        )
        update_job_execution(
            session=session,
            job_execution_id=session.execute(
                text(
                    "SELECT id FROM job_executions WHERE file_id = :fid AND step_name = 'descriptive_statistics'"
                ).bindparams(fid=f.id)
            ).scalar_one(),
            updates={"computation_time_seconds": 60.0},
        )
        create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-001",
            step_name="data_cleaning",
            status="failed",
            retry_count=0,
        )
        # Retry: step2 completed
        create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-002",
            step_name="data_cleaning",
            status="completed",
            retry_count=1,
        )
        update_job_execution(
            session=session,
            job_execution_id=session.execute(
                text(
                    "SELECT id FROM job_executions WHERE file_id = :fid AND step_name = 'data_cleaning' AND retry_count = 1"
                ).bindparams(fid=f.id)
            ).scalar_one(),
            updates={"computation_time_seconds": 90.0},
        )

        result = calculate_checkpoint_savings(session=session, file_id=f.id)
        assert result["file_id"] == f.id
        assert result["time_saved_seconds"] == 60.0  # completed first-attempt step
        assert result["actual_computation_seconds"] == 200.0
        assert result["percent_saved"] == 30.0
        assert result["retry_count"] == 1

    def test_zero_savings_for_file_without_completed_first_attempts(
        self, session
    ) -> None:
        f = _create_completed_file_with_retry(
            session=session,
            object_name="yellow/2022/01/f.parquet",
            computation=100.0,
            retry_count=1,
        )
        # Only retry-count > 0 jobs
        create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-002",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=1,
        )
        update_job_execution(
            session=session,
            job_execution_id=session.execute(
                text("SELECT id FROM job_executions WHERE file_id = :fid").bindparams(
                    fid=f.id
                )
            ).scalar_one(),
            updates={"computation_time_seconds": 100.0},
        )

        result = calculate_checkpoint_savings(session=session, file_id=f.id)
        assert result["time_saved_seconds"] == 0.0
        assert result["percent_saved"] == 0.0

    def test_percent_saved_zero_when_no_computation(self, session) -> None:
        f = _create_completed_file_with_retry(
            session=session,
            object_name="yellow/2022/01/f.parquet",
            computation=0.0,
            retry_count=1,
        )
        result = calculate_checkpoint_savings(session=session, file_id=f.id)
        assert result["percent_saved"] == 0.0


class TestCalculateCheckpointSavingsAggregate:
    """Tests for calculate_checkpoint_savings without file_id."""

    def test_returns_zeros_when_no_files_with_retries(self, session) -> None:
        # File with no retries
        create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="yellow/2022/01/f.parquet",
        )
        update_file(
            session=session,
            file_id=1,
            updates={"overall_status": "completed"},
        )

        result = calculate_checkpoint_savings(session=session)
        assert result["files_with_retries"] == 0
        assert result["total_time_saved_seconds"] == 0.0

    def test_aggregates_across_multiple_files(self, session) -> None:
        f1 = _create_completed_file_with_retry(
            session=session,
            object_name="yellow/2022/01/f1.parquet",
            computation=200.0,
            retry_count=1,
        )
        f2 = _create_completed_file_with_retry(
            session=session,
            object_name="yellow/2022/02/f2.parquet",
            computation=300.0,
            retry_count=2,
        )

        # f1: 60s saved from first-attempt completed step
        j1 = create_job_execution(
            session=session,
            file_id=f1.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=0,
        )
        update_job_execution(
            session=session,
            job_execution_id=j1.id,
            updates={"computation_time_seconds": 60.0},
        )

        # f2: 100s saved from first-attempt completed step
        j2 = create_job_execution(
            session=session,
            file_id=f2.id,
            pipeline_run_id="run-003",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=0,
        )
        update_job_execution(
            session=session,
            job_execution_id=j2.id,
            updates={"computation_time_seconds": 100.0},
        )

        result = calculate_checkpoint_savings(session=session)
        assert result["files_with_retries"] == 2
        assert result["total_time_saved_seconds"] == 160.0
        assert result["avg_time_saved_per_file_seconds"] == 80.0
        assert result["total_computation_seconds"] == 500.0
        assert result["total_time_saved_hours"] == round(160.0 / 3600.0, 2)

    def test_excludes_non_completed_files(self, session) -> None:
        # Failed file with retries — should be excluded
        f = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="yellow/2022/01/f.parquet",
        )
        update_file(
            session=session,
            file_id=f.id,
            updates={"overall_status": "failed", "retry_count": 1},
        )

        result = calculate_checkpoint_savings(session=session)
        assert result["files_with_retries"] == 0


class TestCalculateFailureStatistics:
    """Tests for calculate_failure_statistics."""

    def test_returns_empty_when_no_jobs(self, session) -> None:
        result = calculate_failure_statistics(session=session)
        assert result == []

    def test_calculates_failure_rate(self, session) -> None:
        f1 = create_or_get_file(session=session, bucket="b", object_name="f1.parquet")
        f2 = create_or_get_file(session=session, bucket="b", object_name="f2.parquet")

        # f1: descriptive_statistics completed
        create_job_execution(
            session=session,
            file_id=f1.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=0,
        )
        # f2: descriptive_statistics failed then retried
        create_job_execution(
            session=session,
            file_id=f2.id,
            pipeline_run_id="run-002",
            step_name="descriptive_statistics",
            status="failed",
            retry_count=0,
        )
        create_job_execution(
            session=session,
            file_id=f2.id,
            pipeline_run_id="run-003",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=1,
        )

        result = calculate_failure_statistics(session=session)
        assert len(result) == 1
        stat = result[0]
        assert stat["step_name"] == "descriptive_statistics"
        assert stat["total_files_processed"] == 2
        assert stat["files_that_failed"] == 1
        assert stat["failure_rate_percent"] == 50.0

    def test_groups_by_step_name(self, session) -> None:
        f = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
        )
        create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-001",
            step_name="data_cleaning",
            status="failed",
        )

        result = calculate_failure_statistics(session=session)
        step_names = {r["step_name"] for r in result}
        assert step_names == {"descriptive_statistics", "data_cleaning"}


class TestCalculatePipelineSummary:
    """Tests for calculate_pipeline_summary."""

    def test_returns_zeros_when_no_completed_files(self, session) -> None:
        result = calculate_pipeline_summary(session=session)
        assert result["total_files"] == 0
        assert result["files_with_retries"] == 0
        assert result["retry_rate_percent"] == 0.0
        assert result["total_computation_hours"] == 0.0
        assert result["total_hours_saved_by_checkpointing"] == 0.0

    def test_calculates_summary(self, session) -> None:
        f1 = _create_completed_file_with_retry(
            session=session,
            object_name="yellow/2022/01/f1.parquet",
            computation=600.0,
            retry_count=1,
        )
        f2 = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="yellow/2022/02/f2.parquet",
        )
        update_file(
            session=session,
            file_id=f2.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 400.0,
            },
        )

        # f1 has a completed first-attempt step (savings)
        j = create_job_execution(
            session=session,
            file_id=f1.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=0,
        )
        update_job_execution(
            session=session,
            job_execution_id=j.id,
            updates={"computation_time_seconds": 120.0},
        )

        result = calculate_pipeline_summary(session=session)
        assert result["total_files"] == 2
        assert result["files_with_retries"] == 1
        assert result["retry_rate_percent"] == 50.0
        assert result["avg_computation_minutes_per_file"] == round(500.0 / 60.0, 2)
        assert result["total_computation_hours"] == round(1000.0 / 3600.0, 2)
        assert result["total_hours_saved_by_checkpointing"] == round(120.0 / 3600.0, 2)


class TestCalculateStepPerformance:
    """Tests for calculate_step_performance."""

    def test_returns_empty_when_no_completed_jobs(self, session) -> None:
        result = calculate_step_performance(session=session)
        assert result == []

    def test_excludes_jobs_without_computation_time(self, session) -> None:
        f = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
        )
        # No computation_time_seconds set
        result = calculate_step_performance(session=session)
        assert result == []

    def test_calculates_stats_for_single_step(self, session) -> None:
        f = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        j = create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
        )
        update_job_execution(
            session=session,
            job_execution_id=j.id,
            updates={"computation_time_seconds": 45.0},
        )

        result = calculate_step_performance(session=session)
        assert len(result) == 1
        stat = result[0]
        assert stat["step_name"] == "descriptive_statistics"
        assert stat["executions"] == 1
        assert stat["avg_seconds"] == 45.0
        assert stat["min_seconds"] == 45.0
        assert stat["max_seconds"] == 45.0
        # stddev is None for single value in Postgres
        assert stat["stddev_seconds"] is None

    def test_calculates_stats_across_multiple_executions(self, session) -> None:
        f1 = create_or_get_file(session=session, bucket="b", object_name="f1.parquet")
        f2 = create_or_get_file(session=session, bucket="b", object_name="f2.parquet")

        j1 = create_job_execution(
            session=session,
            file_id=f1.id,
            pipeline_run_id="run-001",
            step_name="data_cleaning",
            status="completed",
        )
        update_job_execution(
            session=session,
            job_execution_id=j1.id,
            updates={"computation_time_seconds": 80.0},
        )
        j2 = create_job_execution(
            session=session,
            file_id=f2.id,
            pipeline_run_id="run-002",
            step_name="data_cleaning",
            status="completed",
        )
        update_job_execution(
            session=session,
            job_execution_id=j2.id,
            updates={"computation_time_seconds": 120.0},
        )

        result = calculate_step_performance(session=session)
        assert len(result) == 1
        stat = result[0]
        assert stat["step_name"] == "data_cleaning"
        assert stat["executions"] == 2
        assert stat["avg_seconds"] == 100.0
        assert stat["min_seconds"] == 80.0
        assert stat["max_seconds"] == 120.0
        assert stat["stddev_seconds"] is not None

    def test_orders_by_avg_seconds_descending(self, session) -> None:
        f = create_or_get_file(session=session, bucket="b", object_name="f.parquet")

        j1 = create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
        )
        update_job_execution(
            session=session,
            job_execution_id=j1.id,
            updates={"computation_time_seconds": 30.0},
        )
        j2 = create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-001",
            step_name="data_cleaning",
            status="completed",
        )
        update_job_execution(
            session=session,
            job_execution_id=j2.id,
            updates={"computation_time_seconds": 90.0},
        )

        result = calculate_step_performance(session=session)
        assert len(result) == 2
        assert result[0]["step_name"] == "data_cleaning"
        assert result[1]["step_name"] == "descriptive_statistics"


class TestCalculatePipelineEfficiency:
    """Tests for calculate_pipeline_efficiency."""

    def test_returns_empty_when_no_files_with_elapsed_time(self, session) -> None:
        # File with zero elapsed time — should be excluded
        f = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        update_file(
            session=session,
            file_id=f.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 100.0,
                "total_elapsed_seconds": 0.0,
            },
        )

        result = calculate_pipeline_efficiency(session=session)
        assert result == []

    def test_single_completed_file(self, session) -> None:
        f = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        update_file(
            session=session,
            file_id=f.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 300.0,
                "total_elapsed_seconds": 600.0,
            },
        )

        result = calculate_pipeline_efficiency(session=session)
        assert len(result) == 1
        stat = result[0]
        assert stat["overall_status"] == "completed"
        assert stat["file_count"] == 1
        assert stat["avg_efficiency_ratio"] == round(300.0 / 600.0, 4)
        assert stat["avg_computation_minutes"] == round(300.0 / 60.0, 2)
        assert stat["avg_elapsed_minutes"] == round(600.0 / 60.0, 2)

    def test_multiple_status_groups(self, session) -> None:
        f1 = create_or_get_file(session=session, bucket="b", object_name="f1.parquet")
        update_file(
            session=session,
            file_id=f1.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 200.0,
                "total_elapsed_seconds": 400.0,
            },
        )
        f2 = create_or_get_file(session=session, bucket="b", object_name="f2.parquet")
        update_file(
            session=session,
            file_id=f2.id,
            updates={
                "overall_status": "failed",
                "total_computation_seconds": 50.0,
                "total_elapsed_seconds": 100.0,
            },
        )

        result = calculate_pipeline_efficiency(session=session)
        statuses = {r["overall_status"] for r in result}
        assert statuses == {"completed", "failed"}
        assert all(r["file_count"] == 1 for r in result)

    def test_averages_across_files_in_same_group(self, session) -> None:
        f1 = create_or_get_file(session=session, bucket="b", object_name="f1.parquet")
        update_file(
            session=session,
            file_id=f1.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 200.0,
                "total_elapsed_seconds": 400.0,
            },
        )
        f2 = create_or_get_file(session=session, bucket="b", object_name="f2.parquet")
        update_file(
            session=session,
            file_id=f2.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 400.0,
                "total_elapsed_seconds": 800.0,
            },
        )

        result = calculate_pipeline_efficiency(session=session)
        assert len(result) == 1
        stat = result[0]
        assert stat["file_count"] == 2
        # Both files have ratio 0.5, so avg is 0.5
        assert stat["avg_efficiency_ratio"] == 0.5
        assert stat["avg_computation_minutes"] == round(300.0 / 60.0, 2)
        assert stat["avg_elapsed_minutes"] == round(600.0 / 60.0, 2)

    def test_excludes_files_with_zero_elapsed(self, session) -> None:
        f1 = create_or_get_file(session=session, bucket="b", object_name="f1.parquet")
        update_file(
            session=session,
            file_id=f1.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 300.0,
                "total_elapsed_seconds": 600.0,
            },
        )
        # This file should be excluded (elapsed = 0)
        f2 = create_or_get_file(session=session, bucket="b", object_name="f2.parquet")
        update_file(
            session=session,
            file_id=f2.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 100.0,
                "total_elapsed_seconds": 0.0,
            },
        )

        result = calculate_pipeline_efficiency(session=session)
        assert len(result) == 1
        assert result[0]["file_count"] == 1
        assert result[0]["avg_efficiency_ratio"] == 0.5


class TestCalculateRecoveryTimeImprovement:
    """Tests for calculate_recovery_time_improvement."""

    def test_returns_zeros_when_no_files_with_retries(self, session) -> None:
        # File with retry_count=0 — should be excluded
        f = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        update_file(
            session=session,
            file_id=f.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 100.0,
                "retry_count": 0,
            },
        )

        result = calculate_recovery_time_improvement(session=session)
        assert result["avg_recovery_with_checkpoint_seconds"] == 0.0
        assert result["avg_recovery_without_checkpoint_seconds"] == 0.0
        assert result["avg_time_saved_seconds"] == 0.0
        assert result["percent_improvement"] == 0.0

    def test_excludes_non_completed_files(self, session) -> None:
        # File with retries but status=failed — should be excluded
        f = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        update_file(
            session=session,
            file_id=f.id,
            updates={
                "overall_status": "failed",
                "total_computation_seconds": 200.0,
                "retry_count": 1,
            },
        )

        result = calculate_recovery_time_improvement(session=session)
        assert result["avg_recovery_with_checkpoint_seconds"] == 0.0
        assert result["percent_improvement"] == 0.0

    def test_single_file_with_retries(self, session) -> None:
        f = _create_completed_file_with_retry(
            session=session,
            object_name="f.parquet",
            computation=500.0,
            retry_count=1,
        )
        # First attempt step — retry_count=0, should NOT count as "with checkpoint"
        je1 = create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-1",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=0,
        )
        update_job_execution(
            session=session,
            job_execution_id=je1.id,
            updates={"computation_time_seconds": 200.0},
        )
        # Retry step — retry_count=1, counts as "with checkpoint"
        je2 = create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-2",
            step_name="data_cleaning",
            status="completed",
            retry_count=1,
        )
        update_job_execution(
            session=session,
            job_execution_id=je2.id,
            updates={"computation_time_seconds": 100.0},
        )

        result = calculate_recovery_time_improvement(session=session)
        # with checkpoint = sum of retry_count > 0 steps = 100.0
        assert result["avg_recovery_with_checkpoint_seconds"] == 100.0
        # without checkpoint = total_computation_seconds = 500.0
        assert result["avg_recovery_without_checkpoint_seconds"] == 500.0
        assert result["avg_time_saved_seconds"] == 400.0
        assert result["percent_improvement"] == 80.0

    def test_averages_across_multiple_files(self, session) -> None:
        # File 1: total=600, retry steps sum=200
        f1 = _create_completed_file_with_retry(
            session=session,
            object_name="f1.parquet",
            computation=600.0,
            retry_count=1,
        )
        je1 = create_job_execution(
            session=session,
            file_id=f1.id,
            pipeline_run_id="run-a",
            step_name="temporal_analysis",
            status="completed",
            retry_count=1,
        )
        update_job_execution(
            session=session,
            job_execution_id=je1.id,
            updates={"computation_time_seconds": 200.0},
        )

        # File 2: total=400, retry steps sum=100
        f2 = _create_completed_file_with_retry(
            session=session,
            object_name="f2.parquet",
            computation=400.0,
            retry_count=2,
        )
        je2 = create_job_execution(
            session=session,
            file_id=f2.id,
            pipeline_run_id="run-b",
            step_name="fare_revenue_analysis",
            status="completed",
            retry_count=1,
        )
        update_job_execution(
            session=session,
            job_execution_id=je2.id,
            updates={"computation_time_seconds": 100.0},
        )

        result = calculate_recovery_time_improvement(session=session)
        # avg_with = (200 + 100) / 2 = 150
        assert result["avg_recovery_with_checkpoint_seconds"] == 150.0
        # avg_without = (600 + 400) / 2 = 500
        assert result["avg_recovery_without_checkpoint_seconds"] == 500.0
        # avg_saved = 500 - 150 = 350
        assert result["avg_time_saved_seconds"] == 350.0
        # percent = 100 * 350 / 500 = 70.0
        assert result["percent_improvement"] == 70.0

    def test_file_with_no_retry_job_executions(self, session) -> None:
        # File has retry_count > 0 but no job_executions with retry_count > 0
        # (edge case: file marked as retried but all jobs are first-attempt)
        f = _create_completed_file_with_retry(
            session=session,
            object_name="f.parquet",
            computation=300.0,
            retry_count=1,
        )
        je = create_job_execution(
            session=session,
            file_id=f.id,
            pipeline_run_id="run-1",
            step_name="descriptive_statistics",
            status="completed",
            retry_count=0,
        )
        update_job_execution(
            session=session,
            job_execution_id=je.id,
            updates={"computation_time_seconds": 300.0},
        )

        result = calculate_recovery_time_improvement(session=session)
        # with checkpoint = 0 (no retry_count > 0 job executions)
        assert result["avg_recovery_with_checkpoint_seconds"] == 0.0
        # without checkpoint = 300
        assert result["avg_recovery_without_checkpoint_seconds"] == 300.0
        assert result["avg_time_saved_seconds"] == 300.0
        assert result["percent_improvement"] == 100.0
