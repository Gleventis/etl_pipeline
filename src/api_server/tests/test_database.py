"""Tests for API server database models and schema using docker-compose Postgres."""

import pytest
from sqlalchemy import inspect, select, text

from src.services.config import Settings
from src.services.database import (
    AnalyticalResults,
    Files,
    JobExecutions,
    get_engine,
    get_session,
    init_schema,
    reset_globals,
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


class TestInitSchema:
    """Tests for init_schema."""

    def test_creates_files_table(self, database_url: str) -> None:
        engine = get_engine(database_url=database_url)
        inspector = inspect(engine)
        assert "files" in inspector.get_table_names()

    def test_creates_job_executions_table(self, database_url: str) -> None:
        engine = get_engine(database_url=database_url)
        inspector = inspect(engine)
        assert "job_executions" in inspector.get_table_names()

    def test_creates_analytical_results_table(self, database_url: str) -> None:
        engine = get_engine(database_url=database_url)
        inspector = inspect(engine)
        assert "analytical_results" in inspector.get_table_names()

    def test_idempotent(self, database_url: str) -> None:
        init_schema(database_url=database_url)


class TestFilesModel:
    """Tests for the Files SQLAlchemy model."""

    def test_insert_and_read(self, session) -> None:
        file = Files(
            bucket="raw-data",
            object_name="yellow/2022/01/yellow_tripdata_2022-01.parquet",
            overall_status="pending",
        )
        session.add(file)
        session.commit()

        result = session.execute(select(Files)).scalar_one()
        assert result.bucket == "raw-data"
        assert result.object_name == "yellow/2022/01/yellow_tripdata_2022-01.parquet"
        assert result.overall_status == "pending"
        assert result.total_computation_seconds == 0.0
        assert result.total_elapsed_seconds == 0.0
        assert result.retry_count == 0
        assert result.created_at is not None
        assert result.updated_at is not None

    def test_unique_constraint_bucket_object_name(self, session) -> None:
        file1 = Files(
            bucket="raw-data",
            object_name="file.parquet",
            overall_status="pending",
        )
        session.add(file1)
        session.commit()

        file2 = Files(
            bucket="raw-data",
            object_name="file.parquet",
            overall_status="completed",
        )
        session.add(file2)
        with pytest.raises(Exception):
            session.commit()
        session.rollback()

    def test_different_buckets_same_object_name_allowed(self, session) -> None:
        file1 = Files(
            bucket="raw-data",
            object_name="file.parquet",
            overall_status="pending",
        )
        file2 = Files(
            bucket="cleaned-data",
            object_name="file.parquet",
            overall_status="pending",
        )
        session.add_all([file1, file2])
        session.commit()

        count = session.execute(select(Files)).scalars().all()
        assert len(count) == 2


class TestJobExecutionsModel:
    """Tests for the JobExecutions SQLAlchemy model."""

    def test_insert_with_valid_file_id(self, session) -> None:
        file = Files(
            bucket="raw-data",
            object_name="file.parquet",
            overall_status="pending",
        )
        session.add(file)
        session.commit()

        job = JobExecutions(
            file_id=file.id,
            pipeline_run_id="abc-def-123",
            step_name="descriptive_statistics",
            status="pending",
        )
        session.add(job)
        session.commit()

        result = session.execute(select(JobExecutions)).scalar_one()
        assert result.file_id == file.id
        assert result.pipeline_run_id == "abc-def-123"
        assert result.step_name == "descriptive_statistics"
        assert result.status == "pending"
        assert result.started_at is None
        assert result.completed_at is None
        assert result.computation_time_seconds is None
        assert result.retry_count == 0
        assert result.error_message is None

    def test_foreign_key_constraint(self, session) -> None:
        job = JobExecutions(
            file_id=99999,
            pipeline_run_id="abc",
            step_name="descriptive_statistics",
            status="pending",
        )
        session.add(job)
        with pytest.raises(Exception):
            session.commit()
        session.rollback()


class TestAnalyticalResultsModel:
    """Tests for the AnalyticalResults SQLAlchemy model."""

    def test_insert_with_jsonb(self, session) -> None:
        file = Files(
            bucket="raw-data",
            object_name="file.parquet",
            overall_status="pending",
        )
        session.add(file)
        session.commit()

        job = JobExecutions(
            file_id=file.id,
            pipeline_run_id="abc-def-123",
            step_name="descriptive_statistics",
            status="completed",
        )
        session.add(job)
        session.commit()

        summary = {"total_rows": 2463931, "avg_fare": 13.52}
        result = AnalyticalResults(
            job_execution_id=job.id,
            result_type="descriptive_statistics",
            summary_data=summary,
            detail_s3_path="results/yellow/2022/01/descriptive_statistics.parquet",
            computation_time_seconds=63.2,
        )
        session.add(result)
        session.commit()

        fetched = session.execute(select(AnalyticalResults)).scalar_one()
        assert fetched.result_type == "descriptive_statistics"
        assert fetched.summary_data == summary
        assert fetched.computation_time_seconds == 63.2
        assert (
            fetched.detail_s3_path
            == "results/yellow/2022/01/descriptive_statistics.parquet"
        )

    def test_foreign_key_constraint(self, session) -> None:
        result = AnalyticalResults(
            job_execution_id=99999,
            result_type="descriptive_statistics",
            summary_data={"key": "value"},
            computation_time_seconds=1.0,
        )
        session.add(result)
        with pytest.raises(Exception):
            session.commit()
        session.rollback()

    def test_nullable_detail_s3_path(self, session) -> None:
        file = Files(
            bucket="raw-data",
            object_name="file.parquet",
            overall_status="pending",
        )
        session.add(file)
        session.commit()

        job = JobExecutions(
            file_id=file.id,
            pipeline_run_id="abc",
            step_name="descriptive_statistics",
            status="completed",
        )
        session.add(job)
        session.commit()

        result = AnalyticalResults(
            job_execution_id=job.id,
            result_type="descriptive_statistics",
            summary_data={"key": "value"},
            detail_s3_path=None,
            computation_time_seconds=1.0,
        )
        session.add(result)
        session.commit()

        fetched = session.execute(select(AnalyticalResults)).scalar_one()
        assert fetched.detail_s3_path is None
