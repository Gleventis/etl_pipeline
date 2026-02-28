"""Tests for CRUD operations."""

import pytest
from sqlalchemy import text

from src.services.config import Settings
from src.services.crud import (
    create_analytical_result,
    create_or_get_file,
    create_job_execution,
    create_job_executions_batch,
    extract_metadata_from_object_name,
    get_analytical_result_by_id,
    get_file_by_id,
    get_job_execution_by_id,
    list_analytical_results,
    list_files,
    list_job_executions,
    update_file,
    update_job_execution,
)
from src.services.database import (
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


class TestCreateOrGetFile:
    """Tests for create_or_get_file."""

    def test_creates_new_file(self, session) -> None:
        result = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="yellow/2022/01/file.parquet",
        )
        assert result.id is not None
        assert result.bucket == "raw-data"
        assert result.object_name == "yellow/2022/01/file.parquet"
        assert result.overall_status == "pending"

    def test_returns_existing_file_idempotent(self, session) -> None:
        first = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="yellow/2022/01/file.parquet",
        )
        second = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="yellow/2022/01/file.parquet",
            overall_status="completed",
        )
        assert first.id == second.id
        assert second.overall_status == "pending"

    def test_different_buckets_create_separate_files(self, session) -> None:
        f1 = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="file.parquet",
        )
        f2 = create_or_get_file(
            session=session,
            bucket="cleaned-data",
            object_name="file.parquet",
        )
        assert f1.id != f2.id

    def test_custom_status(self, session) -> None:
        result = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="file.parquet",
            overall_status="in_progress",
        )
        assert result.overall_status == "in_progress"


class TestGetFileById:
    """Tests for get_file_by_id."""

    def test_returns_file(self, session) -> None:
        created = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="file.parquet",
        )
        result = get_file_by_id(session=session, file_id=created.id)
        assert result is not None
        assert result.id == created.id

    def test_returns_none_for_missing_id(self, session) -> None:
        result = get_file_by_id(session=session, file_id=99999)
        assert result is None


class TestListFiles:
    """Tests for list_files."""

    def test_returns_all_files(self, session) -> None:
        create_or_get_file(session=session, bucket="b", object_name="a.parquet")
        create_or_get_file(session=session, bucket="b", object_name="b.parquet")
        files, total = list_files(session=session)
        assert total == 2
        assert len(files) == 2

    def test_filter_by_status(self, session) -> None:
        create_or_get_file(
            session=session,
            bucket="b",
            object_name="a.parquet",
            overall_status="pending",
        )
        create_or_get_file(
            session=session,
            bucket="b",
            object_name="b.parquet",
            overall_status="completed",
        )
        files, total = list_files(session=session, status="completed")
        assert total == 1
        assert files[0].overall_status == "completed"

    def test_filter_by_bucket(self, session) -> None:
        create_or_get_file(session=session, bucket="raw", object_name="a.parquet")
        create_or_get_file(session=session, bucket="clean", object_name="b.parquet")
        files, total = list_files(session=session, bucket="raw")
        assert total == 1
        assert files[0].bucket == "raw"

    def test_filter_by_object_name_pattern(self, session) -> None:
        create_or_get_file(
            session=session, bucket="b", object_name="yellow/2022/01/f.parquet"
        )
        create_or_get_file(
            session=session, bucket="b", object_name="green/2022/01/f.parquet"
        )
        files, total = list_files(session=session, object_name_pattern="yellow%")
        assert total == 1
        assert "yellow" in files[0].object_name

    def test_filter_by_retry_count_min(self, session) -> None:
        f1 = create_or_get_file(session=session, bucket="b", object_name="a.parquet")
        create_or_get_file(session=session, bucket="b", object_name="b.parquet")
        f1.retry_count = 3
        session.commit()

        files, total = list_files(session=session, retry_count_min=2)
        assert total == 1
        assert files[0].retry_count >= 2

    def test_pagination_limit(self, session) -> None:
        for i in range(5):
            create_or_get_file(session=session, bucket="b", object_name=f"{i}.parquet")
        files, total = list_files(session=session, limit=2)
        assert total == 5
        assert len(files) == 2

    def test_pagination_offset(self, session) -> None:
        for i in range(5):
            create_or_get_file(session=session, bucket="b", object_name=f"{i}.parquet")
        files, total = list_files(session=session, limit=2, offset=3)
        assert total == 5
        assert len(files) == 2

    def test_empty_result(self, session) -> None:
        files, total = list_files(session=session, status="nonexistent")
        assert total == 0
        assert len(files) == 0


class TestUpdateFile:
    """Tests for update_file."""

    def test_updates_status(self, session) -> None:
        created = create_or_get_file(
            session=session, bucket="b", object_name="f.parquet"
        )
        result = update_file(
            session=session,
            file_id=created.id,
            updates={"overall_status": "completed"},
        )
        assert result is not None
        assert result.overall_status == "completed"

    def test_updates_multiple_fields(self, session) -> None:
        created = create_or_get_file(
            session=session, bucket="b", object_name="f.parquet"
        )
        result = update_file(
            session=session,
            file_id=created.id,
            updates={
                "overall_status": "completed",
                "total_computation_seconds": 487.3,
                "retry_count": 1,
            },
        )
        assert result is not None
        assert result.overall_status == "completed"
        assert result.total_computation_seconds == 487.3
        assert result.retry_count == 1

    def test_returns_none_for_missing_id(self, session) -> None:
        result = update_file(
            session=session,
            file_id=99999,
            updates={"overall_status": "completed"},
        )
        assert result is None

    def test_ignores_none_values(self, session) -> None:
        created = create_or_get_file(
            session=session, bucket="b", object_name="f.parquet"
        )
        result = update_file(
            session=session,
            file_id=created.id,
            updates={"overall_status": "completed", "total_computation_seconds": None},
        )
        assert result is not None
        assert result.overall_status == "completed"
        assert result.total_computation_seconds == 0.0


class TestCreateJobExecution:
    """Tests for create_job_execution."""

    def test_creates_job_execution(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        job = create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )
        assert job.id is not None
        assert job.file_id == file.id
        assert job.pipeline_run_id == "run-001"
        assert job.step_name == "descriptive_statistics"
        assert job.status == "pending"
        assert job.retry_count == 0

    def test_raises_for_invalid_file_id(self, session) -> None:
        with pytest.raises(ValueError, match="file with id 99999 does not exist"):
            create_job_execution(
                session=session,
                file_id=99999,
                pipeline_run_id="run-001",
                step_name="descriptive_statistics",
            )

    def test_custom_status_and_retry(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        job = create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="data_cleaning",
            status="running",
            retry_count=2,
        )
        assert job.status == "running"
        assert job.retry_count == 2


class TestCreateJobExecutionsBatch:
    """Tests for create_job_executions_batch."""

    def test_creates_batch(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        executions = [
            {
                "step_name": "descriptive_statistics",
                "status": "pending",
                "retry_count": 0,
            },
            {"step_name": "data_cleaning", "status": "pending", "retry_count": 0},
            {"step_name": "temporal_analysis", "status": "pending", "retry_count": 0},
        ]
        ids = create_job_executions_batch(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            executions=executions,
        )
        assert len(ids) == 3
        assert len(set(ids)) == 3

    def test_raises_for_invalid_file_id(self, session) -> None:
        with pytest.raises(ValueError, match="file with id 99999 does not exist"):
            create_job_executions_batch(
                session=session,
                file_id=99999,
                pipeline_run_id="run-001",
                executions=[{"step_name": "descriptive_statistics"}],
            )

    def test_defaults_for_optional_fields(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        ids = create_job_executions_batch(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            executions=[{"step_name": "data_cleaning"}],
        )
        job = get_job_execution_by_id(session=session, job_execution_id=ids[0])
        assert job is not None
        assert job.status == "pending"
        assert job.retry_count == 0


class TestGetJobExecutionById:
    """Tests for get_job_execution_by_id."""

    def test_returns_job(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        created = create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )
        result = get_job_execution_by_id(session=session, job_execution_id=created.id)
        assert result is not None
        assert result.id == created.id

    def test_returns_none_for_missing_id(self, session) -> None:
        result = get_job_execution_by_id(session=session, job_execution_id=99999)
        assert result is None


class TestListJobExecutions:
    """Tests for list_job_executions."""

    def _create_file_and_jobs(self, session):
        """Helper to create a file with multiple job executions."""
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
            status="completed",
        )
        create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="data_cleaning",
            status="failed",
            retry_count=1,
        )
        create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-002",
            step_name="data_cleaning",
            status="completed",
            retry_count=1,
        )
        return file

    def test_returns_all(self, session) -> None:
        self._create_file_and_jobs(session=session)
        jobs, total = list_job_executions(session=session)
        assert total == 3
        assert len(jobs) == 3

    def test_filter_by_file_id(self, session) -> None:
        file = self._create_file_and_jobs(session=session)
        jobs, total = list_job_executions(session=session, file_id=file.id)
        assert total == 3

    def test_filter_by_pipeline_run_id(self, session) -> None:
        self._create_file_and_jobs(session=session)
        jobs, total = list_job_executions(session=session, pipeline_run_id="run-001")
        assert total == 2

    def test_filter_by_step_name(self, session) -> None:
        self._create_file_and_jobs(session=session)
        jobs, total = list_job_executions(session=session, step_name="data_cleaning")
        assert total == 2

    def test_filter_by_status(self, session) -> None:
        self._create_file_and_jobs(session=session)
        jobs, total = list_job_executions(session=session, status="failed")
        assert total == 1
        assert jobs[0].status == "failed"

    def test_filter_by_retry_count_min(self, session) -> None:
        self._create_file_and_jobs(session=session)
        jobs, total = list_job_executions(session=session, retry_count_min=1)
        assert total == 2

    def test_pagination(self, session) -> None:
        self._create_file_and_jobs(session=session)
        jobs, total = list_job_executions(session=session, limit=1, offset=0)
        assert total == 3
        assert len(jobs) == 1

    def test_empty_result(self, session) -> None:
        jobs, total = list_job_executions(session=session, status="nonexistent")
        assert total == 0
        assert len(jobs) == 0


class TestUpdateJobExecution:
    """Tests for update_job_execution."""

    def test_updates_status(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        job = create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )
        result = update_job_execution(
            session=session,
            job_execution_id=job.id,
            updates={"status": "completed", "computation_time_seconds": 63.2},
        )
        assert result is not None
        assert result.status == "completed"
        assert result.computation_time_seconds == 63.2

    def test_returns_none_for_missing_id(self, session) -> None:
        result = update_job_execution(
            session=session,
            job_execution_id=99999,
            updates={"status": "completed"},
        )
        assert result is None

    def test_ignores_none_values(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        job = create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )
        result = update_job_execution(
            session=session,
            job_execution_id=job.id,
            updates={"status": "running", "error_message": None},
        )
        assert result is not None
        assert result.status == "running"
        assert result.error_message is None


class TestExtractMetadataFromObjectName:
    """Tests for extract_metadata_from_object_name."""

    def test_standard_object_name(self) -> None:
        result = extract_metadata_from_object_name(
            object_name="yellow/2022/01/yellow_tripdata_2022-01.parquet"
        )
        assert result == {"taxi_type": "yellow", "year": "2022", "month": "01"}

    def test_green_taxi(self) -> None:
        result = extract_metadata_from_object_name(
            object_name="green/2023/12/green_tripdata_2023-12.parquet"
        )
        assert result == {"taxi_type": "green", "year": "2023", "month": "12"}

    def test_no_match(self) -> None:
        result = extract_metadata_from_object_name(object_name="flat_file.parquet")
        assert result == {"taxi_type": None, "year": None, "month": None}

    def test_partial_path(self) -> None:
        result = extract_metadata_from_object_name(object_name="yellow/2022/")
        assert result == {"taxi_type": None, "year": None, "month": None}


class TestCreateAnalyticalResult:
    """Tests for create_analytical_result."""

    def test_creates_result(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        job = create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )
        result = create_analytical_result(
            session=session,
            job_execution_id=job.id,
            result_type="descriptive_statistics",
            summary_data={"total_rows": 1000, "avg_fare": 12.5},
            computation_time_seconds=63.2,
            detail_s3_path="results/desc.parquet",
        )
        assert result.id is not None
        assert result.job_execution_id == job.id
        assert result.result_type == "descriptive_statistics"
        assert result.summary_data == {"total_rows": 1000, "avg_fare": 12.5}
        assert result.computation_time_seconds == 63.2
        assert result.detail_s3_path == "results/desc.parquet"

    def test_creates_result_without_s3_path(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        job = create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )
        result = create_analytical_result(
            session=session,
            job_execution_id=job.id,
            result_type="descriptive_statistics",
            summary_data={"key": "value"},
            computation_time_seconds=10.0,
        )
        assert result.detail_s3_path is None

    def test_raises_for_invalid_job_execution_id(self, session) -> None:
        with pytest.raises(
            ValueError, match="job execution with id 99999 does not exist"
        ):
            create_analytical_result(
                session=session,
                job_execution_id=99999,
                result_type="descriptive_statistics",
                summary_data={"key": "value"},
                computation_time_seconds=10.0,
            )


class TestGetAnalyticalResultById:
    """Tests for get_analytical_result_by_id."""

    def test_returns_result(self, session) -> None:
        file = create_or_get_file(session=session, bucket="b", object_name="f.parquet")
        job = create_job_execution(
            session=session,
            file_id=file.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )
        created = create_analytical_result(
            session=session,
            job_execution_id=job.id,
            result_type="descriptive_statistics",
            summary_data={"key": "value"},
            computation_time_seconds=10.0,
        )
        result = get_analytical_result_by_id(session=session, result_id=created.id)
        assert result is not None
        assert result.id == created.id

    def test_returns_none_for_missing_id(self, session) -> None:
        result = get_analytical_result_by_id(session=session, result_id=99999)
        assert result is None


class TestListAnalyticalResults:
    """Tests for list_analytical_results."""

    def _seed_data(self, session):
        """Create files, jobs, and results for filtering tests."""
        f1 = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="yellow/2022/01/yellow_tripdata_2022-01.parquet",
        )
        f2 = create_or_get_file(
            session=session,
            bucket="raw-data",
            object_name="green/2023/06/green_tripdata_2023-06.parquet",
        )
        j1 = create_job_execution(
            session=session,
            file_id=f1.id,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )
        j2 = create_job_execution(
            session=session,
            file_id=f1.id,
            pipeline_run_id="run-001",
            step_name="data_cleaning",
        )
        j3 = create_job_execution(
            session=session,
            file_id=f2.id,
            pipeline_run_id="run-002",
            step_name="descriptive_statistics",
        )
        r1 = create_analytical_result(
            session=session,
            job_execution_id=j1.id,
            result_type="descriptive_statistics",
            summary_data={"rows": 1000},
            computation_time_seconds=60.0,
        )
        r2 = create_analytical_result(
            session=session,
            job_execution_id=j2.id,
            result_type="data_cleaning",
            summary_data={"outliers": 50},
            computation_time_seconds=90.0,
        )
        r3 = create_analytical_result(
            session=session,
            job_execution_id=j3.id,
            result_type="descriptive_statistics",
            summary_data={"rows": 2000},
            computation_time_seconds=45.0,
        )
        return f1, f2, r1, r2, r3

    def test_returns_all(self, session) -> None:
        self._seed_data(session=session)
        results, total = list_analytical_results(session=session)
        assert total == 3
        assert len(results) == 3

    def test_filter_by_result_type(self, session) -> None:
        self._seed_data(session=session)
        results, total = list_analytical_results(
            session=session, result_type="descriptive_statistics"
        )
        assert total == 2

    def test_filter_by_file_id(self, session) -> None:
        f1, _, _, _, _ = self._seed_data(session=session)
        results, total = list_analytical_results(session=session, file_id=f1.id)
        assert total == 2

    def test_filter_by_taxi_type(self, session) -> None:
        self._seed_data(session=session)
        results, total = list_analytical_results(session=session, taxi_type="green")
        assert total == 1
        assert results[0][1].object_name.startswith("green/")

    def test_filter_by_year(self, session) -> None:
        self._seed_data(session=session)
        results, total = list_analytical_results(session=session, year="2023")
        assert total == 1

    def test_filter_by_month(self, session) -> None:
        self._seed_data(session=session)
        results, total = list_analytical_results(session=session, month="01")
        assert total == 2  # yellow/2022/01 has 2 results

    def test_returns_file_info(self, session) -> None:
        self._seed_data(session=session)
        results, _ = list_analytical_results(session=session, taxi_type="yellow")
        ar, file = results[0]
        assert file.bucket == "raw-data"
        assert "yellow" in file.object_name

    def test_pagination(self, session) -> None:
        self._seed_data(session=session)
        results, total = list_analytical_results(session=session, limit=1, offset=0)
        assert total == 3
        assert len(results) == 1

    def test_empty_result(self, session) -> None:
        results, total = list_analytical_results(
            session=session, result_type="nonexistent"
        )
        assert total == 0
        assert len(results) == 0
