"""Tests for scheduler database layer using docker-compose Postgres."""

import pytest

from src.services.config import Settings
from src.services.database import (
    JobRecord,
    get_connection,
    get_failed_jobs,
    get_job_history,
    init_schema,
    save_job_state,
)


@pytest.fixture(scope="module")
def postgres_url() -> str:
    """Return the Postgres URL from the docker-compose environment."""
    settings = Settings()
    return settings.DATABASE_URL


@pytest.fixture()
def conn(postgres_url: str):
    """Provide a fresh connection with initialized schema per test."""
    with get_connection(database_url=postgres_url) as connection:
        init_schema(conn=connection)
        with connection.cursor() as cur:
            cur.execute("DELETE FROM job_state;")
        connection.commit()
        yield connection


class TestInitSchema:
    """Tests for init_schema."""

    def test_creates_table(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT FROM information_schema.tables"
                "  WHERE table_name = 'job_state'"
                ");"
            )
            assert cur.fetchone()[0] is True

    def test_idempotent(self, conn) -> None:
        init_schema(conn=conn)


class TestSaveJobState:
    """Tests for save_job_state."""

    def test_insert_new_job(self, conn) -> None:
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step="descriptive_statistics",
            status="in_progress",
            completed_steps=[],
            failed_step=None,
        )
        history = get_job_history(conn=conn)
        assert len(history) == 1
        assert history[0].object_name == "file1.parquet"
        assert history[0].status == "in_progress"

    def test_upsert_existing_job(self, conn) -> None:
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step="descriptive_statistics",
            status="in_progress",
            completed_steps=[],
            failed_step=None,
        )
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step="data_cleaning",
            status="in_progress",
            completed_steps=["descriptive_statistics"],
            failed_step=None,
        )
        history = get_job_history(conn=conn)
        assert len(history) == 1
        assert history[0].current_step == "data_cleaning"
        assert history[0].completed_steps == ["descriptive_statistics"]

    def test_completed_steps_persisted_as_list(self, conn) -> None:
        steps = ["descriptive_statistics", "data_cleaning"]
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step="temporal_analysis",
            status="in_progress",
            completed_steps=steps,
            failed_step=None,
        )
        history = get_job_history(conn=conn)
        assert history[0].completed_steps == steps


class TestGetFailedJobs:
    """Tests for get_failed_jobs."""

    def test_returns_only_failed(self, conn) -> None:
        save_job_state(
            conn=conn,
            object_name="ok.parquet",
            bucket="raw-data",
            current_step=None,
            status="completed",
            completed_steps=["descriptive_statistics"],
            failed_step=None,
        )
        save_job_state(
            conn=conn,
            object_name="bad.parquet",
            bucket="raw-data",
            current_step="data_cleaning",
            status="failed",
            completed_steps=["descriptive_statistics"],
            failed_step="data_cleaning",
        )
        failed = get_failed_jobs(conn=conn)
        assert len(failed) == 1
        assert failed[0].object_name == "bad.parquet"
        assert failed[0].failed_step == "data_cleaning"

    def test_returns_empty_when_no_failures(self, conn) -> None:
        save_job_state(
            conn=conn,
            object_name="ok.parquet",
            bucket="raw-data",
            current_step=None,
            status="completed",
            completed_steps=["descriptive_statistics"],
            failed_step=None,
        )
        failed = get_failed_jobs(conn=conn)
        assert failed == []


class TestGetJobHistory:
    """Tests for get_job_history."""

    def test_returns_all_jobs_ordered(self, conn) -> None:
        save_job_state(
            conn=conn,
            object_name="first.parquet",
            bucket="raw-data",
            current_step=None,
            status="completed",
            completed_steps=[],
            failed_step=None,
        )
        save_job_state(
            conn=conn,
            object_name="second.parquet",
            bucket="raw-data",
            current_step="descriptive_statistics",
            status="in_progress",
            completed_steps=[],
            failed_step=None,
        )
        history = get_job_history(conn=conn)
        assert len(history) == 2
        assert history[0].object_name == "first.parquet"
        assert history[1].object_name == "second.parquet"

    def test_returns_empty_when_no_jobs(self, conn) -> None:
        history = get_job_history(conn=conn)
        assert history == []


class TestJobRecord:
    """Tests for JobRecord model."""

    def test_frozen(self) -> None:
        record = JobRecord(
            job_id=1,
            object_name="file.parquet",
            bucket="raw-data",
            status="pending",
        )
        with pytest.raises(Exception):
            record.status = "completed"
