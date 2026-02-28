"""Tests for the simplified StateManager."""

import pytest

from src.services.config import Settings
from src.services.database import get_connection, init_schema, save_job_state
from src.services.state_manager import StateManager


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


@pytest.fixture()
def manager(conn) -> StateManager:
    """Provide a StateManager with a clean database."""
    return StateManager(conn=conn)


class TestGetFailedJobs:
    """Tests for StateManager.get_failed_jobs."""

    def test_returns_failed_from_postgres(self, manager: StateManager, conn) -> None:
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
        failed = manager.get_failed_jobs()
        assert len(failed) == 1
        assert failed[0].object_name == "bad.parquet"
        assert failed[0].failed_step == "data_cleaning"

    def test_returns_empty_when_no_failures(self, manager: StateManager, conn) -> None:
        save_job_state(
            conn=conn,
            object_name="ok.parquet",
            bucket="raw-data",
            current_step=None,
            status="completed",
            completed_steps=["descriptive_statistics"],
            failed_step=None,
        )
        failed = manager.get_failed_jobs()
        assert failed == []


class TestGetInProgressJobs:
    """Tests for StateManager.get_in_progress_jobs."""

    def test_returns_in_progress_from_postgres(
        self, manager: StateManager, conn
    ) -> None:
        save_job_state(
            conn=conn,
            object_name="done.parquet",
            bucket="raw-data",
            current_step=None,
            status="completed",
            completed_steps=["descriptive_statistics"],
            failed_step=None,
        )
        save_job_state(
            conn=conn,
            object_name="running.parquet",
            bucket="raw-data",
            current_step="data_cleaning",
            status="in_progress",
            completed_steps=["descriptive_statistics"],
            failed_step=None,
        )
        in_progress = manager.get_in_progress_jobs()
        assert len(in_progress) == 1
        assert in_progress[0].object_name == "running.parquet"

    def test_returns_empty_when_none_in_progress(
        self, manager: StateManager, conn
    ) -> None:
        save_job_state(
            conn=conn,
            object_name="done.parquet",
            bucket="raw-data",
            current_step=None,
            status="completed",
            completed_steps=["descriptive_statistics"],
            failed_step=None,
        )
        in_progress = manager.get_in_progress_jobs()
        assert in_progress == []
