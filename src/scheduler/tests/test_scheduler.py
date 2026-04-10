"""Tests for the scheduler service core logic."""

from unittest.mock import MagicMock, patch

import pytest

from src.server.models import StepDefinition
from src.services.config import Settings
from src.services.database import (
    get_connection,
    init_schema,
    save_job_state,
)
from src.services.pipeline import STEPS
from src.services.scheduler import SchedulerService


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
def settings() -> Settings:
    """Provide default settings."""
    return Settings()


@pytest.fixture()
def service(settings: Settings, postgres_url: str, conn: object) -> SchedulerService:
    """Provide a SchedulerService with a clean database."""
    return SchedulerService(settings=settings, db_url=postgres_url)


class TestScheduleBatch:
    """Tests for SchedulerService.schedule_batch."""

    @patch("src.services.scheduler.process_file_flow")
    def test_single_file_triggers_flow(
        self, mock_flow: MagicMock, service: SchedulerService
    ) -> None:
        statuses = service.schedule_batch(bucket="raw-data", objects=["file1.parquet"])
        assert len(statuses) == 1
        assert statuses[0].object_name == "file1.parquet"
        assert statuses[0].status == "started"
        mock_flow.assert_called_once()

    @patch("src.services.scheduler.process_file_flow")
    def test_multiple_files_trigger_flows(
        self, mock_flow: MagicMock, service: SchedulerService
    ) -> None:
        objects = ["file1.parquet", "file2.parquet"]
        statuses = service.schedule_batch(bucket="raw-data", objects=objects)
        assert len(statuses) == 2
        assert all(s.status == "started" for s in statuses)
        assert mock_flow.call_count == 2

    @patch("src.services.scheduler.process_file_flow")
    def test_already_in_progress_skipped(
        self, mock_flow: MagicMock, service: SchedulerService, conn: object
    ) -> None:
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step=STEPS[1],
            status="in_progress",
            completed_steps=[STEPS[0]],
            failed_step=None,
        )
        statuses = service.schedule_batch(bucket="raw-data", objects=["file1.parquet"])
        assert statuses[0].status == "already_in_progress"
        mock_flow.assert_not_called()

    @patch("src.services.scheduler.uuid")
    @patch("src.services.scheduler.process_file_flow")
    def test_passes_correct_args_to_flow(
        self,
        mock_flow: MagicMock,
        mock_uuid: MagicMock,
        service: SchedulerService,
        settings: Settings,
    ) -> None:
        mock_uuid.uuid4.return_value.hex = "abc123"
        service.schedule_batch(bucket="raw-data", objects=["file1.parquet"])
        mock_flow.assert_called_once_with(
            object_name="file1.parquet",
            bucket="raw-data",
            settings=settings,
            db_url=service._db_url,
            pipeline_run_id="abc123",
            start_step=None,
            skip_checkpoints=[],
            steps=None,
            initial_completed_steps=None,
        )

    @patch("src.services.scheduler.process_file_flow")
    def test_mixed_in_progress_and_new(
        self, mock_flow: MagicMock, service: SchedulerService, conn: object
    ) -> None:
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step=STEPS[0],
            status="in_progress",
            completed_steps=[],
            failed_step=None,
        )
        statuses = service.schedule_batch(
            bucket="raw-data", objects=["file1.parquet", "file2.parquet"]
        )
        assert statuses[0].status == "already_in_progress"
        assert statuses[1].status == "started"
        mock_flow.assert_called_once()

    @patch("src.services.scheduler.uuid")
    @patch("src.services.scheduler.process_file_flow")
    def test_passes_dag_steps_to_flow(
        self,
        mock_flow: MagicMock,
        mock_uuid: MagicMock,
        service: SchedulerService,
        settings: Settings,
    ) -> None:
        mock_uuid.uuid4.return_value.hex = "dag123"
        dag_steps = [
            StepDefinition(name="stats", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="cleaning", action="DATA_CLEANING", after=["stats"]),
        ]
        service.schedule_batch(
            bucket="raw-data", objects=["file1.parquet"], steps=dag_steps
        )
        mock_flow.assert_called_once_with(
            object_name="file1.parquet",
            bucket="raw-data",
            settings=settings,
            db_url=service._db_url,
            pipeline_run_id="dag123",
            start_step=None,
            skip_checkpoints=[],
            steps=dag_steps,
            initial_completed_steps=None,
        )


class TestCheckpointLifecycle:
    """Tests for the full checkpoint lifecycle: schedule → fail → resume."""

    @patch("src.services.scheduler.uuid")
    @patch("src.services.scheduler.process_file_flow")
    def test_schedule_fail_resume_skips_completed_steps(
        self,
        mock_flow: MagicMock,
        mock_uuid: MagicMock,
        service: SchedulerService,
        conn: object,
    ) -> None:
        """Verify that after a mid-pipeline failure, resume_failed triggers
        a flow starting from the failed step, skipping completed steps."""
        mock_uuid.uuid4.return_value.hex = "batch-run-001"

        # 1. schedule_batch triggers the flow; simulate the flow writing
        #    failure state at step 3 (steps 0-1 completed).
        def simulate_failure(**kwargs: object) -> None:
            save_job_state(
                conn=conn,
                object_name="file1.parquet",
                bucket="raw-data",
                current_step=STEPS[2],
                status="failed",
                completed_steps=list(STEPS[:2]),
                failed_step=STEPS[2],
            )

        mock_flow.side_effect = simulate_failure
        service.schedule_batch(bucket="raw-data", objects=["file1.parquet"])
        assert mock_flow.call_count == 1

        # 2. resume_failed should pick up the failed job and restart at step 3.
        mock_flow.reset_mock()
        mock_flow.side_effect = None
        mock_uuid.uuid4.return_value.hex = "resume-run-001"

        resumed = service.resume_failed()

        assert len(resumed) == 1
        assert resumed[0].object_name == "file1.parquet"
        assert resumed[0].restart_step == STEPS[2]
        mock_flow.assert_called_once_with(
            object_name="file1.parquet",
            bucket="raw-data",
            settings=service._settings,
            db_url=service._db_url,
            pipeline_run_id="resume-run-001",
            start_step=STEPS[2],
            skip_checkpoints=[],
            steps=None,
            initial_completed_steps=None,
        )


class TestResumeFailed:
    """Tests for SchedulerService.resume_failed."""

    @patch("src.services.scheduler.uuid")
    @patch("src.services.scheduler.process_file_flow")
    def test_resumes_from_failed_step(
        self,
        mock_flow: MagicMock,
        mock_uuid: MagicMock,
        service: SchedulerService,
        conn: object,
    ) -> None:
        mock_uuid.uuid4.return_value.hex = "resume123"
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step=STEPS[2],
            status="failed",
            completed_steps=list(STEPS[:2]),
            failed_step=STEPS[2],
        )
        resumed = service.resume_failed()
        assert len(resumed) == 1
        assert resumed[0].object_name == "file1.parquet"
        assert resumed[0].restart_step == STEPS[2]
        mock_flow.assert_called_once_with(
            object_name="file1.parquet",
            bucket="raw-data",
            settings=service._settings,
            db_url=service._db_url,
            pipeline_run_id="resume123",
            start_step=STEPS[2],
            skip_checkpoints=[],
            steps=None,
            initial_completed_steps=None,
        )

    @patch("src.services.scheduler.process_file_flow")
    def test_no_failed_jobs_returns_empty(
        self, mock_flow: MagicMock, service: SchedulerService
    ) -> None:
        resumed = service.resume_failed()
        assert resumed == []
        mock_flow.assert_not_called()

    @patch("src.services.scheduler.process_file_flow")
    def test_multiple_failed_jobs_resumed(
        self, mock_flow: MagicMock, service: SchedulerService, conn: object
    ) -> None:
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step=STEPS[1],
            status="failed",
            completed_steps=[STEPS[0]],
            failed_step=STEPS[1],
        )
        save_job_state(
            conn=conn,
            object_name="file2.parquet",
            bucket="raw-data",
            current_step=STEPS[3],
            status="failed",
            completed_steps=list(STEPS[:3]),
            failed_step=STEPS[3],
        )
        resumed = service.resume_failed()
        assert len(resumed) == 2
        assert mock_flow.call_count == 2

    @patch("src.services.scheduler.uuid")
    @patch("src.services.scheduler.process_file_flow")
    def test_dag_resume_passes_steps_and_completed(
        self,
        mock_flow: MagicMock,
        mock_uuid: MagicMock,
        service: SchedulerService,
        conn: object,
    ) -> None:
        """DAG-aware resume passes original steps and completed_steps to flow."""
        mock_uuid.uuid4.return_value.hex = "dag-resume-001"
        dag_steps = [
            {
                "name": "stats",
                "action": "DESCRIPTIVE_STATISTICS",
                "checkpoint": True,
                "after": [],
            },
            {
                "name": "cleaning",
                "action": "DATA_CLEANING",
                "checkpoint": True,
                "after": ["stats"],
            },
            {
                "name": "temporal",
                "action": "TEMPORAL_ANALYSIS",
                "checkpoint": True,
                "after": ["cleaning"],
            },
            {
                "name": "geospatial",
                "action": "GEOSPATIAL_ANALYSIS",
                "checkpoint": True,
                "after": ["cleaning"],
            },
            {
                "name": "fare",
                "action": "FARE_REVENUE_ANALYSIS",
                "checkpoint": True,
                "after": ["temporal", "geospatial"],
            },
        ]
        save_job_state(
            conn=conn,
            object_name="file1.parquet",
            bucket="raw-data",
            current_step="geospatial",
            status="failed",
            completed_steps=["stats", "cleaning", "temporal"],
            failed_step="geospatial",
            dag_steps=dag_steps,
        )
        resumed = service.resume_failed()
        assert len(resumed) == 1
        assert resumed[0].restart_step == "geospatial"

        call_kwargs = mock_flow.call_args.kwargs
        assert call_kwargs["start_step"] is None
        assert call_kwargs["initial_completed_steps"] == [
            "stats",
            "cleaning",
            "temporal",
        ]
        assert call_kwargs["steps"] is not None
        assert len(call_kwargs["steps"]) == 5
        assert call_kwargs["steps"][0].name == "stats"
        assert call_kwargs["steps"][3].after == ["cleaning"]

    @patch("src.services.scheduler.uuid")
    @patch("src.services.scheduler.process_file_flow")
    def test_dag_resume_lifecycle_schedule_fail_resume(
        self,
        mock_flow: MagicMock,
        mock_uuid: MagicMock,
        service: SchedulerService,
        conn: object,
    ) -> None:
        """Full lifecycle: schedule DAG → fail at branch → resume with DAG."""
        mock_uuid.uuid4.return_value.hex = "dag-batch-001"
        dag_steps = [
            StepDefinition(name="stats", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="cleaning", action="DATA_CLEANING", after=["stats"]),
            StepDefinition(
                name="temporal", action="TEMPORAL_ANALYSIS", after=["cleaning"]
            ),
            StepDefinition(
                name="geospatial", action="GEOSPATIAL_ANALYSIS", after=["cleaning"]
            ),
            StepDefinition(
                name="fare",
                action="FARE_REVENUE_ANALYSIS",
                after=["temporal", "geospatial"],
            ),
        ]
        dag_steps_json = [s.model_dump() for s in dag_steps]

        def simulate_dag_failure(**kwargs: object) -> None:
            save_job_state(
                conn=conn,
                object_name="file1.parquet",
                bucket="raw-data",
                current_step="geospatial",
                status="failed",
                completed_steps=["stats", "cleaning", "temporal"],
                failed_step="geospatial",
                dag_steps=dag_steps_json,
            )

        mock_flow.side_effect = simulate_dag_failure
        service.schedule_batch(
            bucket="raw-data", objects=["file1.parquet"], steps=dag_steps
        )
        assert mock_flow.call_count == 1

        mock_flow.reset_mock()
        mock_flow.side_effect = None
        mock_uuid.uuid4.return_value.hex = "dag-resume-001"

        resumed = service.resume_failed()
        assert len(resumed) == 1
        assert resumed[0].restart_step == "geospatial"

        call_kwargs = mock_flow.call_args.kwargs
        assert call_kwargs["start_step"] is None
        assert call_kwargs["initial_completed_steps"] == [
            "stats",
            "cleaning",
            "temporal",
        ]
        assert call_kwargs["steps"] is not None
        assert len(call_kwargs["steps"]) == 5


if __name__ == "__main__":
    import pytest as _pytest

    _pytest.main([__file__, "-v"])
