"""Integration test — partial branch failure and DAG-aware resume.

Verifies that when a parallel branch fails (geo fails, temporal succeeds),
the scheduler persists correct state and resume re-runs only the incomplete
branch (geo + fare), not the entire pipeline.

External HTTP services are mocked; database operations use real Postgres.
"""

from unittest.mock import patch

import pytest

from src.server.models import StepDefinition
from src.services.analyzer_client import AnalyzerResponse
from src.services.config import Settings
from src.services.database import get_connection, get_job_history, init_schema
from src.services.prefect_flows import process_file_flow

DAG_STEPS = [
    StepDefinition(name="desc", action="DESCRIPTIVE_STATISTICS"),
    StepDefinition(name="clean", action="DATA_CLEANING", after=["desc"]),
    StepDefinition(name="temporal", action="TEMPORAL_ANALYSIS", after=["clean"]),
    StepDefinition(name="geo", action="GEOSPATIAL_ANALYSIS", after=["clean"]),
    StepDefinition(
        name="fare", action="FARE_REVENUE_ANALYSIS", after=["temporal", "geo"]
    ),
]

HTTP_PATCHES = [
    "src.services.prefect_flows.create_file_record",
    "src.services.prefect_flows.create_job_execution",
    "src.services.prefect_flows.send_job",
    "src.services.prefect_flows.update_job_execution",
    "src.services.prefect_flows.update_file",
    "src.services.prefect_flows.persist_step_dependencies",
]


def _geo_fails(**kwargs: object) -> AnalyzerResponse:
    """Return failure only for geospatial_analysis, success otherwise."""
    if kwargs.get("step") == "geospatial_analysis":
        return AnalyzerResponse(success=False, error="simulated geo failure")
    return AnalyzerResponse(success=True)


@pytest.fixture(scope="module")
def postgres_url() -> str:
    """Return the Postgres URL from the docker-compose environment."""
    return Settings().DATABASE_URL


@pytest.fixture()
def conn(postgres_url: str):
    """Provide a fresh connection with initialized schema per test."""
    with get_connection(database_url=postgres_url) as connection:
        init_schema(conn=connection)
        with connection.cursor() as cur:
            cur.execute("DELETE FROM job_state;")
        connection.commit()
        yield connection


class TestPartialBranchFailure:
    """Tests for partial branch failure and DAG-aware resume."""

    @patch(HTTP_PATCHES[5])
    @patch(HTTP_PATCHES[4])
    @patch(HTTP_PATCHES[3])
    @patch(HTTP_PATCHES[2])
    @patch(HTTP_PATCHES[1])
    @patch(HTTP_PATCHES[0])
    def test_geo_failure_preserves_temporal_success(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_update_job,
        mock_update_file,
        mock_persist_deps,
        conn,
        postgres_url: str,
    ) -> None:
        """When geo fails in parallel batch, temporal result is preserved."""
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104]
        mock_send_job.side_effect = _geo_fails

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="resume-001",
            steps=DAG_STEPS,
        )

        history = get_job_history(conn=conn)
        assert len(history) == 1
        record = history[0]
        assert record.status == "failed"
        assert record.failed_step == "geo"
        assert set(record.completed_steps) == {"desc", "clean", "temporal"}
        assert record.dag_steps is not None
        assert len(record.dag_steps) == 5

    @patch(HTTP_PATCHES[5])
    @patch(HTTP_PATCHES[4])
    @patch(HTTP_PATCHES[3])
    @patch(HTTP_PATCHES[2])
    @patch(HTTP_PATCHES[1])
    @patch(HTTP_PATCHES[0])
    def test_resume_reruns_only_geo_and_fare(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_update_job,
        mock_update_file,
        mock_persist_deps,
        conn,
        postgres_url: str,
    ) -> None:
        """Resume after geo failure re-runs only geo and fare, not desc/clean/temporal."""
        # --- initial run: geo fails -----------------------------------------
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104]
        mock_send_job.side_effect = _geo_fails

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="resume-002a",
            steps=DAG_STEPS,
        )

        # --- resume: all steps succeed --------------------------------------
        mock_create_file.reset_mock()
        mock_create_job.reset_mock()
        mock_send_job.reset_mock()
        mock_update_job.reset_mock()
        mock_update_file.reset_mock()
        mock_persist_deps.reset_mock()

        mock_create_file.return_value = 11
        mock_create_job.side_effect = [201, 202]
        mock_send_job.side_effect = None
        mock_send_job.return_value = AnalyzerResponse(success=True)

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="resume-002b",
            steps=DAG_STEPS,
            initial_completed_steps=["desc", "clean", "temporal"],
        )

        # only geo and fare were dispatched to analyzer
        dispatched = [c.kwargs["step"] for c in mock_send_job.call_args_list]
        assert dispatched == ["geospatial_analysis", "fare_revenue_analysis"]

        # final DB state is completed with all 5 steps
        history = get_job_history(conn=conn)
        assert len(history) == 1
        record = history[0]
        assert record.status == "completed"
        assert set(record.completed_steps) == {
            "desc",
            "clean",
            "temporal",
            "geo",
            "fare",
        }
        assert record.failed_step is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
