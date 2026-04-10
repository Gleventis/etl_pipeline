"""Integration test — full branching pipeline execution with real Postgres.

Verifies that the DAG-aware flow executes all 5 steps, persists correct
state in Postgres, and dispatches parallel branches concurrently.

External HTTP services (analyzer, API server) are mocked; database
operations use the real docker-compose Postgres.
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


class TestBranchingPipelineExecution:
    """Integration tests for full DAG pipeline execution with real Postgres."""

    @patch(HTTP_PATCHES[5])
    @patch(HTTP_PATCHES[4])
    @patch(HTTP_PATCHES[3])
    @patch(HTTP_PATCHES[2])
    @patch(HTTP_PATCHES[1])
    @patch(HTTP_PATCHES[0])
    def test_all_steps_complete_and_state_persisted(
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
        """All 5 DAG steps complete; final DB state is 'completed' with all steps."""
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="integ-dag-001",
            steps=DAG_STEPS,
        )

        history = get_job_history(conn=conn)
        assert len(history) == 1
        record = history[0]
        assert record.object_name == "yellow/2022/01/file.parquet"
        assert record.status == "completed"
        assert set(record.completed_steps) == {
            "desc",
            "clean",
            "temporal",
            "geo",
            "fare",
        }
        assert record.failed_step is None
        assert record.dag_steps is not None
        assert len(record.dag_steps) == 5

    @patch(HTTP_PATCHES[5])
    @patch(HTTP_PATCHES[4])
    @patch(HTTP_PATCHES[3])
    @patch(HTTP_PATCHES[2])
    @patch(HTTP_PATCHES[1])
    @patch(HTTP_PATCHES[0])
    def test_parallel_steps_dispatched_concurrently(
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
        """Temporal and geospatial steps are submitted in the same batch."""
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="integ-dag-002",
            steps=DAG_STEPS,
        )

        # Verify dependency order via analyzer dispatch sequence
        dispatched = [c.kwargs["step"] for c in mock_send_job.call_args_list]
        assert dispatched.index("descriptive_statistics") < dispatched.index(
            "data_cleaning"
        )
        assert dispatched.index("data_cleaning") < dispatched.index("temporal_analysis")
        assert dispatched.index("data_cleaning") < dispatched.index(
            "geospatial_analysis"
        )
        assert dispatched.index("temporal_analysis") < dispatched.index(
            "fare_revenue_analysis"
        )
        assert dispatched.index("geospatial_analysis") < dispatched.index(
            "fare_revenue_analysis"
        )

        # Verify temporal and geo were in the same batch (adjacent job creation)
        job_step_names = [c.kwargs["step_name"] for c in mock_create_job.call_args_list]
        temporal_idx = job_step_names.index("temporal")
        geo_idx = job_step_names.index("geo")
        assert abs(temporal_idx - geo_idx) == 1

    @patch(HTTP_PATCHES[5])
    @patch(HTTP_PATCHES[4])
    @patch(HTTP_PATCHES[3])
    @patch(HTTP_PATCHES[2])
    @patch(HTTP_PATCHES[1])
    @patch(HTTP_PATCHES[0])
    def test_dag_edges_persisted_to_api_server(
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
        """DAG edges are sent to the API server at pipeline start."""
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="integ-dag-003",
            steps=DAG_STEPS,
        )

        mock_persist_deps.assert_called_once()
        call_kwargs = mock_persist_deps.call_args.kwargs
        assert call_kwargs["pipeline_run_id"] == "integ-dag-003"
        edges = call_kwargs["edges"]
        assert ("clean", "desc") in edges
        assert ("temporal", "clean") in edges
        assert ("geo", "clean") in edges
        assert ("fare", "temporal") in edges
        assert ("fare", "geo") in edges


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
