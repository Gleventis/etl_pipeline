"""Integration test — backward compatibility with linear pipeline (no DAG).

Verifies that the existing linear pipeline executes correctly end-to-end
when ``steps=None``, ensuring the branching pipeline changes introduce no
regressions.

External HTTP services are mocked; database operations use real Postgres.
"""

from unittest.mock import patch

import pytest

from src.services.analyzer_client import AnalyzerResponse
from src.services.config import Settings
from src.services.database import get_connection, get_job_history, init_schema
from src.services.prefect_flows import process_file_flow

LINEAR_STEPS = [
    "descriptive_statistics",
    "data_cleaning",
    "temporal_analysis",
    "geospatial_analysis",
    "fare_revenue_analysis",
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


class TestLinearPipelineCompat:
    """Integration tests for linear pipeline backward compatibility."""

    @patch(HTTP_PATCHES[5])
    @patch(HTTP_PATCHES[4])
    @patch(HTTP_PATCHES[3])
    @patch(HTTP_PATCHES[2])
    @patch(HTTP_PATCHES[1])
    @patch(HTTP_PATCHES[0])
    def test_all_steps_complete_sequentially(
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
        """All 5 linear steps complete; final DB state is 'completed'."""
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="linear-compat-001",
            steps=None,
        )

        history = get_job_history(conn=conn)
        assert len(history) == 1
        record = history[0]
        assert record.object_name == "yellow/2022/01/file.parquet"
        assert record.status == "completed"
        assert record.completed_steps == LINEAR_STEPS
        assert record.failed_step is None
        assert record.dag_steps is None

    @patch(HTTP_PATCHES[5])
    @patch(HTTP_PATCHES[4])
    @patch(HTTP_PATCHES[3])
    @patch(HTTP_PATCHES[2])
    @patch(HTTP_PATCHES[1])
    @patch(HTTP_PATCHES[0])
    def test_steps_dispatched_in_order(
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
        """Steps are dispatched to the analyzer in the exact linear order."""
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="linear-compat-002",
            steps=None,
        )

        dispatched = [c.kwargs["step"] for c in mock_send_job.call_args_list]
        assert dispatched == LINEAR_STEPS

    @patch(HTTP_PATCHES[5])
    @patch(HTTP_PATCHES[4])
    @patch(HTTP_PATCHES[3])
    @patch(HTTP_PATCHES[2])
    @patch(HTTP_PATCHES[1])
    @patch(HTTP_PATCHES[0])
    def test_no_dag_dependencies_persisted(
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
        """Linear mode does not call persist_step_dependencies."""
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)

        process_file_flow(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=Settings(),
            db_url=postgres_url,
            pipeline_run_id="linear-compat-003",
            steps=None,
        )

        mock_persist_deps.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
