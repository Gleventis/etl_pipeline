"""Tests for Prefect flow and task definitions."""

from unittest.mock import MagicMock, call, patch

from src.services.analyzer_client import AnalyzerResponse
from src.services.config import Settings
from src.services.pipeline import STEPS
from src.services.prefect_flows import execute_step, process_file_flow

FLOW_PATCHES = [
    "src.services.prefect_flows.create_file_record",
    "src.services.prefect_flows.create_job_execution",
    "src.services.prefect_flows.send_job",
    "src.services.prefect_flows.get_connection",
    "src.services.prefect_flows.save_job_state",
    "src.services.prefect_flows.update_job_execution",
    "src.services.prefect_flows.update_file",
]


class TestExecuteStep:
    """Tests for the execute_step Prefect task."""

    @patch("src.services.prefect_flows.send_job")
    def test_successful_step(self, mock_send_job) -> None:
        mock_send_job.return_value = AnalyzerResponse(success=True)

        result = execute_step.fn(
            step="descriptive_statistics",
            input_bucket="raw-data",
            object_name="yellow/2022/01/file.parquet",
            analyzer_url="http://analyzer:8002",
            taxi_type="yellow",
            job_execution_id=42,
            timeout=300.0,
        )

        assert result.success is True
        assert result.error is None
        mock_send_job.assert_called_once_with(
            analyzer_url="http://analyzer:8002",
            step="descriptive_statistics",
            input_bucket="raw-data",
            input_object="yellow/2022/01/file.parquet",
            taxi_type="yellow",
            job_execution_id=42,
            timeout=300.0,
        )

    @patch("src.services.prefect_flows.send_job")
    def test_failed_step(self, mock_send_job) -> None:
        mock_send_job.return_value = AnalyzerResponse(
            success=False, error="analyzer error"
        )

        result = execute_step.fn(
            step="data_cleaning",
            input_bucket="raw-data",
            object_name="green/2022/01/file.parquet",
            analyzer_url="http://analyzer:8002",
            taxi_type="green",
            job_execution_id=99,
            timeout=300.0,
        )

        assert result.success is False
        assert result.error == "analyzer error"

    @patch("src.services.prefect_flows.send_job")
    def test_delegates_to_send_job_with_correct_args(self, mock_send_job) -> None:
        mock_send_job.return_value = AnalyzerResponse(success=True)

        execute_step.fn(
            step="temporal_analysis",
            input_bucket="cleaned-data",
            object_name="fhvhv/2022/01/file.parquet",
            analyzer_url="http://localhost:9999",
            taxi_type="fhvhv",
            job_execution_id=7,
            timeout=300.0,
        )

        mock_send_job.assert_called_once_with(
            analyzer_url="http://localhost:9999",
            step="temporal_analysis",
            input_bucket="cleaned-data",
            input_object="fhvhv/2022/01/file.parquet",
            taxi_type="fhvhv",
            job_execution_id=7,
            timeout=300.0,
        )


class TestProcessFileFlow:
    """Tests for the process_file_flow Prefect flow."""

    def _setup_mocks(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_all_steps_succeed(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-001",
        )

        assert mock_send_job.call_count == len(STEPS)
        assert mock_create_job.call_count == len(STEPS)
        mock_create_file.assert_called_once_with(
            api_server_url=settings.API_SERVER_URL,
            bucket="raw-data",
            object_name="yellow/2022/01/file.parquet",
        )
        # Initial persist + one persist per step = 1 + 5 = 6
        assert mock_save.call_count == 1 + len(STEPS)
        last_call = mock_save.call_args
        assert last_call.kwargs["status"] == "completed"
        assert last_call.kwargs["completed_steps"] == list(STEPS)
        # 2 update_job_execution calls per step (running + completed) = 10
        assert mock_update_job.call_count == 2 * len(STEPS)
        for i, step_id in enumerate([101, 102, 103, 104, 105]):
            running_call = mock_update_job.call_args_list[i * 2]
            assert running_call.kwargs["job_execution_id"] == step_id
            assert running_call.kwargs["status"] == "running"
            assert "started_at" in running_call.kwargs
            completed_call = mock_update_job.call_args_list[i * 2 + 1]
            assert completed_call.kwargs["job_execution_id"] == step_id
            assert completed_call.kwargs["status"] == "completed"
            assert "completed_at" in completed_call.kwargs
            assert "computation_time_seconds" in completed_call.kwargs
        # update_file: 1 in_progress + 5 per-step computation + 1 final completed = 7
        assert mock_update_file.call_count == 1 + len(STEPS) + 1
        # First call: set in_progress (no retry)
        first_file_call = mock_update_file.call_args_list[0]
        assert first_file_call.kwargs["file_id"] == 10
        assert first_file_call.kwargs["overall_status"] == "in_progress"
        assert first_file_call.kwargs.get("retry_count") is None
        # Per-step calls: cumulative computation time
        for i in range(len(STEPS)):
            step_file_call = mock_update_file.call_args_list[1 + i]
            assert step_file_call.kwargs["file_id"] == 10
            assert "total_computation_seconds" in step_file_call.kwargs
            assert step_file_call.kwargs["total_computation_seconds"] > 0
        # Final call: completed with elapsed time
        final_file_call = mock_update_file.call_args_list[-1]
        assert final_file_call.kwargs["file_id"] == 10
        assert final_file_call.kwargs["overall_status"] == "completed"
        assert "total_computation_seconds" in final_file_call.kwargs
        assert "total_elapsed_seconds" in final_file_call.kwargs

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_extracts_taxi_type_from_object_path(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.return_value = 100
        mock_send_job.return_value = AnalyzerResponse(success=True)
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="green/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-002",
        )

        # All send_job calls should have taxi_type="green"
        for step_call in mock_send_job.call_args_list:
            assert step_call.kwargs["taxi_type"] == "green"

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_creates_job_execution_per_step(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-003",
        )

        expected_calls = [
            call(
                api_server_url=settings.API_SERVER_URL,
                file_id=10,
                pipeline_run_id="run-003",
                step_name=step,
            )
            for step in STEPS
        ]
        assert mock_create_job.call_args_list == expected_calls

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_passes_job_execution_id_to_send_job(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-004",
        )

        for i, step_call in enumerate(mock_send_job.call_args_list):
            assert step_call.kwargs["job_execution_id"] == 101 + i

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_fails_at_second_step(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102]
        mock_send_job.side_effect = [
            AnalyzerResponse(success=True),
            AnalyzerResponse(success=False, error="boom"),
        ]
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-005",
        )

        assert mock_send_job.call_count == 2
        assert mock_create_job.call_count == 2
        # Initial persist + step 1 success persist + failure persist = 3
        assert mock_save.call_count == 3
        last_call = mock_save.call_args
        assert last_call.kwargs["status"] == "failed"
        assert last_call.kwargs["failed_step"] == STEPS[1]
        assert last_call.kwargs["completed_steps"] == [STEPS[0]]
        # Step 1: running + completed = 2, Step 2: running + failed = 2 → total 4
        assert mock_update_job.call_count == 4
        failed_call = mock_update_job.call_args_list[3]
        assert failed_call.kwargs["job_execution_id"] == 102
        assert failed_call.kwargs["status"] == "failed"
        assert failed_call.kwargs["error_message"] == "boom"
        # update_file: 1 in_progress + 1 step1 computation + 1 failed = 3
        assert mock_update_file.call_count == 3
        assert (
            mock_update_file.call_args_list[0].kwargs["overall_status"] == "in_progress"
        )
        assert "total_computation_seconds" in mock_update_file.call_args_list[1].kwargs
        final_file_call = mock_update_file.call_args_list[-1]
        assert final_file_call.kwargs["overall_status"] == "failed"
        assert "total_computation_seconds" in final_file_call.kwargs
        assert "total_elapsed_seconds" in final_file_call.kwargs

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_resume_from_third_step(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-006",
            start_step=STEPS[2],
        )

        # Only steps 2, 3, 4 should be dispatched
        assert mock_send_job.call_count == 3
        assert mock_create_job.call_count == 3
        # Initial persist + 3 step persists = 4
        assert mock_save.call_count == 4
        first_call = mock_save.call_args_list[0]
        assert first_call.kwargs["completed_steps"] == list(STEPS[:2])
        assert first_call.kwargs["current_step"] == STEPS[2]
        last_call = mock_save.call_args
        assert last_call.kwargs["status"] == "completed"
        assert last_call.kwargs["completed_steps"] == list(STEPS)
        # update_file: 1 in_progress (with retry_count=1) + 3 per-step + 1 completed = 5
        assert mock_update_file.call_count == 5
        first_file_call = mock_update_file.call_args_list[0]
        assert first_file_call.kwargs["overall_status"] == "in_progress"
        assert first_file_call.kwargs["retry_count"] == 1
        final_file_call = mock_update_file.call_args_list[-1]
        assert final_file_call.kwargs["overall_status"] == "completed"
        assert "total_elapsed_seconds" in final_file_call.kwargs

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_fails_at_first_step(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.return_value = 101
        mock_send_job.return_value = AnalyzerResponse(
            success=False, error="immediate fail"
        )
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-007",
        )

        assert mock_send_job.call_count == 1
        # Initial persist + failure persist = 2
        assert mock_save.call_count == 2
        last_call = mock_save.call_args
        assert last_call.kwargs["status"] == "failed"
        assert last_call.kwargs["failed_step"] == STEPS[0]
        assert last_call.kwargs["completed_steps"] == []
        # running + failed = 2
        assert mock_update_job.call_count == 2
        assert mock_update_job.call_args_list[0].kwargs["status"] == "running"
        assert mock_update_job.call_args_list[1].kwargs["status"] == "failed"
        assert (
            mock_update_job.call_args_list[1].kwargs["error_message"]
            == "immediate fail"
        )
        # update_file: 1 in_progress + 1 failed = 2 (no per-step computation on failure)
        assert mock_update_file.call_count == 2
        assert (
            mock_update_file.call_args_list[0].kwargs["overall_status"] == "in_progress"
        )
        final_file_call = mock_update_file.call_args_list[-1]
        assert final_file_call.kwargs["overall_status"] == "failed"
        assert "total_elapsed_seconds" in final_file_call.kwargs

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_passes_correct_buckets_per_step(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.side_effect = [101, 102, 103, 104, 105]
        mock_send_job.return_value = AnalyzerResponse(success=True)
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-008",
        )

        for i, step_call in enumerate(mock_send_job.call_args_list):
            expected_bucket = getattr(settings, f"STEP_{STEPS[i].upper()}_BUCKET")
            assert step_call.kwargs["input_bucket"] == expected_bucket
            assert step_call.kwargs["step"] == STEPS[i]

    @patch("src.services.prefect_flows.update_file")
    @patch("src.services.prefect_flows.update_job_execution")
    @patch("src.services.prefect_flows.save_job_state")
    @patch("src.services.prefect_flows.get_connection")
    @patch("src.services.prefect_flows.send_job")
    @patch("src.services.prefect_flows.create_job_execution")
    @patch("src.services.prefect_flows.create_file_record")
    def test_resume_from_last_step(
        self,
        mock_create_file,
        mock_create_job,
        mock_send_job,
        mock_get_conn,
        mock_save,
        mock_update_job,
        mock_update_file,
    ) -> None:
        mock_create_file.return_value = 10
        mock_create_job.return_value = 105
        mock_send_job.return_value = AnalyzerResponse(success=True)
        self._setup_mocks(mock_get_conn)
        settings = Settings()

        process_file_flow.fn(
            object_name="yellow/2022/01/file.parquet",
            bucket="raw-data",
            settings=settings,
            db_url="postgresql://test:test@localhost/test",
            pipeline_run_id="run-009",
            start_step=STEPS[-1],
        )

        assert mock_send_job.call_count == 1
        last_call = mock_save.call_args
        assert last_call.kwargs["status"] == "completed"
        assert last_call.kwargs["completed_steps"] == list(STEPS)
