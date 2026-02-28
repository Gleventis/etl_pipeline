"""Tests for the API Server client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.services.api_server_client import (
    create_file_record,
    create_job_execution,
    update_file,
    update_job_execution,
)


class TestCreateFileRecord:
    """Tests for the create_file_record function."""

    @patch("src.services.api_server_client.httpx.Client")
    def test_returns_file_id_on_success(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "file_id": 123,
            "bucket": "raw-data",
            "object_name": "yellow/2022/01/file.parquet",
            "overall_status": "pending",
        }
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = create_file_record(
            api_server_url="http://localhost:8000",
            bucket="raw-data",
            object_name="yellow/2022/01/file.parquet",
        )

        assert result == 123
        mock_client.post.assert_called_once_with(
            url="/files",
            json={
                "bucket": "raw-data",
                "object_name": "yellow/2022/01/file.parquet",
                "overall_status": "pending",
            },
            timeout=25.0,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_passes_base_url_and_verify_false(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {"file_id": 1}
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        create_file_record(
            api_server_url="http://api-server:8000",
            bucket="raw-data",
            object_name="file.parquet",
        )

        mock_client_cls.assert_called_once_with(
            base_url="http://api-server:8000",
            verify=False,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_raises_on_http_error(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="server error",
            request=MagicMock(),
            response=mock_response,
        )

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            create_file_record(
                api_server_url="http://localhost:8000",
                bucket="raw-data",
                object_name="file.parquet",
            )

    @patch("src.services.api_server_client.httpx.Client")
    def test_raises_on_network_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.post.side_effect = httpx.ConnectError("connection refused")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.ConnectError):
            create_file_record(
                api_server_url="http://localhost:8000",
                bucket="raw-data",
                object_name="file.parquet",
            )


class TestCreateJobExecution:
    """Tests for the create_job_execution function."""

    @patch("src.services.api_server_client.httpx.Client")
    def test_returns_job_execution_id_on_success(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "job_execution_id": 456,
            "file_id": 123,
            "pipeline_run_id": "run-001",
            "step_name": "descriptive_statistics",
            "status": "pending",
        }
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        result = create_job_execution(
            api_server_url="http://localhost:8000",
            file_id=123,
            pipeline_run_id="run-001",
            step_name="descriptive_statistics",
        )

        assert result == 456
        mock_client.post.assert_called_once_with(
            url="/job-executions",
            json={
                "file_id": 123,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
                "status": "pending",
                "retry_count": 0,
            },
            timeout=25.0,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_passes_base_url_and_verify_false(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.json.return_value = {"job_execution_id": 1}
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        create_job_execution(
            api_server_url="http://api-server:8000",
            file_id=1,
            pipeline_run_id="run-001",
            step_name="data_cleaning",
        )

        mock_client_cls.assert_called_once_with(
            base_url="http://api-server:8000",
            verify=False,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_raises_on_http_error(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="not found",
            request=MagicMock(),
            response=mock_response,
        )

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            create_job_execution(
                api_server_url="http://localhost:8000",
                file_id=999,
                pipeline_run_id="run-001",
                step_name="descriptive_statistics",
            )

    @patch("src.services.api_server_client.httpx.Client")
    def test_raises_on_network_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.post.side_effect = httpx.ConnectError("connection refused")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.ConnectError):
            create_job_execution(
                api_server_url="http://localhost:8000",
                file_id=123,
                pipeline_run_id="run-001",
                step_name="temporal_analysis",
            )


class TestUpdateJobExecution:
    """Tests for the update_job_execution function."""

    @patch("src.services.api_server_client.httpx.Client")
    def test_sends_patch_with_provided_fields(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.patch.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        update_job_execution(
            api_server_url="http://localhost:8000",
            job_execution_id=456,
            status="completed",
            computation_time_seconds=42.5,
        )

        mock_client.patch.assert_called_once_with(
            url="/job-executions/456",
            json={
                "status": "completed",
                "computation_time_seconds": 42.5,
            },
            timeout=25.0,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_excludes_none_fields_from_payload(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.patch.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        update_job_execution(
            api_server_url="http://localhost:8000",
            job_execution_id=1,
            status="failed",
            error_message="timeout",
        )

        mock_client.patch.assert_called_once_with(
            url="/job-executions/1",
            json={
                "status": "failed",
                "error_message": "timeout",
            },
            timeout=25.0,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_passes_base_url_and_verify_false(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.patch.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        update_job_execution(
            api_server_url="http://api-server:8013",
            job_execution_id=1,
            status="running",
        )

        mock_client_cls.assert_called_once_with(
            base_url="http://api-server:8013",
            verify=False,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_raises_on_http_error(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="not found",
            request=MagicMock(),
            response=mock_response,
        )

        mock_client = MagicMock()
        mock_client.patch.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            update_job_execution(
                api_server_url="http://localhost:8000",
                job_execution_id=999,
                status="completed",
            )

    @patch("src.services.api_server_client.httpx.Client")
    def test_raises_on_network_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.patch.side_effect = httpx.ConnectError("connection refused")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.ConnectError):
            update_job_execution(
                api_server_url="http://localhost:8000",
                job_execution_id=1,
                status="completed",
            )


class TestUpdateFile:
    """Tests for the update_file function."""

    @patch("src.services.api_server_client.httpx.Client")
    def test_sends_patch_with_provided_fields(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.patch.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        update_file(
            api_server_url="http://localhost:8000",
            file_id=123,
            overall_status="completed",
            total_computation_seconds=300.5,
        )

        mock_client.patch.assert_called_once_with(
            url="/files/123",
            json={
                "overall_status": "completed",
                "total_computation_seconds": 300.5,
            },
            timeout=25.0,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_excludes_none_fields_from_payload(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.patch.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        update_file(
            api_server_url="http://localhost:8000",
            file_id=1,
            retry_count=2,
        )

        mock_client.patch.assert_called_once_with(
            url="/files/1",
            json={
                "retry_count": 2,
            },
            timeout=25.0,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_passes_base_url_and_verify_false(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        mock_client = MagicMock()
        mock_client.patch.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        update_file(
            api_server_url="http://api-server:8013",
            file_id=1,
            overall_status="in_progress",
        )

        mock_client_cls.assert_called_once_with(
            base_url="http://api-server:8013",
            verify=False,
        )

    @patch("src.services.api_server_client.httpx.Client")
    def test_raises_on_http_error(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message="not found",
            request=MagicMock(),
            response=mock_response,
        )

        mock_client = MagicMock()
        mock_client.patch.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            update_file(
                api_server_url="http://localhost:8000",
                file_id=999,
                overall_status="completed",
            )

    @patch("src.services.api_server_client.httpx.Client")
    def test_raises_on_network_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.patch.side_effect = httpx.ConnectError("connection refused")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.ConnectError):
            update_file(
                api_server_url="http://localhost:8000",
                file_id=1,
                overall_status="completed",
            )
