"""Tests for scheduler routes."""

from unittest.mock import MagicMock

from fastapi import status
from fastapi.testclient import TestClient

from src.server.main import app
from src.server.models import FileStatus, ResumedJob

mock_service = MagicMock()
app.state.scheduler_service = mock_service

client = TestClient(app=app, raise_server_exceptions=True)


class TestScheduleEndpoint:
    """Tests for POST /scheduler/schedule."""

    def test_schedule_returns_202(self) -> None:
        mock_service.schedule_batch.return_value = [
            FileStatus(object_name="file1.parquet", status="started")
        ]
        response = client.post(
            "/scheduler/schedule",
            json={"bucket": "raw-data", "objects": ["file1.parquet"]},
        )
        assert response.status_code == status.HTTP_202_ACCEPTED

    def test_schedule_returns_files_list(self) -> None:
        mock_service.schedule_batch.return_value = [
            FileStatus(object_name="file1.parquet", status="started")
        ]
        response = client.post(
            "/scheduler/schedule",
            json={"bucket": "raw-data", "objects": ["file1.parquet"]},
        )
        body = response.json()
        assert "files" in body
        assert isinstance(body["files"], list)
        assert body["files"][0]["object_name"] == "file1.parquet"
        assert body["files"][0]["status"] == "started"

    def test_schedule_rejects_empty_bucket(self) -> None:
        response = client.post(
            "/scheduler/schedule",
            json={"bucket": "", "objects": ["file1.parquet"]},
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_schedule_rejects_empty_objects(self) -> None:
        response = client.post(
            "/scheduler/schedule",
            json={"bucket": "raw-data", "objects": []},
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_schedule_rejects_missing_body(self) -> None:
        response = client.post("/scheduler/schedule")
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_schedule_calls_service_with_objects(self) -> None:
        mock_service.schedule_batch.return_value = []
        client.post(
            "/scheduler/schedule",
            json={"bucket": "raw-data", "objects": ["a.parquet", "b.parquet"]},
        )
        mock_service.schedule_batch.assert_called_with(
            bucket="raw-data", objects=["a.parquet", "b.parquet"]
        )


class TestResumeEndpoint:
    """Tests for POST /scheduler/resume."""

    def test_resume_returns_200(self) -> None:
        mock_service.resume_failed.return_value = []
        response = client.post("/scheduler/resume")
        assert response.status_code == status.HTTP_200_OK

    def test_resume_returns_resumed_list(self) -> None:
        mock_service.resume_failed.return_value = [
            ResumedJob(
                object_name="file1.parquet",
                restart_step="data_cleaning",
            )
        ]
        response = client.post("/scheduler/resume")
        body = response.json()
        assert "resumed" in body
        assert len(body["resumed"]) == 1
        assert body["resumed"][0]["object_name"] == "file1.parquet"
        assert body["resumed"][0]["restart_step"] == "data_cleaning"


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
