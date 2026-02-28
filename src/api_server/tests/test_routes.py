"""Tests for API server Files routes using docker-compose Postgres."""

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.server.main import app
from src.services.config import Settings
from src.services.database import get_session, init_schema, reset_globals


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


@pytest.fixture(autouse=True)
def _clean_tables(database_url: str):
    """Clean all tables before each test."""
    with get_session(database_url=database_url) as session:
        session.execute(text("DELETE FROM analytical_results"))
        session.execute(text("DELETE FROM job_executions"))
        session.execute(text("DELETE FROM files"))
        session.commit()


@pytest.fixture()
def client() -> TestClient:
    """Provide a FastAPI TestClient."""
    return TestClient(app=app, raise_server_exceptions=False)


class TestPostFile:
    """Tests for POST /files."""

    def test_creates_file_returns_201(self, client: TestClient) -> None:
        response = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/test.parquet"},
        )
        assert response.status_code == status.HTTP_201_CREATED
        body = response.json()
        assert body["bucket"] == "raw-data"
        assert body["object_name"] == "yellow/2022/01/test.parquet"
        assert body["overall_status"] == "pending"
        assert body["file_id"] > 0

    def test_idempotent_returns_same_file(self, client: TestClient) -> None:
        payload = {"bucket": "raw-data", "object_name": "yellow/2022/01/test.parquet"}
        r1 = client.post("/files", json=payload)
        r2 = client.post("/files", json=payload)
        assert r1.json()["file_id"] == r2.json()["file_id"]

    def test_custom_status(self, client: TestClient) -> None:
        response = client.post(
            "/files",
            json={
                "bucket": "raw-data",
                "object_name": "green/2022/01/test.parquet",
                "overall_status": "processing",
            },
        )
        assert response.status_code == status.HTTP_201_CREATED
        assert response.json()["overall_status"] == "processing"

    def test_rejects_empty_bucket(self, client: TestClient) -> None:
        response = client.post(
            "/files",
            json={"bucket": "", "object_name": "test.parquet"},
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_rejects_missing_object_name(self, client: TestClient) -> None:
        response = client.post("/files", json={"bucket": "raw-data"})
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_response_has_timestamps(self, client: TestClient) -> None:
        response = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "test.parquet"},
        )
        body = response.json()
        assert body["created_at"] is not None
        assert body["updated_at"] is not None

    def test_response_has_default_numeric_fields(self, client: TestClient) -> None:
        response = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "test.parquet"},
        )
        body = response.json()
        assert body["total_computation_seconds"] == 0.0
        assert body["total_elapsed_seconds"] == 0.0
        assert body["retry_count"] == 0


class TestGetFileById:
    """Tests for GET /files/{file_id}."""

    def test_returns_existing_file(self, client: TestClient) -> None:
        create_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "test.parquet"},
        )
        file_id = create_resp.json()["file_id"]

        response = client.get(f"/files/{file_id}")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["file_id"] == file_id

    def test_returns_404_for_nonexistent(self, client: TestClient) -> None:
        response = client.get("/files/999999")
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "does not exist" in response.json()["detail"]


class TestGetFiles:
    """Tests for GET /files."""

    def test_returns_empty_list(self, client: TestClient) -> None:
        response = client.get("/files")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["files"] == []
        assert body["total"] == 0

    def test_returns_created_files(self, client: TestClient) -> None:
        client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "file1.parquet"},
        )
        client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "file2.parquet"},
        )

        response = client.get("/files")
        body = response.json()
        assert body["total"] == 2
        assert len(body["files"]) == 2

    def test_filter_by_status(self, client: TestClient) -> None:
        client.post(
            "/files",
            json={
                "bucket": "raw-data",
                "object_name": "a.parquet",
                "overall_status": "completed",
            },
        )
        client.post(
            "/files",
            json={
                "bucket": "raw-data",
                "object_name": "b.parquet",
                "overall_status": "pending",
            },
        )

        response = client.get("/files", params={"status": "completed"})
        body = response.json()
        assert body["total"] == 1
        assert body["files"][0]["overall_status"] == "completed"

    def test_filter_by_bucket(self, client: TestClient) -> None:
        client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "a.parquet"},
        )
        client.post(
            "/files",
            json={"bucket": "results", "object_name": "b.parquet"},
        )

        response = client.get("/files", params={"bucket": "results"})
        body = response.json()
        assert body["total"] == 1
        assert body["files"][0]["bucket"] == "results"

    def test_filter_by_object_name_pattern(self, client: TestClient) -> None:
        client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/test.parquet"},
        )
        client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "green/2022/01/test.parquet"},
        )

        response = client.get("/files", params={"object_name_pattern": "yellow%"})
        body = response.json()
        assert body["total"] == 1
        assert "yellow" in body["files"][0]["object_name"]

    def test_pagination_limit(self, client: TestClient) -> None:
        for i in range(3):
            client.post(
                "/files",
                json={"bucket": "raw-data", "object_name": f"file{i}.parquet"},
            )

        response = client.get("/files", params={"limit": 2})
        body = response.json()
        assert len(body["files"]) == 2
        assert body["total"] == 3
        assert body["limit"] == 2

    def test_pagination_offset(self, client: TestClient) -> None:
        for i in range(3):
            client.post(
                "/files",
                json={"bucket": "raw-data", "object_name": f"file{i}.parquet"},
            )

        response = client.get("/files", params={"limit": 2, "offset": 2})
        body = response.json()
        assert len(body["files"]) == 1
        assert body["offset"] == 2

    def test_response_includes_pagination_metadata(self, client: TestClient) -> None:
        response = client.get("/files", params={"limit": 50, "offset": 10})
        body = response.json()
        assert body["limit"] == 50
        assert body["offset"] == 10


class TestPatchFile:
    """Tests for PATCH /files/{file_id}."""

    def test_updates_status(self, client: TestClient) -> None:
        create_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "test.parquet"},
        )
        file_id = create_resp.json()["file_id"]

        response = client.patch(
            f"/files/{file_id}",
            json={"overall_status": "completed"},
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["overall_status"] == "completed"

    def test_updates_numeric_fields(self, client: TestClient) -> None:
        create_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "test.parquet"},
        )
        file_id = create_resp.json()["file_id"]

        response = client.patch(
            f"/files/{file_id}",
            json={
                "total_computation_seconds": 487.3,
                "total_elapsed_seconds": 512.8,
                "retry_count": 1,
            },
        )
        body = response.json()
        assert body["total_computation_seconds"] == 487.3
        assert body["total_elapsed_seconds"] == 512.8
        assert body["retry_count"] == 1

    def test_returns_404_for_nonexistent(self, client: TestClient) -> None:
        response = client.patch(
            "/files/999999",
            json={"overall_status": "completed"},
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_empty_update_returns_unchanged(self, client: TestClient) -> None:
        create_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "test.parquet"},
        )
        file_id = create_resp.json()["file_id"]

        response = client.patch(f"/files/{file_id}", json={})
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["overall_status"] == "pending"


# --- Job Execution helpers ---


def _create_file(client: TestClient) -> int:
    """Create a file and return its ID."""
    resp = client.post(
        "/files",
        json={"bucket": "raw-data", "object_name": "yellow/2022/01/test.parquet"},
    )
    return resp.json()["file_id"]


class TestPostJobExecution:
    """Tests for POST /job-executions."""

    def test_creates_job_execution_returns_201(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        response = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        assert response.status_code == status.HTTP_201_CREATED
        body = response.json()
        assert body["file_id"] == file_id
        assert body["pipeline_run_id"] == "run-001"
        assert body["step_name"] == "descriptive_statistics"
        assert body["status"] == "pending"
        assert body["retry_count"] == 0
        assert body["job_execution_id"] > 0

    def test_returns_404_for_nonexistent_file(self, client: TestClient) -> None:
        response = client.post(
            "/job-executions",
            json={
                "file_id": 999999,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "does not exist" in response.json()["detail"]

    def test_custom_status_and_retry(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        response = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "data_cleaning",
                "status": "running",
                "retry_count": 2,
            },
        )
        body = response.json()
        assert body["status"] == "running"
        assert body["retry_count"] == 2

    def test_nullable_fields_are_null(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        response = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "temporal_analysis",
            },
        )
        body = response.json()
        assert body["started_at"] is None
        assert body["completed_at"] is None
        assert body["computation_time_seconds"] is None
        assert body["error_message"] is None


class TestPostJobExecutionsBatch:
    """Tests for POST /job-executions/batch."""

    def test_creates_batch_returns_201(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        response = client.post(
            "/job-executions/batch",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "executions": [
                    {"step_name": "descriptive_statistics"},
                    {"step_name": "data_cleaning"},
                    {"step_name": "temporal_analysis"},
                ],
            },
        )
        assert response.status_code == status.HTTP_201_CREATED
        body = response.json()
        assert body["created_count"] == 3
        assert len(body["job_execution_ids"]) == 3

    def test_returns_404_for_nonexistent_file(self, client: TestClient) -> None:
        response = client.post(
            "/job-executions/batch",
            json={
                "file_id": 999999,
                "pipeline_run_id": "run-001",
                "executions": [{"step_name": "descriptive_statistics"}],
            },
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_rejects_empty_executions(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        response = client.post(
            "/job-executions/batch",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "executions": [],
            },
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestGetJobExecutionById:
    """Tests for GET /job-executions/{job_execution_id}."""

    def test_returns_existing(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        create_resp = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        job_id = create_resp.json()["job_execution_id"]

        response = client.get(f"/job-executions/{job_id}")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["job_execution_id"] == job_id

    def test_returns_404_for_nonexistent(self, client: TestClient) -> None:
        response = client.get("/job-executions/999999")
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "does not exist" in response.json()["detail"]


class TestGetJobExecutions:
    """Tests for GET /job-executions."""

    def test_returns_empty_list(self, client: TestClient) -> None:
        response = client.get("/job-executions")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["job_executions"] == []
        assert body["total"] == 0

    def test_filter_by_file_id(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )

        response = client.get("/job-executions", params={"file_id": file_id})
        body = response.json()
        assert body["total"] == 1
        assert body["job_executions"][0]["file_id"] == file_id

    def test_filter_by_step_name(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "data_cleaning",
            },
        )

        response = client.get(
            "/job-executions",
            params={"step_name": "data_cleaning"},
        )
        body = response.json()
        assert body["total"] == 1
        assert body["job_executions"][0]["step_name"] == "data_cleaning"

    def test_filter_by_status(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
                "status": "completed",
            },
        )
        client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "data_cleaning",
                "status": "pending",
            },
        )

        response = client.get("/job-executions", params={"status": "completed"})
        body = response.json()
        assert body["total"] == 1
        assert body["job_executions"][0]["status"] == "completed"

    def test_filter_by_pipeline_run_id(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-002",
                "step_name": "descriptive_statistics",
            },
        )

        response = client.get(
            "/job-executions",
            params={"pipeline_run_id": "run-002"},
        )
        body = response.json()
        assert body["total"] == 1
        assert body["job_executions"][0]["pipeline_run_id"] == "run-002"

    def test_pagination(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        for i in range(3):
            client.post(
                "/job-executions",
                json={
                    "file_id": file_id,
                    "pipeline_run_id": "run-001",
                    "step_name": f"step_{i}",
                },
            )

        response = client.get("/job-executions", params={"limit": 2})
        body = response.json()
        assert len(body["job_executions"]) == 2
        assert body["total"] == 3

        response2 = client.get(
            "/job-executions",
            params={"limit": 2, "offset": 2},
        )
        body2 = response2.json()
        assert len(body2["job_executions"]) == 1


class TestPatchJobExecution:
    """Tests for PATCH /job-executions/{job_execution_id}."""

    def test_updates_status(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        create_resp = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        job_id = create_resp.json()["job_execution_id"]

        response = client.patch(
            f"/job-executions/{job_id}",
            json={"status": "running", "started_at": "2026-03-02T22:00:00Z"},
        )
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "running"
        assert body["started_at"] is not None

    def test_updates_completion_fields(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        create_resp = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        job_id = create_resp.json()["job_execution_id"]

        response = client.patch(
            f"/job-executions/{job_id}",
            json={
                "status": "completed",
                "completed_at": "2026-03-02T22:01:03Z",
                "computation_time_seconds": 63.2,
            },
        )
        body = response.json()
        assert body["status"] == "completed"
        assert body["computation_time_seconds"] == 63.2

    def test_updates_error_message(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        create_resp = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        job_id = create_resp.json()["job_execution_id"]

        response = client.patch(
            f"/job-executions/{job_id}",
            json={"status": "failed", "error_message": "out of memory"},
        )
        body = response.json()
        assert body["status"] == "failed"
        assert body["error_message"] == "out of memory"

    def test_returns_404_for_nonexistent(self, client: TestClient) -> None:
        response = client.patch(
            "/job-executions/999999",
            json={"status": "completed"},
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_empty_update_returns_unchanged(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        create_resp = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
            },
        )
        job_id = create_resp.json()["job_execution_id"]

        response = client.patch(f"/job-executions/{job_id}", json={})
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["status"] == "pending"


# --- Analytical Results helpers ---


def _create_file_with_name(client: TestClient, object_name: str) -> int:
    """Create a file with a specific object_name and return its ID."""
    resp = client.post(
        "/files",
        json={"bucket": "raw-data", "object_name": object_name},
    )
    return resp.json()["file_id"]


def _create_job_execution(client: TestClient, file_id: int) -> int:
    """Create a job execution and return its ID."""
    resp = client.post(
        "/job-executions",
        json={
            "file_id": file_id,
            "pipeline_run_id": "run-001",
            "step_name": "descriptive_statistics",
        },
    )
    return resp.json()["job_execution_id"]


class TestPostAnalyticalResult:
    """Tests for POST /analytical-results."""

    def test_creates_result_returns_201(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        job_id = _create_job_execution(client=client, file_id=file_id)

        response = client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id,
                "result_type": "descriptive_statistics",
                "summary_data": {"avg_fare": 13.52},
                "computation_time_seconds": 63.2,
                "detail_s3_path": "results/yellow/2022/01/desc.parquet",
            },
        )
        assert response.status_code == status.HTTP_201_CREATED
        body = response.json()
        assert body["result_id"] > 0
        assert body["job_execution_id"] == job_id
        assert body["result_type"] == "descriptive_statistics"
        assert body["summary_data"] == {"avg_fare": 13.52}
        assert body["computation_time_seconds"] == 63.2
        assert body["detail_s3_path"] == "results/yellow/2022/01/desc.parquet"

    def test_creates_result_without_s3_path(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        job_id = _create_job_execution(client=client, file_id=file_id)

        response = client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id,
                "result_type": "descriptive_statistics",
                "summary_data": {"rows": 100},
                "computation_time_seconds": 10.0,
            },
        )
        assert response.status_code == status.HTTP_201_CREATED
        assert response.json()["detail_s3_path"] is None

    def test_returns_404_for_nonexistent_job(self, client: TestClient) -> None:
        response = client.post(
            "/analytical-results",
            json={
                "job_execution_id": 999999,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": 1.0,
            },
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "does not exist" in response.json()["detail"]

    def test_rejects_negative_computation_time(self, client: TestClient) -> None:
        response = client.post(
            "/analytical-results",
            json={
                "job_execution_id": 1,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": -1.0,
            },
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_rejects_empty_result_type(self, client: TestClient) -> None:
        response = client.post(
            "/analytical-results",
            json={
                "job_execution_id": 1,
                "result_type": "",
                "summary_data": {},
                "computation_time_seconds": 1.0,
            },
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestGetAnalyticalResultById:
    """Tests for GET /analytical-results/{result_id}."""

    def test_returns_existing(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        job_id = _create_job_execution(client=client, file_id=file_id)
        create_resp = client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id,
                "result_type": "descriptive_statistics",
                "summary_data": {"rows": 100},
                "computation_time_seconds": 10.0,
            },
        )
        result_id = create_resp.json()["result_id"]

        response = client.get(f"/analytical-results/{result_id}")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["result_id"] == result_id

    def test_returns_404_for_nonexistent(self, client: TestClient) -> None:
        response = client.get("/analytical-results/999999")
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "does not exist" in response.json()["detail"]


class TestGetAnalyticalResults:
    """Tests for GET /analytical-results."""

    def test_returns_empty_list(self, client: TestClient) -> None:
        response = client.get("/analytical-results")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["results"] == []
        assert body["total"] == 0

    def test_returns_results_with_file_info(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        job_id = _create_job_execution(client=client, file_id=file_id)
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id,
                "result_type": "descriptive_statistics",
                "summary_data": {"rows": 100},
                "computation_time_seconds": 10.0,
            },
        )

        response = client.get("/analytical-results")
        body = response.json()
        assert body["total"] == 1
        result = body["results"][0]
        assert result["file_info"] is not None
        assert result["file_info"]["file_id"] == file_id
        assert result["file_info"]["bucket"] == "raw-data"

    def test_filter_by_result_type(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        job_id = _create_job_execution(client=client, file_id=file_id)
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": 10.0,
            },
        )
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id,
                "result_type": "data_cleaning",
                "summary_data": {},
                "computation_time_seconds": 20.0,
            },
        )

        response = client.get(
            "/analytical-results",
            params={"result_type": "data_cleaning"},
        )
        body = response.json()
        assert body["total"] == 1
        assert body["results"][0]["result_type"] == "data_cleaning"

    def test_filter_by_file_id(self, client: TestClient) -> None:
        file_id_1 = _create_file_with_name(
            client=client,
            object_name="yellow/2022/01/a.parquet",
        )
        file_id_2 = _create_file_with_name(
            client=client,
            object_name="yellow/2022/02/b.parquet",
        )
        job_id_1 = _create_job_execution(client=client, file_id=file_id_1)
        job_id_2 = _create_job_execution(client=client, file_id=file_id_2)
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id_1,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": 10.0,
            },
        )
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id_2,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": 10.0,
            },
        )

        response = client.get(
            "/analytical-results",
            params={"file_id": file_id_1},
        )
        body = response.json()
        assert body["total"] == 1
        assert body["results"][0]["file_info"]["file_id"] == file_id_1

    def test_filter_by_taxi_type(self, client: TestClient) -> None:
        file_id_y = _create_file_with_name(
            client=client,
            object_name="yellow/2022/01/a.parquet",
        )
        file_id_g = _create_file_with_name(
            client=client,
            object_name="green/2022/01/b.parquet",
        )
        job_y = _create_job_execution(client=client, file_id=file_id_y)
        job_g = _create_job_execution(client=client, file_id=file_id_g)
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_y,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": 10.0,
            },
        )
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_g,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": 10.0,
            },
        )

        response = client.get(
            "/analytical-results",
            params={"taxi_type": "yellow"},
        )
        body = response.json()
        assert body["total"] == 1
        assert "yellow" in body["results"][0]["file_info"]["object_name"]

    def test_filter_by_year(self, client: TestClient) -> None:
        file_id_22 = _create_file_with_name(
            client=client,
            object_name="yellow/2022/01/a.parquet",
        )
        file_id_23 = _create_file_with_name(
            client=client,
            object_name="yellow/2023/01/b.parquet",
        )
        job_22 = _create_job_execution(client=client, file_id=file_id_22)
        job_23 = _create_job_execution(client=client, file_id=file_id_23)
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_22,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": 10.0,
            },
        )
        client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_23,
                "result_type": "descriptive_statistics",
                "summary_data": {},
                "computation_time_seconds": 10.0,
            },
        )

        response = client.get(
            "/analytical-results",
            params={"year": "2023"},
        )
        body = response.json()
        assert body["total"] == 1
        assert "2023" in body["results"][0]["file_info"]["object_name"]

    def test_pagination(self, client: TestClient) -> None:
        file_id = _create_file(client=client)
        job_id = _create_job_execution(client=client, file_id=file_id)
        for i in range(3):
            client.post(
                "/analytical-results",
                json={
                    "job_execution_id": job_id,
                    "result_type": f"step_{i}",
                    "summary_data": {},
                    "computation_time_seconds": float(i),
                },
            )

        response = client.get("/analytical-results", params={"limit": 2})
        body = response.json()
        assert len(body["results"]) == 2
        assert body["total"] == 3

        response2 = client.get(
            "/analytical-results",
            params={"limit": 2, "offset": 2},
        )
        body2 = response2.json()
        assert len(body2["results"]) == 1

    def test_response_includes_pagination_metadata(self, client: TestClient) -> None:
        response = client.get(
            "/analytical-results",
            params={"limit": 50, "offset": 10},
        )
        body = response.json()
        assert body["limit"] == 50
        assert body["offset"] == 10


# --- Metrics Routes ---


def _seed_file_with_jobs(client: TestClient, object_name: str, retry_count: int = 0):
    """Helper: create a file, job execution, and update for metrics tests.

    Returns:
        Tuple of (file_id, job_execution_id).
    """
    file_resp = client.post(
        "/files",
        json={"bucket": "raw-data", "object_name": object_name},
    )
    file_id = file_resp.json()["file_id"]
    client.patch(
        f"/files/{file_id}",
        json={
            "overall_status": "completed",
            "total_computation_seconds": 200.0,
            "retry_count": retry_count,
        },
    )
    job_resp = client.post(
        "/job-executions",
        json={
            "file_id": file_id,
            "pipeline_run_id": "run-001",
            "step_name": "descriptive_statistics",
            "status": "completed",
            "retry_count": 0,
        },
    )
    job_id = job_resp.json()["job_execution_id"]
    client.patch(
        f"/job-executions/{job_id}",
        json={"computation_time_seconds": 60.0},
    )
    return file_id, job_id


class TestGetCheckpointSavings:
    """Tests for GET /metrics/checkpoint-savings."""

    def test_aggregate_returns_200_empty(self, client: TestClient) -> None:
        response = client.get("/metrics/checkpoint-savings")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["files_with_retries"] == 0
        assert body["total_time_saved_seconds"] == 0.0

    def test_per_file_returns_404_for_missing_file(self, client: TestClient) -> None:
        response = client.get(
            "/metrics/checkpoint-savings",
            params={"file_id": 99999},
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_per_file_returns_savings(self, client: TestClient) -> None:
        file_id, _ = _seed_file_with_jobs(
            client=client,
            object_name="yellow/2022/01/f.parquet",
            retry_count=1,
        )
        response = client.get(
            "/metrics/checkpoint-savings",
            params={"file_id": file_id},
        )
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_id"] == file_id
        assert body["time_saved_seconds"] == 60.0
        assert body["actual_computation_seconds"] == 200.0
        assert body["percent_saved"] == 30.0

    def test_aggregate_includes_files_with_retries(self, client: TestClient) -> None:
        _seed_file_with_jobs(
            client=client,
            object_name="yellow/2022/01/f1.parquet",
            retry_count=1,
        )
        response = client.get("/metrics/checkpoint-savings")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["files_with_retries"] == 1
        assert body["total_time_saved_seconds"] == 60.0


class TestGetFailureStatistics:
    """Tests for GET /metrics/failure-statistics."""

    def test_returns_200_empty(self, client: TestClient) -> None:
        response = client.get("/metrics/failure-statistics")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["statistics"] == []

    def test_returns_failure_rates(self, client: TestClient) -> None:
        file_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/f.parquet"},
        )
        file_id = file_resp.json()["file_id"]
        client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "step_name": "descriptive_statistics",
                "status": "failed",
                "retry_count": 0,
            },
        )
        response = client.get("/metrics/failure-statistics")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert len(body["statistics"]) == 1
        assert body["statistics"][0]["step_name"] == "descriptive_statistics"
        assert body["statistics"][0]["files_that_failed"] == 1


class TestGetPipelineSummary:
    """Tests for GET /metrics/pipeline-summary."""

    def test_returns_200_empty(self, client: TestClient) -> None:
        response = client.get("/metrics/pipeline-summary")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["total_files"] == 0
        assert body["total_computation_hours"] == 0.0

    def test_returns_summary_with_data(self, client: TestClient) -> None:
        _seed_file_with_jobs(
            client=client,
            object_name="yellow/2022/01/f.parquet",
            retry_count=1,
        )
        response = client.get("/metrics/pipeline-summary")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["total_files"] == 1
        assert body["files_with_retries"] == 1
        assert body["retry_rate_percent"] == 100.0
        assert body["total_computation_hours"] == round(200.0 / 3600.0, 2)
        assert body["total_hours_saved_by_checkpointing"] == round(60.0 / 3600.0, 2)


class TestGetStepPerformance:
    """Tests for GET /metrics/step-performance."""

    def test_returns_200_empty(self, client: TestClient) -> None:
        response = client.get("/metrics/step-performance")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["statistics"] == []

    def test_returns_step_stats(self, client: TestClient) -> None:
        file_id, job_id = _seed_file_with_jobs(
            client=client,
            object_name="yellow/2022/01/f.parquet",
        )
        response = client.get("/metrics/step-performance")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert len(body["statistics"]) == 1
        stat = body["statistics"][0]
        assert stat["step_name"] == "descriptive_statistics"
        assert stat["executions"] == 1
        assert stat["avg_seconds"] == 60.0
        assert stat["min_seconds"] == 60.0
        assert stat["max_seconds"] == 60.0


class TestGetPipelineEfficiency:
    """Tests for GET /metrics/pipeline-efficiency."""

    def test_returns_200_empty(self, client: TestClient) -> None:
        response = client.get("/metrics/pipeline-efficiency")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["statistics"] == []

    def test_returns_efficiency_stats(self, client: TestClient) -> None:
        file_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/f.parquet"},
        )
        file_id = file_resp.json()["file_id"]
        client.patch(
            f"/files/{file_id}",
            json={
                "overall_status": "completed",
                "total_computation_seconds": 120.0,
                "total_elapsed_seconds": 180.0,
            },
        )
        response = client.get("/metrics/pipeline-efficiency")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert len(body["statistics"]) == 1
        stat = body["statistics"][0]
        assert stat["overall_status"] == "completed"
        assert stat["file_count"] == 1
        assert stat["avg_efficiency_ratio"] == round(120.0 / 180.0, 4)
        assert stat["avg_computation_minutes"] == round(120.0 / 60.0, 2)
        assert stat["avg_elapsed_minutes"] == round(180.0 / 60.0, 2)

    def test_groups_by_status(self, client: TestClient) -> None:
        for obj_name, obj_status, comp, elapsed in [
            ("yellow/2022/01/a.parquet", "completed", 100.0, 200.0),
            ("yellow/2022/01/b.parquet", "failed", 50.0, 150.0),
        ]:
            file_resp = client.post(
                "/files",
                json={"bucket": "raw-data", "object_name": obj_name},
            )
            file_id = file_resp.json()["file_id"]
            client.patch(
                f"/files/{file_id}",
                json={
                    "overall_status": obj_status,
                    "total_computation_seconds": comp,
                    "total_elapsed_seconds": elapsed,
                },
            )
        response = client.get("/metrics/pipeline-efficiency")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert len(body["statistics"]) == 2
        statuses = {s["overall_status"] for s in body["statistics"]}
        assert statuses == {"completed", "failed"}

    def test_excludes_zero_elapsed(self, client: TestClient) -> None:
        file_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/f.parquet"},
        )
        file_id = file_resp.json()["file_id"]
        client.patch(
            f"/files/{file_id}",
            json={
                "overall_status": "completed",
                "total_computation_seconds": 100.0,
                "total_elapsed_seconds": 0.0,
            },
        )
        response = client.get("/metrics/pipeline-efficiency")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["statistics"] == []


class TestGetRecoveryTime:
    """Tests for GET /metrics/recovery-time."""

    def test_returns_zeros_when_no_retries(self, client: TestClient) -> None:
        response = client.get("/metrics/recovery-time")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["avg_recovery_with_checkpoint_seconds"] == 0.0
        assert body["avg_recovery_without_checkpoint_seconds"] == 0.0
        assert body["avg_time_saved_seconds"] == 0.0
        assert body["percent_improvement"] == 0.0

    def test_single_file_with_retries(self, client: TestClient) -> None:
        file_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/a.parquet"},
        )
        file_id = file_resp.json()["file_id"]
        client.patch(
            f"/files/{file_id}",
            json={
                "overall_status": "completed",
                "retry_count": 1,
                "total_computation_seconds": 500.0,
            },
        )
        # First attempt step (retry_count=0) — not counted as "with checkpoint"
        job1 = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-1",
                "step_name": "descriptive_statistics",
                "status": "completed",
                "retry_count": 0,
            },
        )
        job1_id = job1.json()["job_execution_id"]
        client.patch(
            f"/job-executions/{job1_id}",
            json={"computation_time_seconds": 400.0},
        )
        # Retry step (retry_count=1) — counted as "with checkpoint"
        job2 = client.post(
            "/job-executions",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-2",
                "step_name": "data_cleaning",
                "status": "completed",
                "retry_count": 1,
            },
        )
        job2_id = job2.json()["job_execution_id"]
        client.patch(
            f"/job-executions/{job2_id}",
            json={"computation_time_seconds": 100.0},
        )
        response = client.get("/metrics/recovery-time")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["avg_recovery_with_checkpoint_seconds"] == 100.0
        assert body["avg_recovery_without_checkpoint_seconds"] == 500.0
        assert body["avg_time_saved_seconds"] == 400.0
        assert body["percent_improvement"] == 80.0

    def test_excludes_non_completed_files(self, client: TestClient) -> None:
        file_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/b.parquet"},
        )
        file_id = file_resp.json()["file_id"]
        client.patch(
            f"/files/{file_id}",
            json={
                "overall_status": "failed",
                "retry_count": 2,
                "total_computation_seconds": 300.0,
            },
        )
        response = client.get("/metrics/recovery-time")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["avg_recovery_with_checkpoint_seconds"] == 0.0
        assert body["percent_improvement"] == 0.0

    def test_averages_across_multiple_files(self, client: TestClient) -> None:
        for obj_name, comp, retry_with in [
            ("yellow/2022/01/c.parquet", 600.0, 200.0),
            ("yellow/2022/01/d.parquet", 400.0, 100.0),
        ]:
            file_resp = client.post(
                "/files",
                json={"bucket": "raw-data", "object_name": obj_name},
            )
            file_id = file_resp.json()["file_id"]
            client.patch(
                f"/files/{file_id}",
                json={
                    "overall_status": "completed",
                    "retry_count": 1,
                    "total_computation_seconds": comp,
                },
            )
            job_resp = client.post(
                "/job-executions",
                json={
                    "file_id": file_id,
                    "pipeline_run_id": f"run-{obj_name}",
                    "step_name": "data_cleaning",
                    "status": "completed",
                    "retry_count": 1,
                },
            )
            job_id = job_resp.json()["job_execution_id"]
            client.patch(
                f"/job-executions/{job_id}",
                json={"computation_time_seconds": retry_with},
            )
        response = client.get("/metrics/recovery-time")
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        # avg_with = (200 + 100) / 2 = 150
        assert body["avg_recovery_with_checkpoint_seconds"] == 150.0
        # avg_without = (600 + 400) / 2 = 500
        assert body["avg_recovery_without_checkpoint_seconds"] == 500.0
        # avg_saved = 500 - 150 = 350
        assert body["avg_time_saved_seconds"] == 350.0
        assert body["percent_improvement"] == 70.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
