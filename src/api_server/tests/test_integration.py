"""End-to-end integration tests for the API server full workflow."""

from datetime import datetime, timezone

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.server.main import app
from src.services.config import Settings
from src.services.database import get_session, init_schema, reset_globals

STEPS = [
    "descriptive_statistics",
    "data_cleaning",
    "temporal_analysis",
    "geospatial_analysis",
    "fare_revenue_analysis",
]


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


class TestFullPipelineWorkflow:
    """Simulate a complete pipeline run: file → jobs → updates → results → metrics."""

    def test_successful_pipeline_run(self, client: TestClient) -> None:
        """Full pipeline: create file, batch jobs, complete all, store results, check metrics."""
        # 1. Create file
        file_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/data.parquet"},
        )
        assert file_resp.status_code == status.HTTP_201_CREATED
        file_id = file_resp.json()["file_id"]

        # 2. Batch create job executions for all 5 steps
        batch_resp = client.post(
            "/job-executions/batch",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-001",
                "executions": [{"step_name": s} for s in STEPS],
            },
        )
        assert batch_resp.status_code == status.HTTP_201_CREATED
        job_ids = batch_resp.json()["job_execution_ids"]
        assert len(job_ids) == 5

        # 3. Start and complete each job, create analytical results
        now = datetime.now(tz=timezone.utc).isoformat()
        total_computation = 0.0
        for i, job_id in enumerate(job_ids):
            comp_time = 30.0 + i * 15.0  # 30, 45, 60, 75, 90

            # Start
            client.patch(
                f"/job-executions/{job_id}",
                json={"status": "running", "started_at": now},
            )

            # Complete
            patch_resp = client.patch(
                f"/job-executions/{job_id}",
                json={
                    "status": "completed",
                    "completed_at": now,
                    "computation_time_seconds": comp_time,
                },
            )
            assert patch_resp.status_code == status.HTTP_200_OK
            assert patch_resp.json()["status"] == "completed"
            total_computation += comp_time

            # Create analytical result
            ar_resp = client.post(
                "/analytical-results",
                json={
                    "job_execution_id": job_id,
                    "result_type": STEPS[i],
                    "summary_data": {"mean": 42.0 + i, "count": 1000 * (i + 1)},
                    "computation_time_seconds": comp_time,
                    "detail_s3_path": f"s3://results/{STEPS[i]}/output.parquet",
                },
            )
            assert ar_resp.status_code == status.HTTP_201_CREATED

        # 4. Update file as completed
        client.patch(
            f"/files/{file_id}",
            json={
                "overall_status": "completed",
                "total_computation_seconds": total_computation,
                "total_elapsed_seconds": total_computation + 10.0,
            },
        )

        # 5. Verify file state
        file_get = client.get(f"/files/{file_id}")
        assert file_get.json()["overall_status"] == "completed"
        assert file_get.json()["total_computation_seconds"] == total_computation

        # 6. Verify all jobs are completed
        jobs_resp = client.get(
            "/job-executions",
            params={"file_id": file_id, "status": "completed"},
        )
        assert jobs_resp.json()["total"] == 5

        # 7. Verify analytical results
        ar_list = client.get(
            "/analytical-results",
            params={"file_id": file_id},
        )
        assert ar_list.json()["total"] == 5

        # 8. Pipeline summary should show 1 completed file, 0 retries
        summary = client.get("/metrics/pipeline-summary")
        assert summary.status_code == status.HTTP_200_OK
        body = summary.json()
        assert body["total_files"] == 1
        assert body["files_with_retries"] == 0

    def test_pipeline_with_failure_and_retry(self, client: TestClient) -> None:
        """Pipeline where step 3 fails, pipeline retries from checkpoint."""
        # 1. Create file
        file_resp = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "green/2023/06/data.parquet"},
        )
        file_id = file_resp.json()["file_id"]

        # 2. First run — complete steps 1-2, fail step 3
        batch1 = client.post(
            "/job-executions/batch",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-fail-001",
                "executions": [{"step_name": s} for s in STEPS[:3]],
            },
        )
        job_ids_1 = batch1.json()["job_execution_ids"]

        now = datetime.now(tz=timezone.utc).isoformat()

        # Complete steps 1 and 2
        for i in range(2):
            comp_time = 30.0 + i * 15.0
            client.patch(
                f"/job-executions/{job_ids_1[i]}",
                json={
                    "status": "completed",
                    "started_at": now,
                    "completed_at": now,
                    "computation_time_seconds": comp_time,
                },
            )
            client.post(
                "/analytical-results",
                json={
                    "job_execution_id": job_ids_1[i],
                    "result_type": STEPS[i],
                    "summary_data": {"status": "ok"},
                    "computation_time_seconds": comp_time,
                },
            )

        # Fail step 3
        client.patch(
            f"/job-executions/{job_ids_1[2]}",
            json={
                "status": "failed",
                "started_at": now,
                "error_message": "out of memory",
            },
        )

        # 3. Retry run — resume from step 3 (steps 1-2 checkpointed)
        batch2 = client.post(
            "/job-executions/batch",
            json={
                "file_id": file_id,
                "pipeline_run_id": "run-fail-002",
                "executions": [{"step_name": s, "retry_count": 1} for s in STEPS[2:]],
            },
        )
        job_ids_2 = batch2.json()["job_execution_ids"]

        # Complete steps 3-5 on retry
        for i, job_id in enumerate(job_ids_2):
            step_idx = i + 2
            comp_time = 60.0 + i * 15.0
            client.patch(
                f"/job-executions/{job_id}",
                json={
                    "status": "completed",
                    "started_at": now,
                    "completed_at": now,
                    "computation_time_seconds": comp_time,
                },
            )
            client.post(
                "/analytical-results",
                json={
                    "job_execution_id": job_id,
                    "result_type": STEPS[step_idx],
                    "summary_data": {"status": "ok"},
                    "computation_time_seconds": comp_time,
                },
            )

        # 4. Mark file completed with retry
        # total = steps 1-2 first run (30+45) + steps 3-5 retry (60+75+90) = 300
        client.patch(
            f"/files/{file_id}",
            json={
                "overall_status": "completed",
                "total_computation_seconds": 300.0,
                "total_elapsed_seconds": 350.0,
                "retry_count": 1,
            },
        )

        # 5. Verify checkpoint savings for this file
        savings = client.get(
            "/metrics/checkpoint-savings",
            params={"file_id": file_id},
        )
        assert savings.status_code == status.HTTP_200_OK
        body = savings.json()
        assert body["file_id"] == file_id
        # Time saved = completed first-attempt steps (retry_count=0): 30 + 45 = 75
        assert body["time_saved_seconds"] == 75.0
        assert body["actual_computation_seconds"] == 300.0
        assert body["percent_saved"] == 25.0
        assert body["retry_count"] == 1

        # 6. Verify failure statistics
        fail_stats = client.get("/metrics/failure-statistics")
        stats = fail_stats.json()["statistics"]
        temporal_stat = next(
            (s for s in stats if s["step_name"] == "temporal_analysis"), None
        )
        assert temporal_stat is not None
        assert temporal_stat["files_that_failed"] == 1

        # 7. Aggregate checkpoint savings
        agg = client.get("/metrics/checkpoint-savings")
        assert agg.status_code == status.HTTP_200_OK
        agg_body = agg.json()
        assert agg_body["files_with_retries"] == 1
        assert agg_body["total_time_saved_seconds"] == 75.0


class TestComplexFiltering:
    """Test filtering across the full data model."""

    def _seed_two_files(self, client: TestClient) -> tuple[int, int]:
        """Create two files with different taxi types and analytical results."""
        now = datetime.now(tz=timezone.utc).isoformat()

        f1 = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "yellow/2022/01/a.parquet"},
        ).json()["file_id"]
        f2 = client.post(
            "/files",
            json={"bucket": "raw-data", "object_name": "green/2023/06/b.parquet"},
        ).json()["file_id"]

        for fid in (f1, f2):
            job = client.post(
                "/job-executions",
                json={
                    "file_id": fid,
                    "pipeline_run_id": f"run-{fid}",
                    "step_name": "descriptive_statistics",
                },
            ).json()["job_execution_id"]

            client.patch(
                f"/job-executions/{job}",
                json={
                    "status": "completed",
                    "started_at": now,
                    "completed_at": now,
                    "computation_time_seconds": 30.0,
                },
            )

            client.post(
                "/analytical-results",
                json={
                    "job_execution_id": job,
                    "result_type": "descriptive_statistics",
                    "summary_data": {"file_id": fid},
                    "computation_time_seconds": 30.0,
                },
            )

        return f1, f2

    def test_filter_analytical_results_by_taxi_type(self, client: TestClient) -> None:
        f1, f2 = self._seed_two_files(client=client)

        yellow = client.get("/analytical-results", params={"taxi_type": "yellow"})
        assert yellow.json()["total"] == 1
        assert yellow.json()["results"][0]["file_info"]["file_id"] == f1

        green = client.get("/analytical-results", params={"taxi_type": "green"})
        assert green.json()["total"] == 1
        assert green.json()["results"][0]["file_info"]["file_id"] == f2

    def test_filter_analytical_results_by_year(self, client: TestClient) -> None:
        f1, f2 = self._seed_two_files(client=client)

        resp_2022 = client.get("/analytical-results", params={"year": "2022"})
        assert resp_2022.json()["total"] == 1
        assert resp_2022.json()["results"][0]["file_info"]["file_id"] == f1

        resp_2023 = client.get("/analytical-results", params={"year": "2023"})
        assert resp_2023.json()["total"] == 1
        assert resp_2023.json()["results"][0]["file_info"]["file_id"] == f2

    def test_filter_job_executions_by_pipeline_run(self, client: TestClient) -> None:
        f1, f2 = self._seed_two_files(client=client)

        resp = client.get("/job-executions", params={"pipeline_run_id": f"run-{f1}"})
        assert resp.json()["total"] == 1
        assert resp.json()["job_executions"][0]["file_id"] == f1

    def test_filter_files_by_status_and_bucket(self, client: TestClient) -> None:
        client.post(
            "/files",
            json={
                "bucket": "raw-data",
                "object_name": "yellow/2022/01/x.parquet",
                "overall_status": "completed",
            },
        )
        client.post(
            "/files",
            json={
                "bucket": "other-bucket",
                "object_name": "green/2023/01/y.parquet",
                "overall_status": "pending",
            },
        )

        resp = client.get(
            "/files", params={"status": "completed", "bucket": "raw-data"}
        )
        assert resp.json()["total"] == 1
        assert resp.json()["files"][0]["overall_status"] == "completed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
