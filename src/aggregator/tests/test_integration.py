"""End-to-end integration tests for the aggregator service.

Requires API Server + Postgres running via docker compose.
Seeds known analytical results into the API Server, then calls
each aggregation endpoint and verifies correct aggregation.
"""

import uuid

import httpx
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.server.main import app
from src.services.config import SETTINGS

# Unique suffix per test run to avoid collisions with stale DB data
_RUN_ID = uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def api_client() -> httpx.Client:
    """HTTP client pointing at the real API Server."""
    with httpx.Client(
        base_url=SETTINGS.API_SERVER_URL,
        timeout=30.0,
    ) as client:
        yield client


@pytest.fixture(scope="module")
def seeded_data(api_client: httpx.Client) -> dict:
    """Seed the API Server with known test data and return IDs.

    Creates two files (green taxi — unique per run), each with
    descriptive_statistics, data_cleaning, and temporal_analysis results.
    Uses 'green' taxi type to avoid collisions with other test data.
    """
    taxi_type = "green"
    obj1 = f"{taxi_type}/{_RUN_ID}/2023/01/trips.parquet"
    obj2 = f"{taxi_type}/{_RUN_ID}/2023/02/trips.parquet"

    # Create file 1
    resp = api_client.post(
        "/files",
        json={"bucket": "raw-data", "object_name": obj1},
    )
    assert resp.status_code == status.HTTP_201_CREATED
    file1_id = resp.json()["file_id"]

    # Create file 2
    resp = api_client.post(
        "/files",
        json={"bucket": "raw-data", "object_name": obj2},
    )
    assert resp.status_code == status.HTTP_201_CREATED
    file2_id = resp.json()["file_id"]

    # Create job executions for file 1
    resp = api_client.post(
        "/job-executions/batch",
        json={
            "file_id": file1_id,
            "pipeline_run_id": f"integ-{_RUN_ID}-001",
            "executions": [
                {"step_name": "descriptive_statistics"},
                {"step_name": "data_cleaning"},
                {"step_name": "temporal_analysis"},
            ],
        },
    )
    assert resp.status_code == status.HTTP_201_CREATED
    job_ids_f1 = resp.json()["job_execution_ids"]

    # Create job executions for file 2
    resp = api_client.post(
        "/job-executions/batch",
        json={
            "file_id": file2_id,
            "pipeline_run_id": f"integ-{_RUN_ID}-002",
            "executions": [
                {"step_name": "descriptive_statistics"},
                {"step_name": "data_cleaning"},
                {"step_name": "temporal_analysis"},
            ],
        },
    )
    assert resp.status_code == status.HTTP_201_CREATED
    job_ids_f2 = resp.json()["job_execution_ids"]

    # Mark all jobs as completed
    for jid in job_ids_f1 + job_ids_f2:
        resp = api_client.patch(
            f"/job-executions/{jid}",
            json={"status": "completed", "computation_time_seconds": 45.0},
        )
        assert resp.status_code == status.HTTP_200_OK

    # Analytical results for file 1
    descriptive_summary_1 = {
        "num_rows": 1000,
        "num_numeric_columns": 5,
        "percentiles": {
            "fare_amount": {"p1": 2.0, "p50": 12.0, "p99": 60.0},
        },
        "distribution": {
            "fare_amount": {
                "mean": 13.0,
                "std": 10.0,
                "skewness": 2.0,
                "kurtosis": 7.0,
            },
            "trip_distance": {
                "mean": 3.0,
                "std": 4.0,
                "skewness": 3.0,
                "kurtosis": 14.0,
            },
            "tip_amount": {"mean": 2.0, "std": 1.5, "skewness": 1.0, "kurtosis": 3.0},
        },
    }
    cleaning_summary_1 = {
        "num_rows": 1000,
        "outlier_counts": {
            "fare_amount": {"iqr": 100, "zscore": 80, "isolation_forest": 120},
        },
        "quality_violations": {"negative_fares": 10, "zero_distances": 20},
        "strategy_comparison": {
            "removal": {"rows_before": 1000, "rows_after": 950, "rows_removed": 50},
        },
    }
    temporal_summary_1 = {
        "num_rows": 1000,
        "num_hours": 744,
        "peak_hours": [8, 9, 17, 18, 19],
    }

    # Analytical results for file 2
    descriptive_summary_2 = {
        "num_rows": 2000,
        "num_numeric_columns": 5,
        "percentiles": {
            "fare_amount": {"p1": 3.0, "p50": 14.0, "p99": 70.0},
        },
        "distribution": {
            "fare_amount": {
                "mean": 15.0,
                "std": 12.0,
                "skewness": 2.5,
                "kurtosis": 8.0,
            },
            "trip_distance": {
                "mean": 4.0,
                "std": 5.0,
                "skewness": 3.5,
                "kurtosis": 16.0,
            },
            "tip_amount": {"mean": 3.0, "std": 2.0, "skewness": 1.5, "kurtosis": 4.0},
        },
    }
    cleaning_summary_2 = {
        "num_rows": 2000,
        "outlier_counts": {
            "fare_amount": {"iqr": 200, "zscore": 160, "isolation_forest": 240},
        },
        "quality_violations": {
            "negative_fares": 5,
            "zero_distances": 30,
            "impossible_durations": 3,
        },
        "strategy_comparison": {
            "removal": {"rows_before": 2000, "rows_after": 1880, "rows_removed": 120},
        },
    }
    temporal_summary_2 = {
        "num_rows": 2000,
        "num_hours": 672,
        "peak_hours": [9, 17, 18, 19, 20],
    }

    # Post analytical results
    results_to_create = [
        (job_ids_f1[0], "descriptive_statistics", descriptive_summary_1, 30.0),
        (job_ids_f1[1], "data_cleaning", cleaning_summary_1, 50.0),
        (job_ids_f1[2], "temporal_analysis", temporal_summary_1, 60.0),
        (job_ids_f2[0], "descriptive_statistics", descriptive_summary_2, 40.0),
        (job_ids_f2[1], "data_cleaning", cleaning_summary_2, 70.0),
        (job_ids_f2[2], "temporal_analysis", temporal_summary_2, 80.0),
    ]

    result_ids = []
    for job_id, result_type, summary, comp_time in results_to_create:
        resp = api_client.post(
            "/analytical-results",
            json={
                "job_execution_id": job_id,
                "result_type": result_type,
                "summary_data": summary,
                "computation_time_seconds": comp_time,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        result_ids.append(resp.json()["result_id"])

    return {
        "file1_id": file1_id,
        "file2_id": file2_id,
        "taxi_type": taxi_type,
        "result_ids": result_ids,
    }


@pytest.fixture()
def client() -> TestClient:
    """Aggregator TestClient."""
    return TestClient(app=app)


class TestIntegrationDescriptiveStats:
    """End-to-end test for descriptive stats aggregation."""

    def test_aggregates_across_files(self, client, seeded_data):
        """Descriptive stats endpoint aggregates both seeded files."""
        response = client.get(
            "/aggregations/descriptive-stats",
            params={"taxi_type": seeded_data["taxi_type"]},
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] >= 2
        assert body["total_rows"] >= 3000
        assert "fare_amount" in body["aggregated_stats"]


class TestIntegrationTaxiComparison:
    """End-to-end test for taxi comparison aggregation."""

    def test_taxi_type_present(self, client, seeded_data):
        """Taxi comparison includes seeded type with correct metrics."""
        response = client.get("/aggregations/taxi-comparison")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        taxi = seeded_data["taxi_type"]
        assert taxi in body["comparison"]
        assert body["comparison"][taxi]["file_count"] >= 2
        assert body["comparison"][taxi]["total_rows"] >= 3000


class TestIntegrationTemporalPatterns:
    """End-to-end test for temporal patterns aggregation."""

    def test_peak_hours_aggregated(self, client, seeded_data):
        """Peak hours appearing in both files are included."""
        response = client.get(
            "/aggregations/temporal-patterns",
            params={"taxi_type": seeded_data["taxi_type"]},
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] >= 2
        # Hours 9, 17, 18, 19 appear in both files
        for hour in (9, 17, 18, 19):
            assert hour in body["peak_hours"]


class TestIntegrationDataQuality:
    """End-to-end test for data quality aggregation."""

    def test_outliers_summed_across_files(self, client, seeded_data):
        """Outlier counts are summed across files."""
        response = client.get(
            "/aggregations/data-quality",
            params={"taxi_type": seeded_data["taxi_type"]},
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] >= 2
        assert body["total_rows_processed"] >= 3000
        # IQR: 100 + 200 = 300 (at minimum)
        assert body["outlier_summary"]["iqr"]["total_outliers"] >= 300

    def test_quality_violations_present(self, client, seeded_data):
        """Quality violations are present in response."""
        response = client.get(
            "/aggregations/data-quality",
            params={"taxi_type": seeded_data["taxi_type"]},
        )

        body = response.json()
        assert "negative_fares" in body["quality_violations"]
        assert "zero_distances" in body["quality_violations"]


class TestIntegrationPipelinePerformance:
    """End-to-end test for pipeline performance aggregation."""

    def test_steps_grouped_correctly(self, client, seeded_data):
        """Results are grouped by analytical step."""
        response = client.get(
            "/aggregations/pipeline-performance",
            params={"taxi_type": seeded_data["taxi_type"]},
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] >= 2
        assert "descriptive_statistics" in body["steps"]
        assert "data_cleaning" in body["steps"]
        assert "temporal_analysis" in body["steps"]

        desc = body["steps"]["descriptive_statistics"]
        assert desc["files_processed"] >= 2
        assert desc["min_computation_seconds"] <= desc["max_computation_seconds"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
