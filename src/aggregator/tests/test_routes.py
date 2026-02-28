"""Tests for aggregator API routes."""

import httpx
from fastapi import status


class TestHealthRoute:
    """Tests for GET /health."""

    def test_health_returns_ok(self, client):
        """Health endpoint returns 200 with status ok."""
        response = client.get("/health")

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"status": "ok"}


class TestDescriptiveStatsRoute:
    """Tests for GET /aggregations/descriptive-stats."""

    def test_success_with_results(self, client, mock_fetch_results):
        """Returns aggregated stats when API Server returns results."""
        mock_fetch_results.return_value = [
            {
                "summary_data": {
                    "percentiles": {
                        "fare_amount": {"p1": 2.5, "p50": 12.0, "p99": 65.0},
                    },
                    "distribution": {
                        "fare_amount": {"mean": 13.5},
                    },
                    "num_rows": 1000,
                }
            },
        ]

        response = client.get("/aggregations/descriptive-stats")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] == 1
        assert body["total_rows"] == 1000
        assert "fare_amount" in body["aggregated_stats"]
        mock_fetch_results.assert_called_once_with(
            result_type="descriptive_statistics",
            taxi_type=None,
            year=None,
            month=None,
        )

    def test_success_with_filters(self, client, mock_fetch_results):
        """Passes query params through to fetch and includes them in response."""
        mock_fetch_results.return_value = []

        response = client.get(
            "/aggregations/descriptive-stats",
            params={"taxi_type": "yellow", "start_year": "2023", "start_month": "06"},
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["filters_applied"]["taxi_type"] == "yellow"
        assert body["filters_applied"]["start_year"] == "2023"
        assert body["filters_applied"]["start_month"] == "06"
        mock_fetch_results.assert_called_once_with(
            result_type="descriptive_statistics",
            taxi_type="yellow",
            year="2023",
            month="06",
        )

    def test_empty_results(self, client, mock_fetch_results):
        """Returns zero counts when no results match."""
        mock_fetch_results.return_value = []

        response = client.get("/aggregations/descriptive-stats")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] == 0
        assert body["total_rows"] == 0
        assert body["aggregated_stats"] == {}

    def test_api_server_http_error_returns_502(self, client, mock_fetch_results):
        """Returns 502 when API Server responds with an HTTP error."""
        mock_response = httpx.Response(
            status_code=500,
            request=httpx.Request(method="GET", url="http://fake/analytical-results"),
        )
        mock_fetch_results.side_effect = httpx.HTTPStatusError(
            message="Internal Server Error",
            request=mock_response.request,
            response=mock_response,
        )

        response = client.get("/aggregations/descriptive-stats")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "500" in body["detail"]

    def test_api_server_unreachable_returns_502(self, client, mock_fetch_results):
        """Returns 502 when API Server is unreachable."""
        mock_fetch_results.side_effect = httpx.ConnectError(
            message="Connection refused"
        )

        response = client.get("/aggregations/descriptive-stats")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "unreachable" in body["detail"].lower()


class TestTaxiComparisonRoute:
    """Tests for GET /aggregations/taxi-comparison."""

    def test_success_with_results(self, client, mock_fetch_results):
        """Returns comparison when API Server returns results for each type."""

        def side_effect(result_type, taxi_type, year, month):
            if taxi_type == "yellow":
                return [
                    {
                        "summary_data": {
                            "num_rows": 1000,
                            "distribution": {
                                "fare_amount": {"mean": 13.5},
                                "trip_distance": {"mean": 3.2},
                                "tip_amount": {"mean": 2.5},
                            },
                        }
                    }
                ]
            return []

        mock_fetch_results.side_effect = side_effect

        response = client.get("/aggregations/taxi-comparison")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["comparison"]["yellow"]["file_count"] == 1
        assert body["comparison"]["yellow"]["total_rows"] == 1000
        assert body["comparison"]["yellow"]["avg_fare"] == 13.5
        assert body["comparison"]["green"]["file_count"] == 0
        assert body["comparison"]["fhv"]["file_count"] == 0
        assert body["comparison"]["fhvhv"]["file_count"] == 0

    def test_success_with_filters(self, client, mock_fetch_results):
        """Passes query params through and includes them in response."""
        mock_fetch_results.return_value = []

        response = client.get(
            "/aggregations/taxi-comparison",
            params={"start_year": "2023", "start_month": "06"},
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["filters_applied"]["start_year"] == "2023"
        assert body["filters_applied"]["start_month"] == "06"
        # Should be called once per taxi type
        assert mock_fetch_results.call_count == 4

    def test_empty_results_all_types(self, client, mock_fetch_results):
        """Returns zero counts when no results match for any type."""
        mock_fetch_results.return_value = []

        response = client.get("/aggregations/taxi-comparison")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        for taxi_type in ["yellow", "green", "fhv", "fhvhv"]:
            assert body["comparison"][taxi_type]["file_count"] == 0
            assert body["comparison"][taxi_type]["total_rows"] == 0

    def test_fhv_null_fare_fields(self, client, mock_fetch_results):
        """FHV type returns null fare/tip fields when no fare data exists."""

        def side_effect(result_type, taxi_type, year, month):
            if taxi_type == "fhv":
                return [
                    {
                        "summary_data": {
                            "num_rows": 500,
                            "distribution": {
                                "pulocationid": {"mean": 130.0},
                            },
                        }
                    }
                ]
            return []

        mock_fetch_results.side_effect = side_effect

        response = client.get("/aggregations/taxi-comparison")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        fhv = body["comparison"]["fhv"]
        assert fhv["file_count"] == 1
        assert fhv["total_rows"] == 500
        assert fhv["avg_fare"] is None
        assert fhv["avg_tip_percentage"] is None

    def test_api_server_http_error_returns_502(self, client, mock_fetch_results):
        """Returns 502 when API Server responds with an HTTP error."""
        mock_response = httpx.Response(
            status_code=500,
            request=httpx.Request(method="GET", url="http://fake/analytical-results"),
        )
        mock_fetch_results.side_effect = httpx.HTTPStatusError(
            message="Internal Server Error",
            request=mock_response.request,
            response=mock_response,
        )

        response = client.get("/aggregations/taxi-comparison")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "500" in body["detail"]

    def test_api_server_unreachable_returns_502(self, client, mock_fetch_results):
        """Returns 502 when API Server is unreachable."""
        mock_fetch_results.side_effect = httpx.ConnectError(
            message="Connection refused"
        )

        response = client.get("/aggregations/taxi-comparison")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "unreachable" in body["detail"].lower()


class TestTemporalPatternsRoute:
    """Tests for GET /aggregations/temporal-patterns."""

    def test_success_with_results(self, client, mock_fetch_results):
        """Returns aggregated temporal patterns when API Server returns results."""
        mock_fetch_results.return_value = [
            {
                "summary_data": {
                    "num_rows": 1000000,
                    "num_hours": 744,
                    "peak_hours": [8, 9, 17, 18, 19],
                }
            },
            {
                "summary_data": {
                    "num_rows": 900000,
                    "num_hours": 720,
                    "peak_hours": [9, 17, 18, 19, 20],
                }
            },
        ]

        response = client.get("/aggregations/temporal-patterns")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] == 2
        # Peak hours appearing in more than half (>1) of files: 9, 17, 18, 19
        assert 17 in body["peak_hours"]
        assert 18 in body["peak_hours"]
        assert 19 in body["peak_hours"]
        assert 9 in body["peak_hours"]
        mock_fetch_results.assert_called_once_with(
            result_type="temporal_analysis",
            taxi_type=None,
            year=None,
            month=None,
        )

    def test_success_with_filters(self, client, mock_fetch_results):
        """Passes query params through to fetch and includes them in response."""
        mock_fetch_results.return_value = []

        response = client.get(
            "/aggregations/temporal-patterns",
            params={"taxi_type": "yellow", "start_year": "2023", "start_month": "06"},
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["filters_applied"]["taxi_type"] == "yellow"
        assert body["filters_applied"]["start_year"] == "2023"
        assert body["filters_applied"]["start_month"] == "06"
        mock_fetch_results.assert_called_once_with(
            result_type="temporal_analysis",
            taxi_type="yellow",
            year="2023",
            month="06",
        )

    def test_empty_results(self, client, mock_fetch_results):
        """Returns zero counts and empty collections when no results match."""
        mock_fetch_results.return_value = []

        response = client.get("/aggregations/temporal-patterns")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] == 0
        assert body["peak_hours"] == []
        assert body["hourly_avg_trips"] == {}
        assert body["daily_avg_trips"] == {}

    def test_api_server_http_error_returns_502(self, client, mock_fetch_results):
        """Returns 502 when API Server responds with an HTTP error."""
        mock_response = httpx.Response(
            status_code=500,
            request=httpx.Request(method="GET", url="http://fake/analytical-results"),
        )
        mock_fetch_results.side_effect = httpx.HTTPStatusError(
            message="Internal Server Error",
            request=mock_response.request,
            response=mock_response,
        )

        response = client.get("/aggregations/temporal-patterns")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "500" in body["detail"]

    def test_api_server_unreachable_returns_502(self, client, mock_fetch_results):
        """Returns 502 when API Server is unreachable."""
        mock_fetch_results.side_effect = httpx.ConnectError(
            message="Connection refused"
        )

        response = client.get("/aggregations/temporal-patterns")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "unreachable" in body["detail"].lower()


class TestDataQualityRoute:
    """Tests for GET /aggregations/data-quality."""

    def test_success_with_results(self, client, mock_fetch_results):
        """Returns aggregated data quality when API Server returns results."""
        mock_fetch_results.return_value = [
            {
                "summary_data": {
                    "outlier_counts": {
                        "fare_amount": {
                            "iqr": 100,
                            "zscore": 80,
                            "isolation_forest": 150,
                        },
                    },
                    "quality_violations": {
                        "negative_fares": 10,
                        "zero_distances": 20,
                    },
                    "strategy_comparison": {
                        "removal": {
                            "rows_before": 1000,
                            "rows_after": 950,
                            "rows_removed": 50,
                        },
                    },
                    "num_rows": 1000,
                }
            },
        ]

        response = client.get("/aggregations/data-quality")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] == 1
        assert body["total_rows_processed"] == 1000
        assert body["outlier_summary"]["iqr"]["total_outliers"] == 100
        assert body["quality_violations"]["negative_fares"] == 10
        assert body["overall_removal_rate_percent"] == 5.0
        mock_fetch_results.assert_called_once_with(
            result_type="data_cleaning",
            taxi_type=None,
            year=None,
            month=None,
        )

    def test_success_with_filters(self, client, mock_fetch_results):
        """Passes query params through to fetch and includes them in response."""
        mock_fetch_results.return_value = []

        response = client.get(
            "/aggregations/data-quality",
            params={"taxi_type": "yellow", "start_year": "2023", "start_month": "06"},
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["filters_applied"]["taxi_type"] == "yellow"
        assert body["filters_applied"]["start_year"] == "2023"
        assert body["filters_applied"]["start_month"] == "06"
        mock_fetch_results.assert_called_once_with(
            result_type="data_cleaning",
            taxi_type="yellow",
            year="2023",
            month="06",
        )

    def test_empty_results(self, client, mock_fetch_results):
        """Returns zero counts when no results match."""
        mock_fetch_results.return_value = []

        response = client.get("/aggregations/data-quality")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] == 0
        assert body["total_rows_processed"] == 0
        assert body["outlier_summary"] == {}
        assert body["quality_violations"] == {}
        assert body["overall_removal_rate_percent"] == 0.0

    def test_api_server_http_error_returns_502(self, client, mock_fetch_results):
        """Returns 502 when API Server responds with an HTTP error."""
        mock_response = httpx.Response(
            status_code=500,
            request=httpx.Request(method="GET", url="http://fake/analytical-results"),
        )
        mock_fetch_results.side_effect = httpx.HTTPStatusError(
            message="Internal Server Error",
            request=mock_response.request,
            response=mock_response,
        )

        response = client.get("/aggregations/data-quality")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "500" in body["detail"]

    def test_api_server_unreachable_returns_502(self, client, mock_fetch_results):
        """Returns 502 when API Server is unreachable."""
        mock_fetch_results.side_effect = httpx.ConnectError(
            message="Connection refused"
        )

        response = client.get("/aggregations/data-quality")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "unreachable" in body["detail"].lower()


class TestPipelinePerformanceRoute:
    """Tests for GET /aggregations/pipeline-performance."""

    def test_success_with_results(
        self, client, mock_fetch_results, mock_fetch_pipeline_summary
    ):
        """Returns aggregated pipeline performance when API Server returns results."""
        mock_fetch_results.return_value = [
            {
                "result_type": "descriptive_statistics",
                "computation_time_seconds": 45.2,
                "file_info": {"file_id": 1},
            },
            {
                "result_type": "descriptive_statistics",
                "computation_time_seconds": 31.0,
                "file_info": {"file_id": 2},
            },
            {
                "result_type": "data_cleaning",
                "computation_time_seconds": 67.8,
                "file_info": {"file_id": 1},
            },
        ]
        mock_fetch_pipeline_summary.return_value = {
            "total_hours_saved_by_checkpointing": 1.25,
            "percent_time_saved": 3.9,
        }

        response = client.get("/aggregations/pipeline-performance")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] == 2
        assert "descriptive_statistics" in body["steps"]
        assert "data_cleaning" in body["steps"]
        assert body["steps"]["descriptive_statistics"]["files_processed"] == 2
        assert body["steps"]["data_cleaning"]["files_processed"] == 1
        assert body["total_computation_seconds"] > 0
        assert body["pipeline_summary"]["total_hours_saved_by_checkpointing"] == 1.25
        assert body["pipeline_summary"]["percent_time_saved"] == 3.9
        mock_fetch_results.assert_called_once_with(
            result_type=None,
            taxi_type=None,
            year=None,
            month=None,
        )
        mock_fetch_pipeline_summary.assert_called_once()

    def test_success_with_filters(
        self, client, mock_fetch_results, mock_fetch_pipeline_summary
    ):
        """Passes query params through and includes them in response."""
        mock_fetch_results.return_value = []
        mock_fetch_pipeline_summary.return_value = {
            "total_hours_saved_by_checkpointing": 0.0,
            "percent_time_saved": 0.0,
        }

        response = client.get(
            "/aggregations/pipeline-performance",
            params={
                "taxi_type": "yellow",
                "analytical_step": "data_cleaning",
                "start_year": "2023",
                "start_month": "06",
            },
        )

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["filters_applied"]["taxi_type"] == "yellow"
        assert body["filters_applied"]["analytical_step"] == "data_cleaning"
        assert body["filters_applied"]["start_year"] == "2023"
        assert body["filters_applied"]["start_month"] == "06"
        mock_fetch_results.assert_called_once_with(
            result_type="data_cleaning",
            taxi_type="yellow",
            year="2023",
            month="06",
        )

    def test_empty_results(
        self, client, mock_fetch_results, mock_fetch_pipeline_summary
    ):
        """Returns zero counts when no results match."""
        mock_fetch_results.return_value = []
        mock_fetch_pipeline_summary.return_value = {
            "total_hours_saved_by_checkpointing": 0.5,
            "percent_time_saved": 2.0,
        }

        response = client.get("/aggregations/pipeline-performance")

        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["file_count"] == 0
        assert body["steps"] == {}
        assert body["total_computation_seconds"] == 0.0
        assert body["avg_computation_per_file_seconds"] == 0.0
        assert body["pipeline_summary"]["total_hours_saved_by_checkpointing"] == 0.5

    def test_api_server_http_error_on_results_returns_502(
        self, client, mock_fetch_results, mock_fetch_pipeline_summary
    ):
        """Returns 502 when API Server responds with an HTTP error on results fetch."""
        mock_response = httpx.Response(
            status_code=500,
            request=httpx.Request(method="GET", url="http://fake/analytical-results"),
        )
        mock_fetch_results.side_effect = httpx.HTTPStatusError(
            message="Internal Server Error",
            request=mock_response.request,
            response=mock_response,
        )

        response = client.get("/aggregations/pipeline-performance")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "500" in body["detail"]

    def test_api_server_http_error_on_summary_returns_502(
        self, client, mock_fetch_results, mock_fetch_pipeline_summary
    ):
        """Returns 502 when API Server responds with an HTTP error on summary fetch."""
        mock_fetch_results.return_value = []
        mock_response = httpx.Response(
            status_code=503,
            request=httpx.Request(
                method="GET", url="http://fake/metrics/pipeline-summary"
            ),
        )
        mock_fetch_pipeline_summary.side_effect = httpx.HTTPStatusError(
            message="Service Unavailable",
            request=mock_response.request,
            response=mock_response,
        )

        response = client.get("/aggregations/pipeline-performance")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "503" in body["detail"]

    def test_api_server_unreachable_returns_502(
        self, client, mock_fetch_results, mock_fetch_pipeline_summary
    ):
        """Returns 502 when API Server is unreachable."""
        mock_fetch_results.side_effect = httpx.ConnectError(
            message="Connection refused"
        )

        response = client.get("/aggregations/pipeline-performance")

        assert response.status_code == status.HTTP_502_BAD_GATEWAY
        body = response.json()
        assert body["error"] == "Bad Gateway"
        assert "unreachable" in body["detail"].lower()
