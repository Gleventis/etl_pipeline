"""Tests for aggregate_data_quality service."""

from src.server.models import FiltersApplied
from src.services.data_quality import aggregate_data_quality


def _make_filters(**kwargs: str | None) -> FiltersApplied:
    return FiltersApplied(**kwargs)


def _make_result(
    outlier_counts: dict | None = None,
    quality_violations: dict | None = None,
    rows_before: int = 1000,
    rows_removed: int = 50,
    num_rows: int = 1000,
) -> dict:
    return {
        "summary_data": {
            "outlier_counts": outlier_counts or {},
            "quality_violations": quality_violations or {},
            "strategy_comparison": {
                "removal": {
                    "rows_before": rows_before,
                    "rows_after": rows_before - rows_removed,
                    "rows_removed": rows_removed,
                },
            },
            "num_rows": num_rows,
        }
    }


class TestAggregateDataQuality:
    """Tests for aggregate_data_quality function."""

    def test_empty_results(self) -> None:
        filters = _make_filters(taxi_type="yellow")
        response = aggregate_data_quality(results=[], filters=filters)

        assert response.file_count == 0
        assert response.total_rows_processed == 0
        assert response.outlier_summary == {}
        assert response.quality_violations == {}
        assert response.overall_removal_rate_percent == 0.0

    def test_single_file_outlier_summation(self) -> None:
        filters = _make_filters()
        results = [
            _make_result(
                outlier_counts={
                    "fare_amount": {"iqr": 100, "zscore": 80, "isolation_forest": 150},
                    "trip_distance": {"iqr": 50, "zscore": 40, "isolation_forest": 60},
                },
                num_rows=5000,
            )
        ]

        response = aggregate_data_quality(results=results, filters=filters)

        assert response.file_count == 1
        assert response.total_rows_processed == 5000
        assert response.outlier_summary["iqr"].total_outliers == 150
        assert response.outlier_summary["zscore"].total_outliers == 120
        assert response.outlier_summary["isolation_forest"].total_outliers == 210

    def test_multiple_files_outlier_summation(self) -> None:
        filters = _make_filters()
        results = [
            _make_result(
                outlier_counts={
                    "fare_amount": {"iqr": 100, "zscore": 80, "isolation_forest": 150},
                },
                num_rows=1000,
            ),
            _make_result(
                outlier_counts={
                    "fare_amount": {"iqr": 200, "zscore": 160, "isolation_forest": 250},
                },
                num_rows=2000,
            ),
        ]

        response = aggregate_data_quality(results=results, filters=filters)

        assert response.file_count == 2
        assert response.total_rows_processed == 3000
        assert response.outlier_summary["iqr"].total_outliers == 300
        assert response.outlier_summary["zscore"].total_outliers == 240
        assert response.outlier_summary["isolation_forest"].total_outliers == 400

    def test_outlier_rate_calculation(self) -> None:
        filters = _make_filters()
        results = [
            _make_result(
                outlier_counts={
                    "col_a": {"iqr": 100, "zscore": 0, "isolation_forest": 0},
                },
                num_rows=10000,
            )
        ]

        response = aggregate_data_quality(results=results, filters=filters)

        # 100 / 10000 * 100 = 1.0%
        assert response.outlier_summary["iqr"].avg_rate_percent == 1.0
        assert response.outlier_summary["zscore"].avg_rate_percent == 0.0

    def test_quality_violations_summed(self) -> None:
        filters = _make_filters()
        results = [
            _make_result(
                quality_violations={
                    "negative_fares": 10,
                    "zero_distances": 20,
                },
            ),
            _make_result(
                quality_violations={
                    "negative_fares": 5,
                    "zero_distances": 15,
                    "impossible_durations": 3,
                },
            ),
        ]

        response = aggregate_data_quality(results=results, filters=filters)

        assert response.quality_violations["negative_fares"] == 15
        assert response.quality_violations["zero_distances"] == 35
        assert response.quality_violations["impossible_durations"] == 3

    def test_overall_removal_rate(self) -> None:
        filters = _make_filters()
        results = [
            _make_result(rows_before=1000, rows_removed=50),
            _make_result(rows_before=2000, rows_removed=100),
        ]

        response = aggregate_data_quality(results=results, filters=filters)

        # (50 + 100) / (1000 + 2000) * 100 = 5.0%
        assert response.overall_removal_rate_percent == 5.0

    def test_results_without_summary_data(self) -> None:
        filters = _make_filters()
        results = [{"no_summary": True}]

        response = aggregate_data_quality(results=results, filters=filters)

        assert response.file_count == 1
        assert response.total_rows_processed == 0
        assert response.outlier_summary == {}

    def test_filters_passed_through(self) -> None:
        filters = _make_filters(
            taxi_type="green",
            start_year="2023",
            start_month="06",
            end_year="2023",
            end_month="12",
        )

        response = aggregate_data_quality(results=[], filters=filters)

        assert response.filters_applied.taxi_type == "green"
        assert response.filters_applied.start_year == "2023"
        assert response.filters_applied.end_month == "12"
