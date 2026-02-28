"""Tests for aggregate_descriptive_stats service."""

from src.server.models import FiltersApplied
from src.services.descriptive_stats import aggregate_descriptive_stats


def _make_filters(**kwargs: str | None) -> FiltersApplied:
    return FiltersApplied(**kwargs)


def _make_result(
    percentiles: dict | None = None,
    distribution: dict | None = None,
    num_rows: int = 1000,
) -> dict:
    return {
        "summary_data": {
            "percentiles": percentiles or {},
            "distribution": distribution or {},
            "num_rows": num_rows,
        }
    }


class TestAggregateDescriptiveStats:
    """Tests for aggregate_descriptive_stats function."""

    def test_empty_results(self) -> None:
        filters = _make_filters(taxi_type="yellow")
        response = aggregate_descriptive_stats(results=[], filters=filters)

        assert response.file_count == 0
        assert response.total_rows == 0
        assert response.aggregated_stats == {}

    def test_single_file(self) -> None:
        filters = _make_filters(taxi_type="yellow")
        results = [
            _make_result(
                percentiles={
                    "fare_amount": {"p1": 2.5, "p50": 12.0, "p99": 65.0},
                },
                distribution={
                    "fare_amount": {"mean": 13.52},
                },
                num_rows=1000000,
            )
        ]
        response = aggregate_descriptive_stats(results=results, filters=filters)

        assert response.file_count == 1
        assert response.total_rows == 1000000
        assert "fare_amount" in response.aggregated_stats
        stats = response.aggregated_stats["fare_amount"]
        assert stats.mean == 13.52
        assert stats.min == 2.5
        assert stats.max == 65.0
        assert stats.percentiles["p50"] == 12.0

    def test_multiple_files_averages_means(self) -> None:
        filters = _make_filters()
        results = [
            _make_result(
                percentiles={"fare_amount": {"p1": 2.0, "p50": 10.0, "p99": 60.0}},
                distribution={"fare_amount": {"mean": 10.0}},
                num_rows=1000,
            ),
            _make_result(
                percentiles={"fare_amount": {"p1": 4.0, "p50": 14.0, "p99": 80.0}},
                distribution={"fare_amount": {"mean": 20.0}},
                num_rows=2000,
            ),
        ]
        response = aggregate_descriptive_stats(results=results, filters=filters)

        assert response.file_count == 2
        assert response.total_rows == 3000
        stats = response.aggregated_stats["fare_amount"]
        assert stats.mean == 15.0  # (10 + 20) / 2
        assert stats.min == 2.0  # min of p1 values
        assert stats.max == 80.0  # max of p99 values
        assert stats.percentiles["p50"] == 12.0  # (10 + 14) / 2

    def test_multiple_files_different_columns(self) -> None:
        filters = _make_filters()
        results = [
            _make_result(
                percentiles={"fare_amount": {"p1": 2.0, "p99": 60.0}},
                distribution={"fare_amount": {"mean": 10.0}},
                num_rows=1000,
            ),
            _make_result(
                percentiles={"trip_distance": {"p1": 0.1, "p99": 20.0}},
                distribution={"trip_distance": {"mean": 3.0}},
                num_rows=2000,
            ),
        ]
        response = aggregate_descriptive_stats(results=results, filters=filters)

        assert "fare_amount" in response.aggregated_stats
        assert "trip_distance" in response.aggregated_stats

    def test_results_without_summary_data(self) -> None:
        filters = _make_filters()
        results = [{"no_summary": True}]
        response = aggregate_descriptive_stats(results=results, filters=filters)

        assert response.file_count == 1
        assert response.total_rows == 0
        assert response.aggregated_stats == {}

    def test_filters_passed_through(self) -> None:
        filters = _make_filters(
            taxi_type="green",
            start_year="2023",
            start_month="06",
        )
        response = aggregate_descriptive_stats(results=[], filters=filters)

        assert response.filters_applied.taxi_type == "green"
        assert response.filters_applied.start_year == "2023"
        assert response.filters_applied.start_month == "06"

    def test_min_max_across_files(self) -> None:
        filters = _make_filters()
        results = [
            _make_result(
                percentiles={"col": {"p1": 5.0, "p99": 50.0}},
                distribution={"col": {"mean": 25.0}},
                num_rows=100,
            ),
            _make_result(
                percentiles={"col": {"p1": 1.0, "p99": 90.0}},
                distribution={"col": {"mean": 35.0}},
                num_rows=200,
            ),
        ]
        response = aggregate_descriptive_stats(results=results, filters=filters)

        stats = response.aggregated_stats["col"]
        assert stats.min == 1.0  # min across files
        assert stats.max == 90.0  # max across files
