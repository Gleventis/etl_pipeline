"""Tests for aggregate_temporal_patterns service."""

from src.server.models import FiltersApplied
from src.services.temporal_patterns import aggregate_temporal_patterns


def _make_filters(**kwargs: str | None) -> FiltersApplied:
    return FiltersApplied(**kwargs)


def _make_result(
    peak_hours: list[int] | None = None,
    num_rows: int = 1000,
    num_hours: int = 744,
) -> dict:
    return {
        "summary_data": {
            "peak_hours": peak_hours or [],
            "num_rows": num_rows,
            "num_hours": num_hours,
        }
    }


class TestAggregateTemporalPatterns:
    """Tests for aggregate_temporal_patterns function."""

    def test_empty_results(self) -> None:
        filters = _make_filters(taxi_type="yellow")
        response = aggregate_temporal_patterns(results=[], filters=filters)

        assert response.file_count == 0
        assert response.peak_hours == []
        assert response.hourly_avg_trips == {}
        assert response.daily_avg_trips == {}

    def test_single_file(self) -> None:
        filters = _make_filters(taxi_type="yellow")
        results = [_make_result(peak_hours=[8, 9, 17, 18, 19])]

        response = aggregate_temporal_patterns(results=results, filters=filters)

        assert response.file_count == 1
        # With one file, threshold is 0.5 — all hours appear once (>0.5)
        assert response.peak_hours == [8, 9, 17, 18, 19]

    def test_multiple_files_peak_hour_consensus(self) -> None:
        """Peak hours appearing in more than half the files are kept."""
        filters = _make_filters()
        results = [
            _make_result(peak_hours=[8, 9, 17, 18, 19]),
            _make_result(peak_hours=[9, 17, 18, 19, 20]),
            _make_result(peak_hours=[8, 9, 17, 18, 19]),
        ]

        response = aggregate_temporal_patterns(results=results, filters=filters)

        assert response.file_count == 3
        # threshold = 1.5 → hours with count > 1.5 (i.e. ≥2)
        # 8→2, 9→3, 17→3, 18→3, 19→3, 20→1
        assert 9 in response.peak_hours
        assert 17 in response.peak_hours
        assert 18 in response.peak_hours
        assert 19 in response.peak_hours
        assert 8 in response.peak_hours
        assert 20 not in response.peak_hours

    def test_peak_hour_fallback_when_threshold_filters_all(self) -> None:
        """When no hour exceeds threshold, most common hours are returned."""
        filters = _make_filters()
        # 2 files, each with unique peak hours → each hour appears once
        # threshold = 1.0, count must be > 1.0 → none pass
        results = [
            _make_result(peak_hours=[8, 9]),
            _make_result(peak_hours=[17, 18]),
        ]

        response = aggregate_temporal_patterns(results=results, filters=filters)

        # Fallback: all have max_count=1, so all are returned
        assert sorted(response.peak_hours) == [8, 9, 17, 18]

    def test_results_without_summary_data(self) -> None:
        filters = _make_filters()
        results = [{"no_summary": True}]

        response = aggregate_temporal_patterns(results=results, filters=filters)

        assert response.file_count == 1
        assert response.peak_hours == []
        assert response.hourly_avg_trips == {}

    def test_empty_peak_hours_in_summary(self) -> None:
        filters = _make_filters()
        results = [_make_result(peak_hours=[])]

        response = aggregate_temporal_patterns(results=results, filters=filters)

        assert response.file_count == 1
        assert response.peak_hours == []

    def test_filters_passed_through(self) -> None:
        filters = _make_filters(
            taxi_type="green",
            start_year="2023",
            start_month="06",
            end_year="2023",
            end_month="12",
        )

        response = aggregate_temporal_patterns(results=[], filters=filters)

        assert response.filters_applied.taxi_type == "green"
        assert response.filters_applied.start_year == "2023"
        assert response.filters_applied.end_month == "12"

    def test_peak_hours_sorted(self) -> None:
        """Returned peak hours should be in ascending order."""
        filters = _make_filters()
        results = [_make_result(peak_hours=[19, 8, 17, 9, 18])]

        response = aggregate_temporal_patterns(results=results, filters=filters)

        assert response.peak_hours == sorted(response.peak_hours)
