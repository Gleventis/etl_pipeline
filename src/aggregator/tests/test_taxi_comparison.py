"""Tests for aggregate_taxi_comparison service."""

from src.server.models import FiltersApplied
from src.services.taxi_comparison import aggregate_taxi_comparison


def _make_filters(**kwargs: str | None) -> FiltersApplied:
    return FiltersApplied(**kwargs)


def _make_result(
    num_rows: int = 1000,
    distribution: dict | None = None,
) -> dict:
    return {
        "summary_data": {
            "num_rows": num_rows,
            "distribution": distribution or {},
        }
    }


class TestAggregateTaxiComparison:
    """Tests for aggregate_taxi_comparison function."""

    def test_empty_results_by_type(self) -> None:
        filters = _make_filters()
        response = aggregate_taxi_comparison(
            results_by_type={},
            filters=filters,
        )

        assert response.comparison == {}

    def test_all_types_present(self) -> None:
        filters = _make_filters(start_year="2023")
        results_by_type = {
            "yellow": [
                _make_result(
                    num_rows=1000000,
                    distribution={
                        "fare_amount": {"mean": 13.0},
                        "trip_distance": {"mean": 3.0},
                        "tip_amount": {"mean": 2.0},
                    },
                ),
            ],
            "green": [
                _make_result(
                    num_rows=500000,
                    distribution={
                        "fare_amount": {"mean": 11.0},
                        "trip_distance": {"mean": 4.0},
                        "tip_amount": {"mean": 1.5},
                    },
                ),
            ],
            "fhvhv": [
                _make_result(
                    num_rows=2000000,
                    distribution={
                        "base_passenger_fare": {"mean": 20.0},
                        "trip_miles": {"mean": 5.0},
                        "tips": {"mean": 3.0},
                    },
                ),
            ],
            "fhv": [
                _make_result(num_rows=300000),
            ],
        }

        response = aggregate_taxi_comparison(
            results_by_type=results_by_type,
            filters=filters,
        )

        assert len(response.comparison) == 4
        assert response.comparison["yellow"].file_count == 1
        assert response.comparison["yellow"].total_rows == 1000000
        assert response.comparison["yellow"].avg_fare == 13.0
        assert response.comparison["green"].avg_trip_distance == 4.0
        assert response.comparison["fhv"].file_count == 1

    def test_missing_types_only_present_keys(self) -> None:
        filters = _make_filters()
        results_by_type = {
            "yellow": [
                _make_result(
                    num_rows=100,
                    distribution={"fare_amount": {"mean": 10.0}},
                ),
            ],
        }

        response = aggregate_taxi_comparison(
            results_by_type=results_by_type,
            filters=filters,
        )

        assert "yellow" in response.comparison
        assert "green" not in response.comparison
        assert "fhv" not in response.comparison

    def test_fhv_null_fare_fields(self) -> None:
        """FHV data has no fare/distance/tip columns."""
        filters = _make_filters(taxi_type="fhv")
        results_by_type = {
            "fhv": [
                _make_result(
                    num_rows=500000,
                    distribution={
                        "sr_flag": {"mean": 0.1},
                        "pulocationid": {"mean": 130.0},
                    },
                ),
            ],
        }

        response = aggregate_taxi_comparison(
            results_by_type=results_by_type,
            filters=filters,
        )

        fhv = response.comparison["fhv"]
        assert fhv.file_count == 1
        assert fhv.total_rows == 500000
        assert fhv.avg_fare is None
        assert fhv.avg_trip_distance is None
        assert fhv.avg_tip_percentage is None

    def test_multiple_files_per_type(self) -> None:
        filters = _make_filters()
        results_by_type = {
            "yellow": [
                _make_result(
                    num_rows=1000,
                    distribution={
                        "fare_amount": {"mean": 10.0},
                        "trip_distance": {"mean": 2.0},
                        "tip_amount": {"mean": 1.0},
                    },
                ),
                _make_result(
                    num_rows=2000,
                    distribution={
                        "fare_amount": {"mean": 20.0},
                        "trip_distance": {"mean": 4.0},
                        "tip_amount": {"mean": 3.0},
                    },
                ),
            ],
        }

        response = aggregate_taxi_comparison(
            results_by_type=results_by_type,
            filters=filters,
        )

        yellow = response.comparison["yellow"]
        assert yellow.file_count == 2
        assert yellow.total_rows == 3000
        assert yellow.avg_fare == 15.0  # (10 + 20) / 2
        assert yellow.avg_trip_distance == 3.0  # (2 + 4) / 2

    def test_tip_percentage_calculation(self) -> None:
        filters = _make_filters()
        results_by_type = {
            "yellow": [
                _make_result(
                    num_rows=100,
                    distribution={
                        "fare_amount": {"mean": 20.0},
                        "tip_amount": {"mean": 4.0},
                    },
                ),
            ],
        }

        response = aggregate_taxi_comparison(
            results_by_type=results_by_type,
            filters=filters,
        )

        assert response.comparison["yellow"].avg_tip_percentage == 20.0

    def test_filters_passed_through(self) -> None:
        filters = _make_filters(
            start_year="2023",
            start_month="06",
            end_year="2023",
            end_month="12",
        )

        response = aggregate_taxi_comparison(
            results_by_type={},
            filters=filters,
        )

        assert response.filters_applied.start_year == "2023"
        assert response.filters_applied.end_month == "12"

    def test_type_with_empty_results_list(self) -> None:
        filters = _make_filters()
        results_by_type = {
            "yellow": [],
        }

        response = aggregate_taxi_comparison(
            results_by_type=results_by_type,
            filters=filters,
        )

        yellow = response.comparison["yellow"]
        assert yellow.file_count == 0
        assert yellow.total_rows == 0
        assert yellow.avg_fare is None

    def test_fhvhv_uses_trip_miles_and_tips(self) -> None:
        """FHVHV uses trip_miles instead of trip_distance, tips instead of tip_amount."""
        filters = _make_filters()
        results_by_type = {
            "fhvhv": [
                _make_result(
                    num_rows=100,
                    distribution={
                        "trip_miles": {"mean": 6.0},
                        "tips": {"mean": 3.0},
                        "fare_amount": {"mean": 25.0},
                    },
                ),
            ],
        }

        response = aggregate_taxi_comparison(
            results_by_type=results_by_type,
            filters=filters,
        )

        fhvhv = response.comparison["fhvhv"]
        assert fhvhv.avg_trip_distance == 6.0
        assert fhvhv.avg_tip_percentage == 12.0  # 3/25 * 100
