"""Aggregate taxi type comparison from descriptive statistics results."""

from src.server.models import FiltersApplied, TaxiComparisonResponse, TaxiMetrics


def _extract_metrics(results: list[dict]) -> TaxiMetrics:
    """Extract averaged metrics from a list of descriptive_statistics results.

    Args:
        results: Analytical result dicts for a single taxi type.

    Returns:
        Aggregated taxi metrics.
    """
    if not results:
        return TaxiMetrics(file_count=0, total_rows=0)

    summaries = [r["summary_data"] for r in results if "summary_data" in r]
    total_rows = sum(s.get("num_rows", 0) for s in summaries)

    fare_means: list[float] = []
    distance_means: list[float] = []
    tip_means: list[float] = []

    for s in summaries:
        dist = s.get("distribution", {})
        fare_mean = dist.get("fare_amount", {}).get("mean")
        if fare_mean is not None:
            fare_means.append(fare_mean)

        distance_mean = dist.get("trip_distance", {}).get("mean")
        if distance_mean is None:
            distance_mean = dist.get("trip_miles", {}).get("mean")
        if distance_mean is not None:
            distance_means.append(distance_mean)

        tip_mean = dist.get("tip_amount", {}).get("mean")
        if tip_mean is None:
            tip_mean = dist.get("tips", {}).get("mean")
        if tip_mean is not None:
            tip_means.append(tip_mean)

    avg_fare = sum(fare_means) / len(fare_means) if fare_means else None
    avg_distance = sum(distance_means) / len(distance_means) if distance_means else None

    avg_tip_pct: float | None = None
    if tip_means and fare_means:
        avg_tip = sum(tip_means) / len(tip_means)
        avg_fare_val = sum(fare_means) / len(fare_means)
        if avg_fare_val > 0:
            avg_tip_pct = round(avg_tip / avg_fare_val * 100, 2)

    return TaxiMetrics(
        file_count=len(results),
        total_rows=total_rows,
        avg_fare=avg_fare,
        avg_trip_distance=avg_distance,
        avg_tip_percentage=avg_tip_pct,
    )


def aggregate_taxi_comparison(
    results_by_type: dict[str, list[dict]],
    filters: FiltersApplied,
) -> TaxiComparisonResponse:
    """Aggregate taxi comparison from per-type descriptive statistics results.

    Args:
        results_by_type: Dict mapping taxi type name to its list of
            analytical result dicts (descriptive_statistics).
        filters: Filters that were applied (taxi_type is ignored for comparison).

    Returns:
        Side-by-side taxi type comparison response.
    """
    comparison = {
        taxi_type: _extract_metrics(results=results)
        for taxi_type, results in results_by_type.items()
    }

    return TaxiComparisonResponse(
        comparison=comparison,
        filters_applied=filters,
    )


if __name__ == "__main__":
    sample_results_by_type = {
        "yellow": [
            {
                "summary_data": {
                    "num_rows": 1000000,
                    "distribution": {
                        "fare_amount": {"mean": 13.52},
                        "trip_distance": {"mean": 3.2},
                        "tip_amount": {"mean": 2.5},
                    },
                }
            },
        ],
        "fhv": [
            {
                "summary_data": {
                    "num_rows": 500000,
                    "distribution": {
                        "sr_flag": {"mean": 0.1},
                        "pulocationid": {"mean": 130.0},
                    },
                }
            },
        ],
    }
    sample_filters = FiltersApplied(start_year="2022", start_month="01")
    response = aggregate_taxi_comparison(
        results_by_type=sample_results_by_type,
        filters=sample_filters,
    )
    print(response.model_dump_json(indent=2))
