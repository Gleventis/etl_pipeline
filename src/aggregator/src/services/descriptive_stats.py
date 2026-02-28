"""Aggregate descriptive statistics across multiple analytical results."""

from src.server.models import ColumnStats, DescriptiveStatsResponse, FiltersApplied


def aggregate_descriptive_stats(
    results: list[dict],
    filters: FiltersApplied,
) -> DescriptiveStatsResponse:
    """Aggregate descriptive statistics from multiple analytical result dicts.

    Computes cross-file aggregates: mean of means, min of mins, max of maxes,
    averaged percentiles, and total row counts.

    Args:
        results: List of analytical result dicts, each containing a
            'summary_data' key with percentiles, distribution, and row counts.
        filters: Filters that were applied to produce this aggregation.

    Returns:
        Aggregated descriptive statistics response.
    """
    if not results:
        return DescriptiveStatsResponse(
            file_count=0,
            total_rows=0,
            aggregated_stats={},
            filters_applied=filters,
        )

    summaries = [r["summary_data"] for r in results if "summary_data" in r]
    if not summaries:
        return DescriptiveStatsResponse(
            file_count=len(results),
            total_rows=0,
            aggregated_stats={},
            filters_applied=filters,
        )

    total_rows = sum(s.get("num_rows", 0) for s in summaries)

    # Collect per-column distribution stats and percentiles across files
    col_means: dict[str, list[float]] = {}
    col_mins: dict[str, list[float]] = {}
    col_maxes: dict[str, list[float]] = {}
    col_percentiles: dict[str, dict[str, list[float]]] = {}

    for summary in summaries:
        distribution = summary.get("distribution", {})
        percentiles = summary.get("percentiles", {})

        for col, dist in distribution.items():
            mean_val = dist.get("mean")
            if mean_val is not None:
                col_means.setdefault(col, []).append(mean_val)

        for col, pcts in percentiles.items():
            for pct_key, pct_val in pcts.items():
                if pct_val is not None:
                    col_percentiles.setdefault(col, {}).setdefault(pct_key, []).append(
                        pct_val
                    )
                    if pct_key == "p1":
                        col_mins.setdefault(col, []).append(pct_val)
                    elif pct_key == "p99":
                        col_maxes.setdefault(col, []).append(pct_val)

    all_columns = set(col_means.keys()) | set(col_percentiles.keys())
    aggregated_stats: dict[str, ColumnStats] = {}

    for col in sorted(all_columns):
        mean = sum(col_means[col]) / len(col_means[col]) if col in col_means else None
        min_val = min(col_mins[col]) if col in col_mins else None
        max_val = max(col_maxes[col]) if col in col_maxes else None

        avg_percentiles: dict[str, float] = {}
        if col in col_percentiles:
            for pct_key, values in col_percentiles[col].items():
                avg_percentiles[pct_key] = sum(values) / len(values)

        aggregated_stats[col] = ColumnStats(
            mean=mean,
            min=min_val,
            max=max_val,
            percentiles=avg_percentiles,
        )

    return DescriptiveStatsResponse(
        file_count=len(results),
        total_rows=total_rows,
        aggregated_stats=aggregated_stats,
        filters_applied=filters,
    )


if __name__ == "__main__":
    sample_results = [
        {
            "summary_data": {
                "percentiles": {
                    "fare_amount": {"p1": 2.5, "p50": 12.0, "p99": 65.0},
                    "trip_distance": {"p1": 0.1, "p50": 1.6, "p99": 20.0},
                },
                "distribution": {
                    "fare_amount": {
                        "mean": 13.52,
                        "std": 12.1,
                        "skewness": 2.3,
                        "kurtosis": 8.1,
                    },
                    "trip_distance": {
                        "mean": 3.2,
                        "std": 4.5,
                        "skewness": 3.1,
                        "kurtosis": 15.2,
                    },
                },
                "num_rows": 1000000,
                "num_numeric_columns": 14,
            }
        },
        {
            "summary_data": {
                "percentiles": {
                    "fare_amount": {"p1": 2.0, "p50": 11.0, "p99": 60.0},
                    "trip_distance": {"p1": 0.2, "p50": 1.8, "p99": 18.0},
                },
                "distribution": {
                    "fare_amount": {
                        "mean": 12.10,
                        "std": 11.5,
                        "skewness": 2.1,
                        "kurtosis": 7.5,
                    },
                    "trip_distance": {
                        "mean": 3.5,
                        "std": 4.8,
                        "skewness": 3.0,
                        "kurtosis": 14.0,
                    },
                },
                "num_rows": 900000,
                "num_numeric_columns": 14,
            }
        },
    ]
    sample_filters = FiltersApplied(
        taxi_type="yellow", start_year="2022", start_month="01"
    )
    response = aggregate_descriptive_stats(
        results=sample_results, filters=sample_filters
    )
    print(response.model_dump_json(indent=2))
