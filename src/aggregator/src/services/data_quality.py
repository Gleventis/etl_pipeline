"""Aggregate data quality metrics across multiple analytical results."""

from src.server.models import (
    DataQualityResponse,
    FiltersApplied,
    OutlierMethodSummary,
)

_OUTLIER_METHODS = ("iqr", "zscore", "isolation_forest")


def aggregate_data_quality(
    results: list[dict],
    filters: FiltersApplied,
) -> DataQualityResponse:
    """Aggregate data cleaning results across multiple files.

    Sums outlier counts per detection method, sums quality violations,
    and computes overall removal rate.

    Args:
        results: List of analytical result dicts, each containing a
            'summary_data' key with outlier_counts, quality_violations,
            strategy_comparison, and num_rows.
        filters: Filters that were applied to produce this aggregation.

    Returns:
        Aggregated data quality response.
    """
    if not results:
        return _empty_response(filters=filters)

    summaries = [r["summary_data"] for r in results if "summary_data" in r]
    if not summaries:
        return _empty_response(filters=filters, file_count=len(results))

    total_rows = sum(s.get("num_rows", 0) for s in summaries)

    # Aggregate outlier counts per method across all columns and files
    method_totals: dict[str, int] = {m: 0 for m in _OUTLIER_METHODS}
    for s in summaries:
        for _col, methods in s.get("outlier_counts", {}).items():
            for method in _OUTLIER_METHODS:
                method_totals[method] += methods.get(method, 0)

    outlier_summary: dict[str, OutlierMethodSummary] = {}
    for method, total in method_totals.items():
        avg_rate = (total / total_rows * 100) if total_rows > 0 else 0.0
        outlier_summary[method] = OutlierMethodSummary(
            total_outliers=total,
            avg_rate_percent=round(avg_rate, 2),
        )

    # Sum quality violations across files
    quality_violations: dict[str, int] = {}
    for s in summaries:
        for violation, count in s.get("quality_violations", {}).items():
            quality_violations[violation] = quality_violations.get(violation, 0) + count

    # Overall removal rate from strategy_comparison
    total_removed = 0
    total_before = 0
    for s in summaries:
        removal = s.get("strategy_comparison", {}).get("removal", {})
        total_before += removal.get("rows_before", 0)
        total_removed += removal.get("rows_removed", 0)

    overall_rate = (total_removed / total_before * 100) if total_before > 0 else 0.0

    return DataQualityResponse(
        file_count=len(results),
        total_rows_processed=total_rows,
        outlier_summary=outlier_summary,
        quality_violations=quality_violations,
        overall_removal_rate_percent=round(overall_rate, 2),
        filters_applied=filters,
    )


def _empty_response(
    filters: FiltersApplied,
    file_count: int = 0,
) -> DataQualityResponse:
    """Return a zero-valued response."""
    return DataQualityResponse(
        file_count=file_count,
        total_rows_processed=0,
        outlier_summary={},
        quality_violations={},
        overall_removal_rate_percent=0.0,
        filters_applied=filters,
    )


if __name__ == "__main__":
    sample_results = [
        {
            "summary_data": {
                "outlier_counts": {
                    "fare_amount": {"iqr": 100, "zscore": 80, "isolation_forest": 150},
                    "trip_distance": {
                        "iqr": 50,
                        "zscore": 40,
                        "isolation_forest": 60,
                    },
                },
                "quality_violations": {
                    "negative_fares": 10,
                    "zero_distances": 20,
                    "impossible_durations": 5,
                },
                "strategy_comparison": {
                    "removal": {
                        "rows_before": 1000,
                        "rows_after": 950,
                        "rows_removed": 50,
                    },
                },
                "num_rows": 1000,
                "num_outlier_columns": 2,
            }
        },
        {
            "summary_data": {
                "outlier_counts": {
                    "fare_amount": {"iqr": 200, "zscore": 160, "isolation_forest": 250},
                },
                "quality_violations": {
                    "negative_fares": 5,
                    "zero_distances": 15,
                },
                "strategy_comparison": {
                    "removal": {
                        "rows_before": 2000,
                        "rows_after": 1900,
                        "rows_removed": 100,
                    },
                },
                "num_rows": 2000,
                "num_outlier_columns": 1,
            }
        },
    ]
    sample_filters = FiltersApplied(
        taxi_type="yellow", start_year="2022", start_month="01"
    )
    response = aggregate_data_quality(results=sample_results, filters=sample_filters)
    print(response.model_dump_json(indent=2))
