"""Aggregate temporal patterns across multiple analytical results."""

from collections import Counter

from src.server.models import FiltersApplied, TemporalPatternsResponse


def aggregate_temporal_patterns(
    results: list[dict],
    filters: FiltersApplied,
) -> TemporalPatternsResponse:
    """Aggregate temporal analysis results across multiple files.

    Merges peak hours by frequency across files. Hourly and daily volume
    breakdowns are not available in the analyzer's summary_data, so those
    fields are returned as empty dicts.

    Args:
        results: List of analytical result dicts, each containing a
            'summary_data' key with peak_hours, num_rows, num_hours.
        filters: Filters that were applied to produce this aggregation.

    Returns:
        Aggregated temporal patterns response.
    """
    if not results:
        return TemporalPatternsResponse(
            file_count=0,
            hourly_avg_trips={},
            peak_hours=[],
            daily_avg_trips={},
            filters_applied=filters,
        )

    summaries = [r["summary_data"] for r in results if "summary_data" in r]
    if not summaries:
        return TemporalPatternsResponse(
            file_count=len(results),
            hourly_avg_trips={},
            peak_hours=[],
            daily_avg_trips={},
            filters_applied=filters,
        )

    # Aggregate peak hours: count frequency across files, keep those appearing
    # in more than half the files
    peak_counter: Counter[int] = Counter()
    for s in summaries:
        for hour in s.get("peak_hours", []):
            peak_counter[hour] += 1

    threshold = len(summaries) / 2
    peak_hours = sorted(
        hour for hour, count in peak_counter.items() if count > threshold
    )

    # Fallback: if threshold filters out everything, take the most common ones
    if not peak_hours and peak_counter:
        max_count = peak_counter.most_common(1)[0][1]
        peak_hours = sorted(
            hour for hour, count in peak_counter.items() if count == max_count
        )

    return TemporalPatternsResponse(
        file_count=len(results),
        hourly_avg_trips={},
        peak_hours=peak_hours,
        daily_avg_trips={},
        filters_applied=filters,
    )


if __name__ == "__main__":
    sample_results = [
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
        {
            "summary_data": {
                "num_rows": 950000,
                "num_hours": 744,
                "peak_hours": [8, 9, 17, 18, 19],
            }
        },
    ]
    sample_filters = FiltersApplied(
        taxi_type="yellow", start_year="2022", start_month="01"
    )
    response = aggregate_temporal_patterns(
        results=sample_results, filters=sample_filters
    )
    print(response.model_dump_json(indent=2))
