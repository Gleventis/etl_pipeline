"""Aggregate pipeline performance metrics across analytical results."""

from src.server.models import (
    PipelineFiltersApplied,
    PipelinePerformanceResponse,
    PipelineSavings,
    StepPerformance,
)


def aggregate_pipeline_performance(
    results: list[dict],
    pipeline_summary: dict,
    filters: PipelineFiltersApplied,
) -> PipelinePerformanceResponse:
    """Aggregate computation time per analytical step across files.

    Groups results by result_type, computes per-step min/max/avg/total
    of computation_time_seconds, and includes pipeline checkpoint savings.

    Args:
        results: List of analytical result dicts from the API Server,
            each with 'result_type' and 'computation_time_seconds' keys.
        pipeline_summary: Dict from GET /metrics/pipeline-summary with
            'total_hours_saved_by_checkpointing' and 'percent_time_saved'.
        filters: Filters that were applied to produce this aggregation.

    Returns:
        Aggregated pipeline performance response.
    """
    if not results:
        return _empty_response(
            pipeline_summary=pipeline_summary,
            filters=filters,
        )

    # Group computation times by result_type
    step_times: dict[str, list[float]] = {}
    for r in results:
        result_type = r.get("result_type")
        comp_time = r.get("computation_time_seconds")
        if result_type is not None and comp_time is not None:
            step_times.setdefault(result_type, []).append(comp_time)

    # Collect unique file identifiers to count distinct files
    file_ids: set[int | str] = set()
    for r in results:
        file_info = r.get("file_info")
        if file_info and "file_id" in file_info:
            file_ids.add(file_info["file_id"])

    file_count = len(file_ids) if file_ids else len(results)

    steps: dict[str, StepPerformance] = {}
    total_computation = 0.0

    for step, times in sorted(step_times.items()):
        step_total = sum(times)
        total_computation += step_total
        steps[step] = StepPerformance(
            files_processed=len(times),
            avg_computation_seconds=round(step_total / len(times), 2),
            total_computation_seconds=round(step_total, 2),
            min_computation_seconds=round(min(times), 2),
            max_computation_seconds=round(max(times), 2),
        )

    avg_per_file = round(total_computation / file_count, 2) if file_count > 0 else 0.0

    savings = PipelineSavings(
        total_hours_saved_by_checkpointing=pipeline_summary.get(
            "total_hours_saved_by_checkpointing", 0.0
        ),
        percent_time_saved=pipeline_summary.get("percent_time_saved", 0.0),
    )

    return PipelinePerformanceResponse(
        file_count=file_count,
        steps=steps,
        total_computation_seconds=round(total_computation, 2),
        avg_computation_per_file_seconds=avg_per_file,
        pipeline_summary=savings,
        filters_applied=filters,
    )


def _empty_response(
    pipeline_summary: dict,
    filters: PipelineFiltersApplied,
) -> PipelinePerformanceResponse:
    """Return a zero-valued response."""
    return PipelinePerformanceResponse(
        file_count=0,
        steps={},
        total_computation_seconds=0.0,
        avg_computation_per_file_seconds=0.0,
        pipeline_summary=PipelineSavings(
            total_hours_saved_by_checkpointing=pipeline_summary.get(
                "total_hours_saved_by_checkpointing", 0.0
            ),
            percent_time_saved=pipeline_summary.get("percent_time_saved", 0.0),
        ),
        filters_applied=filters,
    )


if __name__ == "__main__":
    sample_results = [
        {
            "result_type": "descriptive_statistics",
            "computation_time_seconds": 45.2,
            "file_info": {"file_id": 1, "bucket": "raw", "object_name": "f1.parquet"},
        },
        {
            "result_type": "descriptive_statistics",
            "computation_time_seconds": 31.0,
            "file_info": {"file_id": 2, "bucket": "raw", "object_name": "f2.parquet"},
        },
        {
            "result_type": "data_cleaning",
            "computation_time_seconds": 67.8,
            "file_info": {"file_id": 1, "bucket": "raw", "object_name": "f1.parquet"},
        },
        {
            "result_type": "data_cleaning",
            "computation_time_seconds": 89.2,
            "file_info": {"file_id": 2, "bucket": "raw", "object_name": "f2.parquet"},
        },
    ]
    sample_summary = {
        "total_hours_saved_by_checkpointing": 1.25,
        "percent_time_saved": 3.9,
    }
    sample_filters = PipelineFiltersApplied(
        taxi_type="yellow",
        start_year="2022",
        start_month="01",
        end_year="2022",
        end_month="12",
    )
    response = aggregate_pipeline_performance(
        results=sample_results,
        pipeline_summary=sample_summary,
        filters=sample_filters,
    )
    print(response.model_dump_json(indent=2))
