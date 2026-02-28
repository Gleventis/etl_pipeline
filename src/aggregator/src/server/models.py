"""Pydantic response models for the aggregator service."""

from pydantic import BaseModel, ConfigDict


class FiltersApplied(BaseModel):
    """Filters that were applied to produce the aggregation result."""

    model_config = ConfigDict(frozen=True)

    taxi_type: str | None = None
    start_year: str | None = None
    start_month: str | None = None
    end_year: str | None = None
    end_month: str | None = None


class ColumnStats(BaseModel):
    """Aggregated statistics for a single numeric column."""

    model_config = ConfigDict(frozen=True)

    mean: float | None = None
    min: float | None = None
    max: float | None = None
    percentiles: dict[str, float] = {}


class DescriptiveStatsResponse(BaseModel):
    """Response for GET /aggregations/descriptive-stats."""

    model_config = ConfigDict(frozen=True)

    file_count: int
    total_rows: int
    aggregated_stats: dict[str, ColumnStats]
    filters_applied: FiltersApplied


class TemporalPatternsResponse(BaseModel):
    """Response for GET /aggregations/temporal-patterns."""

    model_config = ConfigDict(frozen=True)

    file_count: int
    hourly_avg_trips: dict[str, float]
    peak_hours: list[int]
    daily_avg_trips: dict[str, float]
    filters_applied: FiltersApplied


class TaxiMetrics(BaseModel):
    """Key metrics for a single taxi type."""

    model_config = ConfigDict(frozen=True)

    file_count: int
    total_rows: int
    avg_fare: float | None = None
    avg_trip_distance: float | None = None
    avg_tip_percentage: float | None = None


class TaxiComparisonResponse(BaseModel):
    """Response for GET /aggregations/taxi-comparison."""

    model_config = ConfigDict(frozen=True)

    comparison: dict[str, TaxiMetrics]
    filters_applied: FiltersApplied


class OutlierMethodSummary(BaseModel):
    """Outlier detection summary for a single method."""

    model_config = ConfigDict(frozen=True)

    total_outliers: int
    avg_rate_percent: float


class DataQualityResponse(BaseModel):
    """Response for GET /aggregations/data-quality."""

    model_config = ConfigDict(frozen=True)

    file_count: int
    total_rows_processed: int
    outlier_summary: dict[str, OutlierMethodSummary]
    quality_violations: dict[str, int]
    overall_removal_rate_percent: float
    filters_applied: FiltersApplied


class PipelineFiltersApplied(FiltersApplied):
    """Filters applied for pipeline performance, including analytical step."""

    analytical_step: str | None = None


class StepPerformance(BaseModel):
    """Computation time statistics for a single analytical step."""

    model_config = ConfigDict(frozen=True)

    files_processed: int
    avg_computation_seconds: float
    total_computation_seconds: float
    min_computation_seconds: float
    max_computation_seconds: float


class PipelineSavings(BaseModel):
    """Checkpoint savings summary from the pipeline."""

    model_config = ConfigDict(frozen=True)

    total_hours_saved_by_checkpointing: float
    percent_time_saved: float


class PipelinePerformanceResponse(BaseModel):
    """Response for GET /aggregations/pipeline-performance."""

    model_config = ConfigDict(frozen=True)

    file_count: int
    steps: dict[str, StepPerformance]
    total_computation_seconds: float
    avg_computation_per_file_seconds: float
    pipeline_summary: PipelineSavings
    filters_applied: PipelineFiltersApplied


if __name__ == "__main__":
    response = DescriptiveStatsResponse(
        file_count=12,
        total_rows=29567172,
        aggregated_stats={
            "avg_fare": ColumnStats(
                mean=13.52,
                min=8.21,
                max=18.73,
                percentiles={"p50": 12.5, "p95": 45.0},
            )
        },
        filters_applied=FiltersApplied(
            taxi_type="yellow",
            start_year="2022",
            start_month="01",
            end_year="2022",
            end_month="12",
        ),
    )
    print(response.model_dump_json(indent=2))

    temporal_response = TemporalPatternsResponse(
        file_count=12,
        hourly_avg_trips={"0": 1523.4, "17": 5432.1, "23": 2103.8},
        peak_hours=[17, 18, 19],
        daily_avg_trips={"monday": 45231.0, "tuesday": 47892.0},
        filters_applied=FiltersApplied(
            taxi_type="yellow",
            start_year="2022",
            start_month="01",
            end_year="2022",
            end_month="12",
        ),
    )
    print(temporal_response.model_dump_json(indent=2))

    taxi_response = TaxiComparisonResponse(
        comparison={
            "yellow": TaxiMetrics(
                file_count=12,
                total_rows=29567172,
                avg_fare=13.52,
                avg_trip_distance=3.2,
                avg_tip_percentage=18.5,
            ),
            "fhv": TaxiMetrics(
                file_count=12,
                total_rows=5678901,
            ),
        },
        filters_applied=FiltersApplied(
            start_year="2022",
            start_month="01",
            end_year="2022",
            end_month="12",
        ),
    )
    print(taxi_response.model_dump_json(indent=2))

    data_quality_response = DataQualityResponse(
        file_count=12,
        total_rows_processed=29567172,
        outlier_summary={
            "iqr": OutlierMethodSummary(total_outliers=234567, avg_rate_percent=0.79),
            "zscore": OutlierMethodSummary(
                total_outliers=198432, avg_rate_percent=0.67
            ),
            "isolation_forest": OutlierMethodSummary(
                total_outliers=312456, avg_rate_percent=1.06
            ),
        },
        quality_violations={
            "negative_fares": 1234,
            "zero_distances": 5678,
            "impossible_durations": 890,
        },
        overall_removal_rate_percent=1.2,
        filters_applied=FiltersApplied(
            taxi_type="yellow",
            start_year="2022",
            start_month="01",
            end_year="2022",
            end_month="12",
        ),
    )
    print(data_quality_response.model_dump_json(indent=2))

    pipeline_response = PipelinePerformanceResponse(
        file_count=12,
        steps={
            "descriptive_statistics": StepPerformance(
                files_processed=12,
                avg_computation_seconds=45.2,
                total_computation_seconds=542.4,
                min_computation_seconds=31.0,
                max_computation_seconds=58.7,
            ),
            "data_cleaning": StepPerformance(
                files_processed=12,
                avg_computation_seconds=67.8,
                total_computation_seconds=813.6,
                min_computation_seconds=45.1,
                max_computation_seconds=89.2,
            ),
        },
        total_computation_seconds=3456.7,
        avg_computation_per_file_seconds=288.1,
        pipeline_summary=PipelineSavings(
            total_hours_saved_by_checkpointing=1.25,
            percent_time_saved=3.9,
        ),
        filters_applied=PipelineFiltersApplied(
            taxi_type="yellow",
            analytical_step=None,
            start_year="2022",
            start_month="01",
            end_year="2022",
            end_month="12",
        ),
    )
    print(pipeline_response.model_dump_json(indent=2))
