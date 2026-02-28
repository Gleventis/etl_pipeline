"""Tests for aggregate_pipeline_performance service."""

from src.server.models import PipelineFiltersApplied
from src.services.pipeline_performance import aggregate_pipeline_performance


def _make_filters(**kwargs: str | None) -> PipelineFiltersApplied:
    return PipelineFiltersApplied(**kwargs)


def _make_summary(
    hours_saved: float = 1.5,
    percent_saved: float = 4.2,
) -> dict:
    return {
        "total_hours_saved_by_checkpointing": hours_saved,
        "percent_time_saved": percent_saved,
    }


def _make_result(
    result_type: str,
    computation_time: float,
    file_id: int = 1,
) -> dict:
    return {
        "result_type": result_type,
        "computation_time_seconds": computation_time,
        "file_info": {"file_id": file_id, "bucket": "raw", "object_name": "f.parquet"},
    }


class TestAggregatePipelinePerformance:
    """Tests for aggregate_pipeline_performance function."""

    def test_empty_results(self) -> None:
        filters = _make_filters(taxi_type="yellow")
        summary = _make_summary(hours_saved=2.0, percent_saved=5.0)

        response = aggregate_pipeline_performance(
            results=[],
            pipeline_summary=summary,
            filters=filters,
        )

        assert response.file_count == 0
        assert response.steps == {}
        assert response.total_computation_seconds == 0.0
        assert response.avg_computation_per_file_seconds == 0.0
        assert response.pipeline_summary.total_hours_saved_by_checkpointing == 2.0
        assert response.pipeline_summary.percent_time_saved == 5.0

    def test_single_step_single_file(self) -> None:
        filters = _make_filters()
        summary = _make_summary()
        results = [
            _make_result(
                result_type="descriptive_statistics",
                computation_time=45.2,
                file_id=1,
            ),
        ]

        response = aggregate_pipeline_performance(
            results=results,
            pipeline_summary=summary,
            filters=filters,
        )

        assert response.file_count == 1
        assert "descriptive_statistics" in response.steps
        step = response.steps["descriptive_statistics"]
        assert step.files_processed == 1
        assert step.avg_computation_seconds == 45.2
        assert step.min_computation_seconds == 45.2
        assert step.max_computation_seconds == 45.2
        assert step.total_computation_seconds == 45.2
        assert response.total_computation_seconds == 45.2

    def test_per_step_grouping_multiple_steps(self) -> None:
        filters = _make_filters()
        summary = _make_summary()
        results = [
            _make_result(
                result_type="descriptive_statistics", computation_time=40.0, file_id=1
            ),
            _make_result(
                result_type="descriptive_statistics", computation_time=60.0, file_id=2
            ),
            _make_result(result_type="data_cleaning", computation_time=80.0, file_id=1),
            _make_result(
                result_type="data_cleaning", computation_time=100.0, file_id=2
            ),
        ]

        response = aggregate_pipeline_performance(
            results=results,
            pipeline_summary=summary,
            filters=filters,
        )

        assert response.file_count == 2
        assert len(response.steps) == 2

        desc = response.steps["descriptive_statistics"]
        assert desc.files_processed == 2
        assert desc.avg_computation_seconds == 50.0
        assert desc.min_computation_seconds == 40.0
        assert desc.max_computation_seconds == 60.0
        assert desc.total_computation_seconds == 100.0

        clean = response.steps["data_cleaning"]
        assert clean.files_processed == 2
        assert clean.avg_computation_seconds == 90.0
        assert clean.min_computation_seconds == 80.0
        assert clean.max_computation_seconds == 100.0
        assert clean.total_computation_seconds == 180.0

        assert response.total_computation_seconds == 280.0

    def test_avg_computation_per_file(self) -> None:
        filters = _make_filters()
        summary = _make_summary()
        results = [
            _make_result(
                result_type="descriptive_statistics", computation_time=30.0, file_id=1
            ),
            _make_result(result_type="data_cleaning", computation_time=70.0, file_id=1),
            _make_result(
                result_type="descriptive_statistics", computation_time=50.0, file_id=2
            ),
            _make_result(result_type="data_cleaning", computation_time=90.0, file_id=2),
        ]

        response = aggregate_pipeline_performance(
            results=results,
            pipeline_summary=summary,
            filters=filters,
        )

        # total = 30 + 70 + 50 + 90 = 240, 2 files → 120.0 per file
        assert response.file_count == 2
        assert response.avg_computation_per_file_seconds == 120.0

    def test_pipeline_summary_included(self) -> None:
        filters = _make_filters()
        summary = _make_summary(hours_saved=3.5, percent_saved=12.1)
        results = [
            _make_result(
                result_type="temporal_analysis", computation_time=10.0, file_id=1
            ),
        ]

        response = aggregate_pipeline_performance(
            results=results,
            pipeline_summary=summary,
            filters=filters,
        )

        assert response.pipeline_summary.total_hours_saved_by_checkpointing == 3.5
        assert response.pipeline_summary.percent_time_saved == 12.1

    def test_results_without_file_info_fallback_count(self) -> None:
        """When file_info is missing, file_count falls back to len(results)."""
        filters = _make_filters()
        summary = _make_summary()
        results = [
            {"result_type": "descriptive_statistics", "computation_time_seconds": 20.0},
            {"result_type": "descriptive_statistics", "computation_time_seconds": 30.0},
        ]

        response = aggregate_pipeline_performance(
            results=results,
            pipeline_summary=summary,
            filters=filters,
        )

        assert response.file_count == 2

    def test_results_missing_computation_time_skipped(self) -> None:
        """Results without computation_time_seconds are skipped in step grouping."""
        filters = _make_filters()
        summary = _make_summary()
        results = [
            _make_result(
                result_type="descriptive_statistics", computation_time=50.0, file_id=1
            ),
            {"result_type": "descriptive_statistics", "file_info": {"file_id": 2}},
        ]

        response = aggregate_pipeline_performance(
            results=results,
            pipeline_summary=summary,
            filters=filters,
        )

        assert response.file_count == 2
        step = response.steps["descriptive_statistics"]
        assert step.files_processed == 1
        assert step.total_computation_seconds == 50.0

    def test_filters_passed_through(self) -> None:
        filters = _make_filters(
            taxi_type="green",
            start_year="2023",
            start_month="01",
            end_year="2023",
            end_month="06",
            analytical_step="data_cleaning",
        )
        summary = _make_summary()

        response = aggregate_pipeline_performance(
            results=[],
            pipeline_summary=summary,
            filters=filters,
        )

        assert response.filters_applied.taxi_type == "green"
        assert response.filters_applied.analytical_step == "data_cleaning"
        assert response.filters_applied.start_year == "2023"
        assert response.filters_applied.end_month == "06"

    def test_empty_pipeline_summary_defaults(self) -> None:
        filters = _make_filters()
        summary: dict = {}

        response = aggregate_pipeline_performance(
            results=[],
            pipeline_summary=summary,
            filters=filters,
        )

        assert response.pipeline_summary.total_hours_saved_by_checkpointing == 0.0
        assert response.pipeline_summary.percent_time_saved == 0.0
