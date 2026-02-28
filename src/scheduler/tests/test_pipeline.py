"""Tests for pipeline step definitions and resolution."""

import pytest

from src.services.config import Settings
from src.services.pipeline import STEPS, get_input_bucket, get_next_step


class TestSteps:
    """Tests for the STEPS constant."""

    def test_step_count(self) -> None:
        assert len(STEPS) == 5

    def test_step_order(self) -> None:
        assert STEPS == [
            "descriptive_statistics",
            "data_cleaning",
            "temporal_analysis",
            "geospatial_analysis",
            "fare_revenue_analysis",
        ]


class TestGetInputBucket:
    """Tests for get_input_bucket."""

    @pytest.fixture()
    def settings(self, monkeypatch: pytest.MonkeyPatch) -> Settings:
        monkeypatch.delenv("STEP_DESCRIPTIVE_STATISTICS_BUCKET", raising=False)
        monkeypatch.delenv("STEP_DATA_CLEANING_BUCKET", raising=False)
        monkeypatch.delenv("STEP_TEMPORAL_ANALYSIS_BUCKET", raising=False)
        monkeypatch.delenv("STEP_GEOSPATIAL_ANALYSIS_BUCKET", raising=False)
        monkeypatch.delenv("STEP_FARE_REVENUE_ANALYSIS_BUCKET", raising=False)
        return Settings()

    def test_descriptive_statistics_default(self, settings: Settings) -> None:
        result = get_input_bucket(step="descriptive_statistics", settings=settings)
        assert result == "raw-data"

    def test_data_cleaning_default(self, settings: Settings) -> None:
        result = get_input_bucket(step="data_cleaning", settings=settings)
        assert result == "raw-data"

    def test_temporal_analysis_default(self, settings: Settings) -> None:
        result = get_input_bucket(step="temporal_analysis", settings=settings)
        assert result == "cleaned-data"

    def test_geospatial_analysis_default(self, settings: Settings) -> None:
        result = get_input_bucket(step="geospatial_analysis", settings=settings)
        assert result == "cleaned-data"

    def test_fare_revenue_analysis_default(self, settings: Settings) -> None:
        result = get_input_bucket(step="fare_revenue_analysis", settings=settings)
        assert result == "cleaned-data"

    def test_custom_bucket_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("STEP_DESCRIPTIVE_STATISTICS_BUCKET", "custom-bucket")
        settings = Settings()
        result = get_input_bucket(step="descriptive_statistics", settings=settings)
        assert result == "custom-bucket"

    def test_unknown_step_raises(self, settings: Settings) -> None:
        with pytest.raises(ValueError, match="unknown pipeline step"):
            get_input_bucket(step="nonexistent", settings=settings)

    def test_all_steps_have_bucket(self, settings: Settings) -> None:
        for step in STEPS:
            result = get_input_bucket(step=step, settings=settings)
            assert isinstance(result, str)
            assert len(result) > 0


class TestGetNextStep:
    """Tests for get_next_step."""

    def test_no_completed_returns_first(self) -> None:
        result = get_next_step(completed_steps=[])
        assert result == "descriptive_statistics"

    def test_first_completed_returns_second(self) -> None:
        result = get_next_step(completed_steps=["descriptive_statistics"])
        assert result == "data_cleaning"

    def test_partial_completion(self) -> None:
        result = get_next_step(
            completed_steps=["descriptive_statistics", "data_cleaning"]
        )
        assert result == "temporal_analysis"

    def test_all_completed_returns_none(self) -> None:
        result = get_next_step(completed_steps=list(STEPS))
        assert result is None

    def test_out_of_order_completion(self) -> None:
        result = get_next_step(
            completed_steps=["data_cleaning", "descriptive_statistics"]
        )
        assert result == "temporal_analysis"

    def test_four_completed_returns_last(self) -> None:
        result = get_next_step(completed_steps=STEPS[:4])
        assert result == "fare_revenue_analysis"
