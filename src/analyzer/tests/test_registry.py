"""Tests for the analyzer registry."""

import pytest

from src.server.models import StepName, TaxiType
from src.services.base.data_cleaning import BaseDataCleaning
from src.services.base.descriptive_statistics import BaseDescriptiveStatistics
from src.services.base.fare_revenue_analysis import BaseFareRevenueAnalysis
from src.services.base.geospatial_analysis import BaseGeospatialAnalysis
from src.services.base.temporal_analysis import BaseTemporalAnalysis
from src.services.registry import get_analyzer

_STEP_TO_BASE: dict[StepName, type] = {
    StepName.DESCRIPTIVE_STATISTICS: BaseDescriptiveStatistics,
    StepName.DATA_CLEANING: BaseDataCleaning,
    StepName.TEMPORAL_ANALYSIS: BaseTemporalAnalysis,
    StepName.GEOSPATIAL_ANALYSIS: BaseGeospatialAnalysis,
    StepName.FARE_REVENUE_ANALYSIS: BaseFareRevenueAnalysis,
}

_ALL_COMBINATIONS = [(step, taxi_type) for step in StepName for taxi_type in TaxiType]


class TestGetAnalyzer:
    """Verify registry resolves all 20 combinations and rejects unknowns."""

    @pytest.mark.parametrize(
        ("step_name", "taxi_type"),
        _ALL_COMBINATIONS,
        ids=[f"{s.value}-{t.value}" for s, t in _ALL_COMBINATIONS],
    )
    def test_resolves_all_combinations(
        self, step_name: StepName, taxi_type: TaxiType
    ) -> None:
        analyzer = get_analyzer(step_name=step_name, taxi_type=taxi_type)
        expected_base = _STEP_TO_BASE[step_name]
        assert isinstance(analyzer, expected_base)

    def test_raises_for_unknown_step(self) -> None:
        with pytest.raises(ValueError, match="no analyzer registered"):
            get_analyzer(step_name="nonexistent", taxi_type=TaxiType.YELLOW)  # type: ignore[arg-type]

    def test_raises_for_unknown_taxi_type(self) -> None:
        with pytest.raises(ValueError, match="no analyzer registered"):
            get_analyzer(step_name=StepName.DATA_CLEANING, taxi_type="unknown")  # type: ignore[arg-type]

    def test_returns_new_instance_each_call(self) -> None:
        a = get_analyzer(
            step_name=StepName.DESCRIPTIVE_STATISTICS, taxi_type=TaxiType.YELLOW
        )
        b = get_analyzer(
            step_name=StepName.DESCRIPTIVE_STATISTICS, taxi_type=TaxiType.YELLOW
        )
        assert a is not b
