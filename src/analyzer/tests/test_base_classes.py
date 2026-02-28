"""Tests for abstract base classes — importable and not directly instantiable."""

import pytest

from src.services.base.data_cleaning import BaseDataCleaning
from src.services.base.descriptive_statistics import BaseDescriptiveStatistics
from src.services.base.fare_revenue_analysis import BaseFareRevenueAnalysis
from src.services.base.geospatial_analysis import BaseGeospatialAnalysis
from src.services.base.temporal_analysis import BaseTemporalAnalysis

ALL_BASES = [
    BaseDescriptiveStatistics,
    BaseDataCleaning,
    BaseTemporalAnalysis,
    BaseGeospatialAnalysis,
    BaseFareRevenueAnalysis,
]


class TestAbstractBaseClasses:
    """Verify ABCs define the correct interface and reject direct instantiation."""

    @pytest.mark.parametrize("abc_class", ALL_BASES, ids=lambda c: c.__name__)
    def test_cannot_instantiate_directly(self, abc_class: type) -> None:
        with pytest.raises(TypeError, match="abstract method"):
            abc_class()

    @pytest.mark.parametrize("abc_class", ALL_BASES, ids=lambda c: c.__name__)
    def test_has_analyze_method(self, abc_class: type) -> None:
        assert hasattr(abc_class, "analyze")
        assert len(abc_class.__abstractmethods__) > 0
