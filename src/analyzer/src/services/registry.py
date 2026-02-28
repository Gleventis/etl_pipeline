"""Registry mapping (step_name, taxi_type) to concrete analyzer classes."""

from src.server.models import StepName, TaxiType
from src.services.base.data_cleaning import BaseDataCleaning
from src.services.base.descriptive_statistics import BaseDescriptiveStatistics
from src.services.base.fare_revenue_analysis import BaseFareRevenueAnalysis
from src.services.base.geospatial_analysis import BaseGeospatialAnalysis
from src.services.base.temporal_analysis import BaseTemporalAnalysis
from src.services.fhv.data_cleaning import FhvDataCleaning
from src.services.fhv.descriptive_statistics import FhvDescriptiveStatistics
from src.services.fhv.fare_revenue_analysis import FhvFareRevenueAnalysis
from src.services.fhv.geospatial_analysis import FhvGeospatialAnalysis
from src.services.fhv.temporal_analysis import FhvTemporalAnalysis
from src.services.fhvhv.data_cleaning import FhvhvDataCleaning
from src.services.fhvhv.descriptive_statistics import FhvhvDescriptiveStatistics
from src.services.fhvhv.fare_revenue_analysis import FhvhvFareRevenueAnalysis
from src.services.fhvhv.geospatial_analysis import FhvhvGeospatialAnalysis
from src.services.fhvhv.temporal_analysis import FhvhvTemporalAnalysis
from src.services.green.data_cleaning import GreenDataCleaning
from src.services.green.descriptive_statistics import GreenDescriptiveStatistics
from src.services.green.fare_revenue_analysis import GreenFareRevenueAnalysis
from src.services.green.geospatial_analysis import GreenGeospatialAnalysis
from src.services.green.temporal_analysis import GreenTemporalAnalysis
from src.services.yellow.data_cleaning import YellowDataCleaning
from src.services.yellow.descriptive_statistics import YellowDescriptiveStatistics
from src.services.yellow.fare_revenue_analysis import YellowFareRevenueAnalysis
from src.services.yellow.geospatial_analysis import YellowGeospatialAnalysis
from src.services.yellow.temporal_analysis import YellowTemporalAnalysis

_BaseAnalyzer = (
    BaseDescriptiveStatistics
    | BaseDataCleaning
    | BaseTemporalAnalysis
    | BaseGeospatialAnalysis
    | BaseFareRevenueAnalysis
)

_REGISTRY: dict[tuple[StepName, TaxiType], type[_BaseAnalyzer]] = {
    (StepName.DESCRIPTIVE_STATISTICS, TaxiType.YELLOW): YellowDescriptiveStatistics,
    (StepName.DESCRIPTIVE_STATISTICS, TaxiType.GREEN): GreenDescriptiveStatistics,
    (StepName.DESCRIPTIVE_STATISTICS, TaxiType.FHV): FhvDescriptiveStatistics,
    (StepName.DESCRIPTIVE_STATISTICS, TaxiType.FHVHV): FhvhvDescriptiveStatistics,
    (StepName.DATA_CLEANING, TaxiType.YELLOW): YellowDataCleaning,
    (StepName.DATA_CLEANING, TaxiType.GREEN): GreenDataCleaning,
    (StepName.DATA_CLEANING, TaxiType.FHV): FhvDataCleaning,
    (StepName.DATA_CLEANING, TaxiType.FHVHV): FhvhvDataCleaning,
    (StepName.TEMPORAL_ANALYSIS, TaxiType.YELLOW): YellowTemporalAnalysis,
    (StepName.TEMPORAL_ANALYSIS, TaxiType.GREEN): GreenTemporalAnalysis,
    (StepName.TEMPORAL_ANALYSIS, TaxiType.FHV): FhvTemporalAnalysis,
    (StepName.TEMPORAL_ANALYSIS, TaxiType.FHVHV): FhvhvTemporalAnalysis,
    (StepName.GEOSPATIAL_ANALYSIS, TaxiType.YELLOW): YellowGeospatialAnalysis,
    (StepName.GEOSPATIAL_ANALYSIS, TaxiType.GREEN): GreenGeospatialAnalysis,
    (StepName.GEOSPATIAL_ANALYSIS, TaxiType.FHV): FhvGeospatialAnalysis,
    (StepName.GEOSPATIAL_ANALYSIS, TaxiType.FHVHV): FhvhvGeospatialAnalysis,
    (StepName.FARE_REVENUE_ANALYSIS, TaxiType.YELLOW): YellowFareRevenueAnalysis,
    (StepName.FARE_REVENUE_ANALYSIS, TaxiType.GREEN): GreenFareRevenueAnalysis,
    (StepName.FARE_REVENUE_ANALYSIS, TaxiType.FHV): FhvFareRevenueAnalysis,
    (StepName.FARE_REVENUE_ANALYSIS, TaxiType.FHVHV): FhvhvFareRevenueAnalysis,
}


def get_analyzer(step_name: StepName, taxi_type: TaxiType) -> _BaseAnalyzer:
    """Resolve and instantiate the concrete analyzer for a step and taxi type.

    Args:
        step_name: Analytical pipeline step identifier.
        taxi_type: NYC TLC taxi type.

    Returns:
        Instance of the concrete analyzer class.

    Raises:
        ValueError: If the (step_name, taxi_type) combination is not registered.
    """
    key = (step_name, taxi_type)
    cls = _REGISTRY.get(key)
    if cls is None:
        raise ValueError(
            f"no analyzer registered for step={step_name!r}, taxi_type={taxi_type!r}"
        )
    return cls()


if __name__ == "__main__":
    analyzer = get_analyzer(
        step_name=StepName.DESCRIPTIVE_STATISTICS,
        taxi_type=TaxiType.YELLOW,
    )
    print(f"Resolved: {type(analyzer).__name__}")
