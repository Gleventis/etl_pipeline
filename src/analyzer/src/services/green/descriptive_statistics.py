"""Green taxi descriptive statistics implementation."""

import polars as pl

from src.services.base.descriptive_statistics import BaseDescriptiveStatistics

_GREEN_NUMERIC = [
    "vendorid",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "total_amount",
    "trip_type",
    "congestion_surcharge",
]


class GreenDescriptiveStatistics(BaseDescriptiveStatistics):
    """Descriptive statistics for green taxi data."""

    def _numeric_columns(self, df: pl.DataFrame) -> list[str]:
        """Return numeric columns present in the green taxi dataframe."""
        return [c for c in _GREEN_NUMERIC if c in df.columns]
