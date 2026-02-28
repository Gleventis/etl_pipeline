"""Yellow taxi descriptive statistics implementation."""

import polars as pl

from src.services.base.descriptive_statistics import BaseDescriptiveStatistics

_YELLOW_NUMERIC = [
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
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]


class YellowDescriptiveStatistics(BaseDescriptiveStatistics):
    """Descriptive statistics for yellow taxi data."""

    def _numeric_columns(self, df: pl.DataFrame) -> list[str]:
        """Return numeric columns present in the yellow taxi dataframe."""
        return [c for c in _YELLOW_NUMERIC if c in df.columns]
