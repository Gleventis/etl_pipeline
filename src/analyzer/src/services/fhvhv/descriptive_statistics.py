"""FHVHV taxi descriptive statistics implementation."""

import polars as pl

from src.services.base.descriptive_statistics import BaseDescriptiveStatistics

_FHVHV_NUMERIC = [
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "pulocationid",
    "dolocationid",
]


class FhvhvDescriptiveStatistics(BaseDescriptiveStatistics):
    """Descriptive statistics for FHVHV taxi data.

    Partial: trip_miles, trip_time, fare-related columns, and location IDs.
    """

    def _numeric_columns(self, df: pl.DataFrame) -> list[str]:
        """Return numeric columns present in the FHVHV taxi dataframe."""
        return [c for c in _FHVHV_NUMERIC if c in df.columns]
