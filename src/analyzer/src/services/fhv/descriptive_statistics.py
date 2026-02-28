"""FHV taxi descriptive statistics implementation."""

import polars as pl

from src.services.base.descriptive_statistics import BaseDescriptiveStatistics

_FHV_NUMERIC = [
    "sr_flag",
    "pulocationid",
    "dolocationid",
]


class FhvDescriptiveStatistics(BaseDescriptiveStatistics):
    """Descriptive statistics for FHV taxi data.

    FHV data has very few numeric columns — only sr_flag and location IDs.
    Histograms and correlation are computed on whatever is available.
    """

    def _numeric_columns(self, df: pl.DataFrame) -> list[str]:
        """Return numeric columns present in the FHV taxi dataframe."""
        return [c for c in _FHV_NUMERIC if c in df.columns]
