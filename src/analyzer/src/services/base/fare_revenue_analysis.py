"""Abstract base class for the fare revenue analysis analytical step."""

from abc import ABC, abstractmethod

import polars as pl

from src.server.models import StepResult


class BaseFareRevenueAnalysis(ABC):
    """Revenue forecasting, fare anomaly detection, tip prediction, surcharge breakdowns.

    Concrete implementations handle taxi-type-specific fare and surcharge columns.
    """

    @abstractmethod
    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run fare revenue analysis on the given dataframe.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with summary_data dict, detail_bytes, and detail_s3_key.
        """


if __name__ == "__main__":
    print(f"ABC defined: {BaseFareRevenueAnalysis.__name__}")
