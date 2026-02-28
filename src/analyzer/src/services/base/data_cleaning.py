"""Abstract base class for the data cleaning analytical step."""

from abc import ABC, abstractmethod

import polars as pl

from src.server.models import StepResult


class BaseDataCleaning(ABC):
    """Run outlier detection, cleaning strategies, and data quality validation.

    Concrete implementations handle taxi-type-specific columns and rules.
    """

    @abstractmethod
    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run data cleaning on the given dataframe.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with summary_data dict, detail_bytes, and detail_s3_key.
        """


if __name__ == "__main__":
    print(f"ABC defined: {BaseDataCleaning.__name__}")
