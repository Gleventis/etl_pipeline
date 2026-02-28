"""Abstract base class for the temporal analysis analytical step."""

from abc import ABC, abstractmethod

import polars as pl

from src.server.models import StepResult


class BaseTemporalAnalysis(ABC):
    """Time-series decomposition, Fourier transforms, rolling stats, peak detection.

    Concrete implementations handle taxi-type-specific datetime columns.
    """

    @abstractmethod
    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run temporal analysis on the given dataframe.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with summary_data dict, detail_bytes, and detail_s3_key.
        """


if __name__ == "__main__":
    print(f"ABC defined: {BaseTemporalAnalysis.__name__}")
