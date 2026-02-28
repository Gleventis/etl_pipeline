"""Abstract base class for the descriptive statistics analytical step."""

import io
import json
import logging
from abc import ABC, abstractmethod

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from scipy import stats

from src.server.models import StepResult

logger = logging.getLogger(__name__)

_PERCENTILES = (1, 5, 10, 25, 50, 75, 90, 95, 99)
_HISTOGRAM_BINS = 100


class BaseDescriptiveStatistics(ABC):
    """Compute percentiles, histograms, correlation matrix, and distribution statistics.

    Concrete implementations handle taxi-type-specific column selection.
    """

    @abstractmethod
    def _numeric_columns(self, df: pl.DataFrame) -> list[str]:
        """Return the ordered list of numeric columns to analyze.

        Args:
            df: Input dataframe.

        Returns:
            Column names that exist in df and should be analyzed.
        """

    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run descriptive statistics on the given dataframe.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with summary_data dict, detail_bytes, and detail_s3_key.
        """
        columns = self._numeric_columns(df=df)
        if not columns:
            return StepResult(
                summary_data={"skipped": True, "reason": "no numeric columns"},
                detail_bytes=b"",
                detail_s3_key="descriptive_statistics_detail.parquet",
            )

        arrays = {
            col: df[col].drop_nulls().to_numpy().astype(np.float64) for col in columns
        }

        # --- percentiles ---
        percentiles = {}
        for col, arr in arrays.items():
            if len(arr) == 0:
                continue
            percentiles[col] = {
                f"p{p}": float(np.percentile(a=arr, q=p)) for p in _PERCENTILES
            }

        # --- histograms ---
        histograms = {}
        for col, arr in arrays.items():
            if len(arr) == 0:
                continue
            counts, bin_edges = np.histogram(a=arr, bins=_HISTOGRAM_BINS)
            histograms[col] = {
                "counts": counts.tolist(),
                "bin_edges": bin_edges.tolist(),
            }

        # --- correlation matrix ---
        valid_cols = [c for c in columns if len(arrays[c]) == len(df)]
        correlation: dict[str, dict[str, float]] = {}
        if len(valid_cols) >= 2:
            matrix_data = np.column_stack([arrays[c] for c in valid_cols])
            corr = np.corrcoef(matrix_data, rowvar=False)
            for i, c1 in enumerate(valid_cols):
                correlation[c1] = {}
                for j, c2 in enumerate(valid_cols):
                    val = float(corr[i, j])
                    correlation[c1][c2] = val if np.isfinite(val) else None  # type: ignore[assignment]

        # --- distribution statistics ---
        distribution = {}
        for col, arr in arrays.items():
            if len(arr) < 3:
                continue
            distribution[col] = {
                "skewness": float(stats.skew(a=arr)),
                "kurtosis": float(stats.kurtosis(a=arr)),
                "mean": float(np.mean(a=arr)),
                "std": float(np.std(a=arr)),
            }

        summary_data = {
            "percentiles": percentiles,
            "distribution": distribution,
            "correlation_columns": valid_cols,
            "num_rows": len(df),
            "num_numeric_columns": len(columns),
        }

        # --- detail parquet: histograms + correlation ---
        detail = {
            "histograms": json.dumps(histograms),
            "correlation": json.dumps(correlation),
        }
        table = pa.table(
            {k: [v] for k, v in detail.items()},
        )
        buf = io.BytesIO()
        pq.write_table(table=table, where=buf)

        return StepResult(
            summary_data=summary_data,
            detail_bytes=buf.getvalue(),
            detail_s3_key="descriptive_statistics_detail.parquet",
        )


if __name__ == "__main__":
    print(f"ABC defined: {BaseDescriptiveStatistics.__name__}")
