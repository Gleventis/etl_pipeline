"""FHV taxi data cleaning implementation."""

import logging

import polars as pl

from src.server.models import StepResult
from src.services.base.cleaning_utils import (
    apply_capping_strategy,
    apply_removal_strategy,
    build_step_result,
    run_outlier_detection,
)
from src.services.base.data_cleaning import BaseDataCleaning

logger = logging.getLogger(__name__)

_OUTLIER_COLUMNS: list[str] = []


def _compute_duration_seconds(df: pl.DataFrame) -> pl.Series:
    """Compute trip duration in seconds from pickup/dropoff datetimes."""
    pickup = df["pickup_datetime"].cast(pl.Datetime)
    dropoff = df["dropoff_datetime"].cast(pl.Datetime)
    return (dropoff - pickup).dt.total_seconds()


def _quality_rules(df: pl.DataFrame) -> dict[str, int]:
    """Check FHV-specific data quality rules and return violation counts.

    FHV has no fare or distance columns, so only duration rules apply.
    """
    violations: dict[str, int] = {}

    if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
        durations = _compute_duration_seconds(df=df)
        violations["impossible_durations"] = int((durations <= 0).sum())

    return violations


class FhvDataCleaning(BaseDataCleaning):
    """Data cleaning for FHV taxi data.

    FHV data has no fare or distance columns, so outlier detection is limited.
    Only duration-based quality rules are applied.
    """

    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run data cleaning on FHV taxi dataframe.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with cleaning summary, cleaned parquet bytes, and s3 key.
        """
        if df.height == 0:
            return StepResult(
                summary_data={"skipped": True, "reason": "empty dataframe"},
                detail_bytes=b"",
                detail_s3_key="data_cleaning_detail.parquet",
            )

        columns = [c for c in _OUTLIER_COLUMNS if c in df.columns]
        outlier_counts = run_outlier_detection(df=df, columns=columns)
        quality_violations = _quality_rules(df=df)
        removed_df = apply_removal_strategy(df=df, columns=columns)
        capped_df = apply_capping_strategy(df=df, columns=columns)

        return build_step_result(
            df=df,
            cleaned_df=removed_df,
            capped_df=capped_df,
            columns=columns,
            outlier_counts=outlier_counts,
            quality_violations=quality_violations,
        )


if __name__ == "__main__":
    sample = pl.DataFrame(
        {
            "dispatching_base_num": ["B00001", "B00002", "B00003", "B00004", "B00005"],
            "pickup_datetime": [
                "2023-01-01T00:00:00",
                "2023-01-01T01:00:00",
                "2023-01-01T02:00:00",
                "2023-01-01T03:00:00",
                "2023-01-01T04:00:00",
            ],
            "dropoff_datetime": [
                "2023-01-01T00:30:00",
                "2023-01-01T01:30:00",
                "2023-01-01T01:00:00",
                "2023-01-01T03:30:00",
                "2023-01-01T04:30:00",
            ],
            "pulocationid": [1, 2, 3, 4, 5],
            "dolocationid": [6, 7, 8, 9, 10],
            "sr_flag": [None, 1, None, 1, None],
            "affiliated_base_number": [
                "B00001",
                "B00002",
                "B00003",
                "B00004",
                "B00005",
            ],
        }
    )
    result = FhvDataCleaning().analyze(df=sample)
    print(f"Summary: {result.summary_data}")
