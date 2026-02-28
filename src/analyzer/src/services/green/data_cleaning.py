"""Green taxi data cleaning implementation."""

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

_OUTLIER_COLUMNS = [
    "fare_amount",
    "trip_distance",
    "total_amount",
    "tip_amount",
    "tolls_amount",
]


def _compute_duration_seconds(df: pl.DataFrame) -> pl.Series:
    """Compute trip duration in seconds from pickup/dropoff datetimes."""
    pickup = df["lpep_pickup_datetime"].cast(pl.Datetime)
    dropoff = df["lpep_dropoff_datetime"].cast(pl.Datetime)
    return (dropoff - pickup).dt.total_seconds()


def _quality_rules(df: pl.DataFrame) -> dict[str, int]:
    """Check Green-specific data quality rules and return violation counts."""
    violations: dict[str, int] = {}

    if "fare_amount" in df.columns:
        violations["negative_fares"] = int(df.filter(pl.col("fare_amount") < 0).height)

    if "trip_distance" in df.columns:
        violations["zero_distances"] = int(
            df.filter(pl.col("trip_distance") == 0).height
        )

    if "lpep_pickup_datetime" in df.columns and "lpep_dropoff_datetime" in df.columns:
        durations = _compute_duration_seconds(df=df)
        violations["impossible_durations"] = int((durations <= 0).sum())

    if "passenger_count" in df.columns:
        violations["invalid_passenger_count"] = int(
            df.filter(
                (pl.col("passenger_count") <= 0) | (pl.col("passenger_count") > 9)
            ).height
        )

    if "ehail_fee" in df.columns:
        violations["negative_ehail_fee"] = int(
            df.filter(pl.col("ehail_fee") < 0).height
        )

    return violations


class GreenDataCleaning(BaseDataCleaning):
    """Data cleaning for green taxi data."""

    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run data cleaning on green taxi dataframe.

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
            "fare_amount": [10.0, 20.0, -5.0, 500.0, 15.0],
            "trip_distance": [2.0, 0.0, 5.0, 100.0, 3.0],
            "total_amount": [12.0, 22.0, -3.0, 520.0, 17.0],
            "tip_amount": [2.0, 0.0, 1.0, 50.0, 3.0],
            "tolls_amount": [0.0, 0.0, 0.0, 10.0, 0.0],
            "ehail_fee": [0.0, 1.0, -0.5, 2.0, 0.0],
            "lpep_pickup_datetime": [
                "2023-01-01T00:00:00",
                "2023-01-01T01:00:00",
                "2023-01-01T02:00:00",
                "2023-01-01T03:00:00",
                "2023-01-01T04:00:00",
            ],
            "lpep_dropoff_datetime": [
                "2023-01-01T00:30:00",
                "2023-01-01T01:30:00",
                "2023-01-01T01:00:00",
                "2023-01-01T03:30:00",
                "2023-01-01T04:30:00",
            ],
            "passenger_count": [1, 2, 0, 3, 1],
        }
    )
    result = GreenDataCleaning().analyze(df=sample)
    print(f"Summary: {result.summary_data}")
