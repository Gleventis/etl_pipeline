"""FHVHV taxi data cleaning implementation."""

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
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tips",
    "driver_pay",
]


def _compute_duration_seconds(df: pl.DataFrame) -> pl.Series:
    """Compute trip duration in seconds from pickup/dropoff datetimes."""
    pickup = df["pickup_datetime"].cast(pl.Datetime)
    dropoff = df["dropoff_datetime"].cast(pl.Datetime)
    return (dropoff - pickup).dt.total_seconds()


def _quality_rules(df: pl.DataFrame) -> dict[str, int]:
    """Check FHVHV-specific data quality rules and return violation counts."""
    violations: dict[str, int] = {}

    if "base_passenger_fare" in df.columns:
        violations["negative_fares"] = int(
            df.filter(pl.col("base_passenger_fare") < 0).height
        )

    if "trip_miles" in df.columns:
        violations["zero_distances"] = int(df.filter(pl.col("trip_miles") == 0).height)

    if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
        durations = _compute_duration_seconds(df=df)
        violations["impossible_durations"] = int((durations <= 0).sum())

    if "trip_time" in df.columns:
        violations["negative_trip_time"] = int(
            df.filter(pl.col("trip_time") <= 0).height
        )

    return violations


class FhvhvDataCleaning(BaseDataCleaning):
    """Data cleaning for FHVHV taxi data.

    FHVHV has partial fare data: trip_miles, trip_time, base_passenger_fare,
    tips, and driver_pay are used for outlier detection. Quality rules check
    negative fares, zero distances, impossible durations, and negative trip time.
    """

    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run data cleaning on FHVHV taxi dataframe.

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
            "hvfhs_license_num": ["HV0003", "HV0003", "HV0003", "HV0004", "HV0003"],
            "trip_miles": [2.0, 0.0, 5.0, 100.0, 3.0],
            "trip_time": [600, -10, 1200, 50000, 900],
            "base_passenger_fare": [10.0, 20.0, -5.0, 500.0, 15.0],
            "tips": [2.0, 0.0, 1.0, 50.0, 3.0],
            "driver_pay": [8.0, 16.0, -4.0, 400.0, 12.0],
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
        }
    )
    result = FhvhvDataCleaning().analyze(df=sample)
    print(f"Summary: {result.summary_data}")
