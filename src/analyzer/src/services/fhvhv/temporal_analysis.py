"""FHVHV taxi temporal analysis implementation."""

import io
import json
import logging

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from src.server.models import StepResult
from src.services.base.temporal_analysis import BaseTemporalAnalysis

logger = logging.getLogger(__name__)

_PICKUP_COL = "pickup_datetime"
_FARE_COL = "base_passenger_fare"
_REQUEST_COL = "request_datetime"
_ON_SCENE_COL = "on_scene_datetime"


def _build_hourly_series(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate trip counts and fare averages by hour."""
    return (
        df.with_columns(pl.col(_PICKUP_COL).cast(pl.Datetime).alias("pickup_dt"))
        .with_columns(pl.col("pickup_dt").dt.truncate("1h").alias("hour"))
        .group_by("hour")
        .agg(
            pl.len().alias("trip_count"),
            pl.col(_FARE_COL).mean().alias("avg_fare")
            if _FARE_COL in df.columns
            else pl.lit(value=None).alias("avg_fare"),
        )
        .sort("hour")
    )


def _decompose(values: np.ndarray, period: int) -> dict[str, list[float]]:
    """Simple additive decomposition: trend (moving average) + seasonal + residual."""
    n = len(values)
    if n < period * 2:
        return {"trend": values.tolist(), "seasonal": [0.0] * n, "residual": [0.0] * n}

    kernel = np.ones(shape=period) / period
    trend = np.convolve(values, kernel, mode="same")

    detrended = values - trend
    seasonal = np.zeros(shape=n)
    for i in range(period):
        indices = np.arange(start=i, stop=n, step=period)
        seasonal[indices] = detrended[indices].mean()

    residual = values - trend - seasonal
    return {
        "trend": trend.tolist(),
        "seasonal": seasonal.tolist(),
        "residual": residual.tolist(),
    }


def _fourier_top_frequencies(
    values: np.ndarray, top_n: int = 5
) -> list[dict[str, float]]:
    """Return top N frequencies by magnitude from FFT."""
    fft_vals = np.fft.rfft(a=values)
    magnitudes = np.abs(fft_vals)
    freqs = np.fft.rfftfreq(n=len(values))

    if len(magnitudes) > 1:
        indices = np.argsort(a=magnitudes[1:])[::-1][:top_n] + 1
    else:
        return []

    return [
        {"frequency": float(freqs[i]), "magnitude": float(magnitudes[i])}
        for i in indices
    ]


def _rolling_stats(hourly: pl.DataFrame) -> dict[str, list[dict[str, float | int]]]:
    """Compute rolling window statistics at hourly, daily, weekly granularity."""
    result: dict[str, list[dict[str, float | int]]] = {}

    for window_name, window_hours in [("hourly", 1), ("daily", 24), ("weekly", 168)]:
        if hourly.height < window_hours:
            result[window_name] = []
            continue

        rolled = hourly.with_columns(
            pl.col("trip_count")
            .rolling_mean(window_size=window_hours)
            .alias("rolling_mean_trips"),
            pl.col("trip_count")
            .rolling_std(window_size=window_hours)
            .alias("rolling_std_trips"),
        ).drop_nulls(subset=["rolling_mean_trips"])

        result[window_name] = [
            {
                "hour": row["hour"].isoformat()
                if hasattr(row["hour"], "isoformat")
                else str(row["hour"]),
                "rolling_mean_trips": float(row["rolling_mean_trips"]),
                "rolling_std_trips": float(row["rolling_std_trips"])
                if row["rolling_std_trips"] is not None
                else 0.0,
            }
            for row in rolled.iter_rows(named=True)
        ]

    return result


def _detect_peak_hours(hourly: pl.DataFrame) -> list[int]:
    """Detect peak hours (hour-of-day with above-average trip counts)."""
    by_hour_of_day = (
        hourly.with_columns(
            pl.col("hour").dt.hour().alias("hour_of_day"),
        )
        .group_by("hour_of_day")
        .agg(pl.col("trip_count").mean().alias("mean_trips"))
        .sort("hour_of_day")
    )

    if by_hour_of_day.height == 0:
        return []

    overall_mean = by_hour_of_day["mean_trips"].mean()
    peaks = by_hour_of_day.filter(pl.col("mean_trips") > overall_mean)
    return peaks["hour_of_day"].to_list()


def _compute_wait_times(df: pl.DataFrame) -> dict[str, float | None]:
    """Compute wait time statistics from request → on_scene → pickup.

    Args:
        df: Input dataframe with datetime columns.

    Returns:
        Dict with mean/median wait times in seconds, or None if columns missing.
    """
    has_request = _REQUEST_COL in df.columns
    has_on_scene = _ON_SCENE_COL in df.columns
    has_pickup = _PICKUP_COL in df.columns

    if not has_pickup:
        return {}

    result: dict[str, float | None] = {}

    if has_request:
        request_to_pickup = (
            df.filter(
                pl.col(_REQUEST_COL).is_not_null() & pl.col(_PICKUP_COL).is_not_null()
            )
            .with_columns(
                (
                    pl.col(_PICKUP_COL).cast(pl.Datetime)
                    - pl.col(_REQUEST_COL).cast(pl.Datetime)
                )
                .dt.total_seconds()
                .alias("request_to_pickup_s")
            )
            .filter(pl.col("request_to_pickup_s") >= 0)
        )
        if request_to_pickup.height > 0:
            col = request_to_pickup["request_to_pickup_s"]
            result["mean_request_to_pickup_s"] = float(col.mean())
            result["median_request_to_pickup_s"] = float(col.median())
        else:
            result["mean_request_to_pickup_s"] = None
            result["median_request_to_pickup_s"] = None

    if has_request and has_on_scene:
        request_to_scene = (
            df.filter(
                pl.col(_REQUEST_COL).is_not_null() & pl.col(_ON_SCENE_COL).is_not_null()
            )
            .with_columns(
                (
                    pl.col(_ON_SCENE_COL).cast(pl.Datetime)
                    - pl.col(_REQUEST_COL).cast(pl.Datetime)
                )
                .dt.total_seconds()
                .alias("request_to_scene_s")
            )
            .filter(pl.col("request_to_scene_s") >= 0)
        )
        if request_to_scene.height > 0:
            col = request_to_scene["request_to_scene_s"]
            result["mean_request_to_scene_s"] = float(col.mean())
            result["median_request_to_scene_s"] = float(col.median())
        else:
            result["mean_request_to_scene_s"] = None
            result["median_request_to_scene_s"] = None

    if has_on_scene:
        scene_to_pickup = (
            df.filter(
                pl.col(_ON_SCENE_COL).is_not_null() & pl.col(_PICKUP_COL).is_not_null()
            )
            .with_columns(
                (
                    pl.col(_PICKUP_COL).cast(pl.Datetime)
                    - pl.col(_ON_SCENE_COL).cast(pl.Datetime)
                )
                .dt.total_seconds()
                .alias("scene_to_pickup_s")
            )
            .filter(pl.col("scene_to_pickup_s") >= 0)
        )
        if scene_to_pickup.height > 0:
            col = scene_to_pickup["scene_to_pickup_s"]
            result["mean_scene_to_pickup_s"] = float(col.mean())
            result["median_scene_to_pickup_s"] = float(col.median())
        else:
            result["mean_scene_to_pickup_s"] = None
            result["median_scene_to_pickup_s"] = None

    return result


class FhvhvTemporalAnalysis(BaseTemporalAnalysis):
    """Temporal analysis for FHVHV taxi data.

    Includes fare-based aggregations and wait time analysis
    (request → on_scene → pickup).
    """

    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run temporal analysis on FHVHV taxi dataframe.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with temporal patterns summary, detail parquet, and s3 key.
        """
        if df.height == 0 or _PICKUP_COL not in df.columns:
            return StepResult(
                summary_data={
                    "skipped": True,
                    "reason": "empty or missing pickup column",
                },
                detail_bytes=b"",
                detail_s3_key="temporal_analysis_detail.parquet",
            )

        hourly = _build_hourly_series(df=df)
        trip_counts = hourly["trip_count"].to_numpy().astype(np.float64)

        decomposition = _decompose(values=trip_counts, period=24)
        top_frequencies = _fourier_top_frequencies(values=trip_counts)
        rolling = _rolling_stats(hourly=hourly)
        peak_hours = _detect_peak_hours(hourly=hourly)
        wait_times = _compute_wait_times(df=df)

        summary_data = {
            "num_rows": df.height,
            "num_hours": hourly.height,
            "peak_hours": peak_hours,
            "top_frequencies": top_frequencies,
            "decomposition_length": len(decomposition["trend"]),
            "wait_times": wait_times,
        }

        detail = {
            "decomposition": json.dumps(decomposition),
            "rolling_stats": json.dumps(rolling),
            "top_frequencies": json.dumps(top_frequencies),
            "wait_times": json.dumps(wait_times),
        }
        table = pa.table({k: [v] for k, v in detail.items()})
        buf = io.BytesIO()
        pq.write_table(table=table, where=buf)

        return StepResult(
            summary_data=summary_data,
            detail_bytes=buf.getvalue(),
            detail_s3_key="temporal_analysis_detail.parquet",
        )


if __name__ == "__main__":
    rng = np.random.default_rng(seed=42)
    n = 500
    base = pl.Series(
        name=_PICKUP_COL,
        values=[f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:00:00" for i in range(n)],
    )
    request_times = pl.Series(
        name=_REQUEST_COL,
        values=[
            f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:{rng.integers(low=0, high=30):02d}:00"
            for i in range(n)
        ],
    )
    sample = pl.DataFrame(
        {
            _REQUEST_COL: request_times,
            _ON_SCENE_COL: base,
            _PICKUP_COL: base,
            "dropoff_datetime": base,
            _FARE_COL: rng.uniform(low=5.0, high=50.0, size=n).tolist(),
        }
    )
    result = FhvhvTemporalAnalysis().analyze(df=sample)
    print(f"Summary: {result.summary_data}")
