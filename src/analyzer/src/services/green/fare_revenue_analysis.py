"""Green taxi fare revenue analysis implementation."""

import io
import json
import logging

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from scipy import stats
from sklearn.linear_model import LinearRegression

from src.server.models import StepResult
from src.services.base.fare_revenue_analysis import BaseFareRevenueAnalysis

logger = logging.getLogger(__name__)

_FARE_COL = "fare_amount"
_TOTAL_COL = "total_amount"
_TIP_COL = "tip_amount"
_DIST_COL = "trip_distance"
_PU_COL = "pulocationid"
_PICKUP_COL = "lpep_pickup_datetime"
_DROPOFF_COL = "lpep_dropoff_datetime"
_SURCHARGE_COLS = [
    "extra",
    "mta_tax",
    "ehail_fee",
    "improvement_surcharge",
    "congestion_surcharge",
]


def _daily_revenue(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate daily total revenue."""
    return (
        df.with_columns(pl.col(_PICKUP_COL).cast(pl.Datetime).alias("pickup_dt"))
        .with_columns(pl.col("pickup_dt").dt.truncate("1d").alias("day"))
        .group_by("day")
        .agg(pl.col(_TOTAL_COL).sum().alias("daily_revenue"))
        .sort("day")
    )


def _revenue_forecast(daily: pl.DataFrame) -> dict:
    """Linear regression on daily revenue time-series."""
    if daily.height < 2:
        return {"slope": 0.0, "intercept": 0.0, "r_squared": 0.0, "predictions": []}

    x = np.arange(daily.height).reshape(-1, 1)
    y = daily["daily_revenue"].to_numpy().astype(np.float64)

    model = LinearRegression()
    model.fit(X=x, y=y)
    predictions = model.predict(X=x).tolist()

    return {
        "slope": float(model.coef_[0]),
        "intercept": float(model.intercept_),
        "r_squared": float(model.score(X=x, y=y)),
        "predictions": predictions,
    }


def _fare_anomalies(df: pl.DataFrame) -> dict[str, int]:
    """Z-score anomaly detection on fare columns."""
    anomaly_counts: dict[str, int] = {}
    for col in [_FARE_COL, _TOTAL_COL]:
        if col not in df.columns:
            continue
        values = df[col].drop_nulls().to_numpy().astype(np.float64)
        if len(values) < 2:
            anomaly_counts[col] = 0
            continue
        z_scores = np.abs(stats.zscore(a=values))
        anomaly_counts[col] = int(np.sum(z_scores > 3))
    return anomaly_counts


def _tip_prediction(df: pl.DataFrame) -> dict:
    """Regression to predict tip_amount from distance, duration, fare."""
    required = [_TIP_COL, _DIST_COL, _FARE_COL, _PICKUP_COL, _DROPOFF_COL]
    if not all(c in df.columns for c in required):
        return {"skipped": True, "reason": "missing required columns"}

    subset = df.select(required).drop_nulls()
    if subset.height < 2:
        return {"skipped": True, "reason": "insufficient data"}

    pickup = subset[_PICKUP_COL].cast(pl.Datetime)
    dropoff = subset[_DROPOFF_COL].cast(pl.Datetime)
    duration_s = (dropoff - pickup).dt.total_seconds().to_numpy().astype(np.float64)

    features = np.column_stack(
        [
            subset[_DIST_COL].to_numpy().astype(np.float64),
            duration_s,
            subset[_FARE_COL].to_numpy().astype(np.float64),
        ]
    )
    target = subset[_TIP_COL].to_numpy().astype(np.float64)

    model = LinearRegression()
    model.fit(X=features, y=target)

    return {
        "coefficients": {
            "distance": float(model.coef_[0]),
            "duration": float(model.coef_[1]),
            "fare": float(model.coef_[2]),
        },
        "intercept": float(model.intercept_),
        "r_squared": float(model.score(X=features, y=target)),
    }


def _fare_distribution(df: pl.DataFrame) -> dict:
    """Fare distribution by zone, time-of-day, and distance bucket."""
    result: dict[str, list[dict]] = {}

    # by zone
    if _PU_COL in df.columns and _FARE_COL in df.columns:
        by_zone = (
            df.group_by(_PU_COL)
            .agg(
                pl.col(_FARE_COL).mean().alias("mean_fare"),
                pl.col(_FARE_COL).median().alias("median_fare"),
                pl.len().alias("trip_count"),
            )
            .sort("trip_count", descending=True)
        )
        result["by_zone"] = [
            {
                "zone": int(row[_PU_COL]),
                "mean_fare": float(row["mean_fare"])
                if row["mean_fare"] is not None
                else 0.0,
                "median_fare": float(row["median_fare"])
                if row["median_fare"] is not None
                else 0.0,
                "trip_count": int(row["trip_count"]),
            }
            for row in by_zone.iter_rows(named=True)
        ]

    # by time-of-day
    if _PICKUP_COL in df.columns and _FARE_COL in df.columns:
        by_hour = (
            df.with_columns(
                pl.col(_PICKUP_COL).cast(pl.Datetime).dt.hour().alias("hour_of_day")
            )
            .group_by("hour_of_day")
            .agg(
                pl.col(_FARE_COL).mean().alias("mean_fare"),
                pl.len().alias("trip_count"),
            )
            .sort("hour_of_day")
        )
        result["by_time_of_day"] = [
            {
                "hour": int(row["hour_of_day"]),
                "mean_fare": float(row["mean_fare"])
                if row["mean_fare"] is not None
                else 0.0,
                "trip_count": int(row["trip_count"]),
            }
            for row in by_hour.iter_rows(named=True)
        ]

    # by distance bucket
    if _DIST_COL in df.columns and _FARE_COL in df.columns:
        buckets = [0, 1, 2, 5, 10, 20, 50, float("inf")]
        labels = ["0-1", "1-2", "2-5", "5-10", "10-20", "20-50", "50+"]
        by_dist = (
            df.with_columns(
                pl.col(_DIST_COL)
                .cut(breaks=buckets[1:-1], labels=labels)
                .alias("distance_bucket")
            )
            .group_by("distance_bucket")
            .agg(
                pl.col(_FARE_COL).mean().alias("mean_fare"),
                pl.len().alias("trip_count"),
            )
            .sort("trip_count", descending=True)
        )
        result["by_distance_bucket"] = [
            {
                "bucket": str(row["distance_bucket"]),
                "mean_fare": float(row["mean_fare"])
                if row["mean_fare"] is not None
                else 0.0,
                "trip_count": int(row["trip_count"]),
            }
            for row in by_dist.iter_rows(named=True)
        ]

    return result


def _surcharge_breakdown(df: pl.DataFrame) -> dict[str, float]:
    """Total and mean for each surcharge column."""
    breakdown: dict[str, float] = {}
    for col in _SURCHARGE_COLS:
        if col not in df.columns:
            continue
        series = df[col].drop_nulls()
        breakdown[f"{col}_total"] = float(series.sum())
        breakdown[f"{col}_mean"] = float(series.mean()) if series.len() > 0 else 0.0
    return breakdown


class GreenFareRevenueAnalysis(BaseFareRevenueAnalysis):
    """Fare revenue analysis for green taxi data."""

    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run fare revenue analysis on green taxi dataframe.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with revenue summary, detail parquet, and s3 key.
        """
        if df.height == 0 or _FARE_COL not in df.columns:
            return StepResult(
                summary_data={
                    "skipped": True,
                    "reason": "empty or missing fare columns",
                },
                detail_bytes=b"",
                detail_s3_key="fare_revenue_analysis_detail.parquet",
            )

        daily = _daily_revenue(df=df)
        forecast = _revenue_forecast(daily=daily)
        anomalies = _fare_anomalies(df=df)
        tip_pred = _tip_prediction(df=df)
        distribution = _fare_distribution(df=df)
        surcharges = _surcharge_breakdown(df=df)

        summary_data = {
            "num_rows": df.height,
            "num_days": daily.height,
            "forecast_slope": forecast["slope"],
            "forecast_r_squared": forecast["r_squared"],
            "anomaly_counts": anomalies,
            "tip_prediction_r_squared": tip_pred.get("r_squared"),
            "surcharge_breakdown": surcharges,
        }

        detail = {
            "forecast": json.dumps(forecast),
            "anomalies": json.dumps(anomalies),
            "tip_prediction": json.dumps(tip_pred),
            "distribution": json.dumps(distribution),
            "surcharges": json.dumps(surcharges),
        }
        table = pa.table({k: [v] for k, v in detail.items()})
        buf = io.BytesIO()
        pq.write_table(table=table, where=buf)

        return StepResult(
            summary_data=summary_data,
            detail_bytes=buf.getvalue(),
            detail_s3_key="fare_revenue_analysis_detail.parquet",
        )


if __name__ == "__main__":
    rng = np.random.default_rng(seed=42)
    n = 200
    sample = pl.DataFrame(
        {
            _PICKUP_COL: [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:15:00" for i in range(n)
            ],
            _DROPOFF_COL: [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:45:00" for i in range(n)
            ],
            _FARE_COL: rng.uniform(low=5.0, high=60.0, size=n).tolist(),
            _TOTAL_COL: rng.uniform(low=8.0, high=80.0, size=n).tolist(),
            _TIP_COL: rng.uniform(low=0.0, high=15.0, size=n).tolist(),
            _DIST_COL: rng.uniform(low=0.5, high=25.0, size=n).tolist(),
            _PU_COL: rng.integers(low=1, high=265, size=n).tolist(),
            "extra": rng.choice(a=[0.0, 0.5, 1.0], size=n).tolist(),
            "mta_tax": [0.5] * n,
            "ehail_fee": rng.choice(a=[0.0, 1.0], size=n).tolist(),
            "improvement_surcharge": [0.3] * n,
            "congestion_surcharge": rng.choice(a=[0.0, 2.5], size=n).tolist(),
        }
    )
    result = GreenFareRevenueAnalysis().analyze(df=sample)
    print(f"Summary: {result.summary_data}")
