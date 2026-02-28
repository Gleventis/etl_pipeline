"""Shared utilities for data cleaning across taxi types."""

import io

import numpy as np
import polars as pl
import pyarrow.parquet as pq
from scipy import stats
from sklearn.ensemble import IsolationForest

from src.server.models import StepResult

IQR_MULTIPLIER = 1.5
ZSCORE_THRESHOLD = 3.0
ISOLATION_FOREST_CONTAMINATION = 0.05


def detect_iqr(arr: np.ndarray) -> np.ndarray:
    """Return boolean mask where True means outlier via IQR method."""
    q1 = np.percentile(a=arr, q=25)
    q3 = np.percentile(a=arr, q=75)
    iqr = q3 - q1
    lower = q1 - IQR_MULTIPLIER * iqr
    upper = q3 + IQR_MULTIPLIER * iqr
    return (arr < lower) | (arr > upper)


def detect_zscore(arr: np.ndarray) -> np.ndarray:
    """Return boolean mask where True means outlier via Z-score method."""
    z = np.abs(stats.zscore(a=arr))
    return z > ZSCORE_THRESHOLD


def detect_isolation_forest(arr: np.ndarray) -> np.ndarray:
    """Return boolean mask where True means outlier via Isolation Forest."""
    model = IsolationForest(
        contamination=ISOLATION_FOREST_CONTAMINATION,
        random_state=42,
    )
    labels = model.fit_predict(X=arr.reshape(-1, 1))
    return labels == -1


def cap_outliers(arr: np.ndarray) -> np.ndarray:
    """Cap outliers at IQR bounds instead of removing them."""
    q1 = np.percentile(a=arr, q=25)
    q3 = np.percentile(a=arr, q=75)
    iqr = q3 - q1
    lower = q1 - IQR_MULTIPLIER * iqr
    upper = q3 + IQR_MULTIPLIER * iqr
    return np.clip(a=arr, a_min=lower, a_max=upper)


def run_outlier_detection(
    df: pl.DataFrame,
    columns: list[str],
) -> dict[str, dict[str, int]]:
    """Run IQR, Z-score, and Isolation Forest on each column."""
    outlier_counts: dict[str, dict[str, int]] = {}
    for col in columns:
        arr = df[col].drop_nulls().to_numpy().astype(np.float64)
        if len(arr) < 2:
            continue
        outlier_counts[col] = {
            "iqr": int(detect_iqr(arr=arr).sum()),
            "zscore": int(detect_zscore(arr=arr).sum()),
            "isolation_forest": int(detect_isolation_forest(arr=arr).sum()),
        }
    return outlier_counts


def apply_removal_strategy(
    df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    """Remove rows flagged as outliers by IQR on any column."""
    mask = pl.lit(value=True)
    for col in columns:
        arr = df[col].drop_nulls().to_numpy().astype(np.float64)
        if len(arr) < 2:
            continue
        iqr_mask = detect_iqr(arr=arr)
        mask = mask & ~pl.Series(name=col, values=iqr_mask)
    return df.filter(mask)


def apply_capping_strategy(
    df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    """Cap outlier values at IQR bounds."""
    capped = df.clone()
    for col in columns:
        if col not in capped.columns:
            continue
        arr = capped[col].drop_nulls().to_numpy().astype(np.float64)
        if len(arr) < 2:
            continue
        capped_arr = cap_outliers(arr=arr)
        capped = capped.with_columns(pl.Series(name=col, values=capped_arr.tolist()))
    return capped


def build_step_result(
    df: pl.DataFrame,
    cleaned_df: pl.DataFrame,
    capped_df: pl.DataFrame,
    columns: list[str],
    outlier_counts: dict[str, dict[str, int]],
    quality_violations: dict[str, int],
) -> StepResult:
    """Build the StepResult from cleaning outputs."""
    summary_data = {
        "outlier_counts": outlier_counts,
        "quality_violations": quality_violations,
        "strategy_comparison": {
            "removal": {
                "rows_before": df.height,
                "rows_after": cleaned_df.height,
                "rows_removed": df.height - cleaned_df.height,
            },
            "capping": {
                "rows_before": df.height,
                "rows_after": capped_df.height,
                "columns_capped": len(columns),
            },
        },
        "num_rows": df.height,
        "num_outlier_columns": len(columns),
    }

    buf = io.BytesIO()
    table = cleaned_df.to_arrow()
    pq.write_table(table=table, where=buf)

    return StepResult(
        summary_data=summary_data,
        detail_bytes=buf.getvalue(),
        detail_s3_key="data_cleaning_detail.parquet",
    )


if __name__ == "__main__":
    arr = np.array([1.0, 2.0, 3.0, 100.0, 2.5])
    print(f"IQR outliers: {detect_iqr(arr=arr).sum()}")
    print(f"Z-score outliers: {detect_zscore(arr=arr).sum()}")
