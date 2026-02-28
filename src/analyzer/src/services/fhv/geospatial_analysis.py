"""FHV taxi geospatial analysis implementation."""

import io
import json
import logging

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.cluster import DBSCAN, KMeans

from src.server.models import StepResult
from src.services.base.geospatial_analysis import BaseGeospatialAnalysis

logger = logging.getLogger(__name__)

_PU_COL = "pulocationid"
_DO_COL = "dolocationid"
_TOP_N_ROUTES = 20


def _zone_trip_counts(df: pl.DataFrame) -> pl.DataFrame:
    """Count trips per pickup zone."""
    return (
        df.group_by(_PU_COL)
        .agg(pl.len().alias("trip_count"))
        .sort("trip_count", descending=True)
    )


def _route_counts(df: pl.DataFrame, top_n: int = _TOP_N_ROUTES) -> list[dict]:
    """Find top N most common pickup-dropoff zone pairs."""
    routes = (
        df.group_by([_PU_COL, _DO_COL])
        .agg(pl.len().alias("trip_count"))
        .sort("trip_count", descending=True)
        .head(top_n)
    )
    return [
        {
            "pickup_zone": int(row[_PU_COL]),
            "dropoff_zone": int(row[_DO_COL]),
            "trip_count": int(row["trip_count"]),
        }
        for row in routes.iter_rows(named=True)
    ]


def _dbscan_clusters(df: pl.DataFrame) -> dict:
    """DBSCAN clustering on pickup-dropoff zone pairs."""
    pairs = df.select([_PU_COL, _DO_COL]).drop_nulls()
    if pairs.height < 2:
        return {"n_clusters": 0, "n_noise": 0, "labels": []}

    features = pairs.to_numpy().astype(np.float64)
    model = DBSCAN(eps=5.0, min_samples=10)
    labels = model.fit_predict(X=features)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = int(np.sum(labels == -1))

    return {
        "n_clusters": n_clusters,
        "n_noise": n_noise,
        "labels": labels.tolist(),
    }


def _kmeans_clusters(zone_counts: pl.DataFrame, n_clusters: int = 5) -> dict:
    """K-means clustering on zone trip volumes."""
    if zone_counts.height < n_clusters:
        n_clusters = max(1, zone_counts.height)

    features = zone_counts["trip_count"].to_numpy().reshape(-1, 1).astype(np.float64)
    model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = model.fit_predict(X=features)

    return {
        "n_clusters": n_clusters,
        "centers": model.cluster_centers_.flatten().tolist(),
        "labels": labels.tolist(),
        "zones": zone_counts[_PU_COL].to_list(),
    }


class FhvGeospatialAnalysis(BaseGeospatialAnalysis):
    """Geospatial analysis for FHV taxi data.

    FHV data has zone IDs but no trip_distance, so distance distribution is skipped.
    """

    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Run geospatial analysis on FHV taxi dataframe.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with cluster metadata, top routes, zone heatmap, and detail parquet.
        """
        if df.height == 0 or _PU_COL not in df.columns or _DO_COL not in df.columns:
            return StepResult(
                summary_data={
                    "skipped": True,
                    "reason": "empty or missing zone columns",
                },
                detail_bytes=b"",
                detail_s3_key="geospatial_analysis_detail.parquet",
            )

        zone_counts = _zone_trip_counts(df=df)
        top_routes = _route_counts(df=df)
        dbscan = _dbscan_clusters(df=df)
        kmeans = _kmeans_clusters(zone_counts=zone_counts)

        heatmap = [
            {"zone": int(row[_PU_COL]), "trip_count": int(row["trip_count"])}
            for row in zone_counts.iter_rows(named=True)
        ]

        summary_data = {
            "num_rows": df.height,
            "num_zones": zone_counts.height,
            "top_routes": top_routes,
            "dbscan_n_clusters": dbscan["n_clusters"],
            "dbscan_n_noise": dbscan["n_noise"],
            "kmeans_n_clusters": kmeans["n_clusters"],
            "kmeans_centers": kmeans["centers"],
            "distance_distribution_skipped": True,
        }

        detail = {
            "heatmap": json.dumps(heatmap),
            "dbscan_labels": json.dumps(dbscan["labels"]),
            "kmeans_labels": json.dumps(kmeans["labels"]),
            "kmeans_zones": json.dumps(kmeans["zones"]),
            "top_routes": json.dumps(top_routes),
        }
        table = pa.table({k: [v] for k, v in detail.items()})
        buf = io.BytesIO()
        pq.write_table(table=table, where=buf)

        return StepResult(
            summary_data=summary_data,
            detail_bytes=buf.getvalue(),
            detail_s3_key="geospatial_analysis_detail.parquet",
        )


if __name__ == "__main__":
    rng = np.random.default_rng(seed=42)
    n = 200
    sample = pl.DataFrame(
        {
            _PU_COL: rng.integers(low=1, high=265, size=n).tolist(),
            _DO_COL: rng.integers(low=1, high=265, size=n).tolist(),
        }
    )
    result = FhvGeospatialAnalysis().analyze(df=sample)
    print(f"Summary: {result.summary_data}")
