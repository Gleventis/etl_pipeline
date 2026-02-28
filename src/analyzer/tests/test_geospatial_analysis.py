"""Tests for geospatial analysis implementations across all taxi types."""

import io
import json

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from src.services.fhv.geospatial_analysis import FhvGeospatialAnalysis
from src.services.fhvhv.geospatial_analysis import FhvhvGeospatialAnalysis
from src.services.green.geospatial_analysis import GreenGeospatialAnalysis
from src.services.yellow.geospatial_analysis import YellowGeospatialAnalysis

_EXPECTED_SUMMARY_KEYS = {
    "num_rows",
    "num_zones",
    "top_routes",
    "dbscan_n_clusters",
    "dbscan_n_noise",
    "kmeans_n_clusters",
    "kmeans_centers",
}


def _make_yellow_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=20.0, size=n).tolist(),
        }
    )


def _make_green_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=20.0, size=n).tolist(),
        }
    )


def _make_fhv_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
        }
    )


def _make_fhvhv_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "trip_miles": rng.uniform(low=0.5, high=30.0, size=n).tolist(),
        }
    )


def _read_detail_parquet(detail_bytes: bytes) -> dict[str, str]:
    """Read detail parquet and return column name → value mapping."""
    table = pq.read_table(source=io.BytesIO(detail_bytes))
    return {col: table.column(col)[0].as_py() for col in table.column_names}


class TestYellowGeospatialAnalysis:
    """Tests for yellow taxi geospatial analysis."""

    def test_summary_data_structure(self) -> None:
        df = _make_yellow_df()
        result = YellowGeospatialAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_dbscan_cluster_assignments(self) -> None:
        df = _make_yellow_df()
        result = YellowGeospatialAnalysis().analyze(df=df)

        assert isinstance(result.summary_data["dbscan_n_clusters"], int)
        assert isinstance(result.summary_data["dbscan_n_noise"], int)
        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        labels = json.loads(detail["dbscan_labels"])
        assert isinstance(labels, list)
        assert len(labels) > 0

    def test_kmeans_cluster_assignments(self) -> None:
        df = _make_yellow_df()
        result = YellowGeospatialAnalysis().analyze(df=df)

        assert result.summary_data["kmeans_n_clusters"] > 0
        assert (
            len(result.summary_data["kmeans_centers"])
            == result.summary_data["kmeans_n_clusters"]
        )
        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        labels = json.loads(detail["kmeans_labels"])
        zones = json.loads(detail["kmeans_zones"])
        assert len(labels) == len(zones)

    def test_top_route_counts(self) -> None:
        df = _make_yellow_df()
        result = YellowGeospatialAnalysis().analyze(df=df)

        routes = result.summary_data["top_routes"]
        assert isinstance(routes, list)
        assert len(routes) > 0
        assert "pickup_zone" in routes[0]
        assert "dropoff_zone" in routes[0]
        assert "trip_count" in routes[0]

    def test_zone_heatmap_data(self) -> None:
        df = _make_yellow_df()
        result = YellowGeospatialAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        heatmap = json.loads(detail["heatmap"])
        assert isinstance(heatmap, list)
        assert len(heatmap) > 0
        assert "zone" in heatmap[0]
        assert "trip_count" in heatmap[0]

    def test_distance_by_zone(self) -> None:
        df = _make_yellow_df()
        result = YellowGeospatialAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        distance = json.loads(detail["distance_by_zone"])
        assert isinstance(distance, list)
        assert len(distance) > 0
        assert "zone" in distance[0]
        assert "mean_distance" in distance[0]

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_yellow_df()
        result = YellowGeospatialAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1

    def test_detail_s3_key(self) -> None:
        df = _make_yellow_df()
        result = YellowGeospatialAnalysis().analyze(df=df)

        assert result.detail_s3_key == "geospatial_analysis_detail.parquet"


class TestGreenGeospatialAnalysis:
    """Tests for green taxi geospatial analysis."""

    def test_summary_data_structure(self) -> None:
        df = _make_green_df()
        result = GreenGeospatialAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_top_route_counts(self) -> None:
        df = _make_green_df()
        result = GreenGeospatialAnalysis().analyze(df=df)

        routes = result.summary_data["top_routes"]
        assert isinstance(routes, list)
        assert len(routes) > 0

    def test_distance_by_zone(self) -> None:
        df = _make_green_df()
        result = GreenGeospatialAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        distance = json.loads(detail["distance_by_zone"])
        assert isinstance(distance, list)
        assert len(distance) > 0

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_green_df()
        result = GreenGeospatialAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1


class TestFhvGeospatialAnalysis:
    """Tests for FHV taxi geospatial analysis — no distance distribution."""

    def test_summary_data_structure(self) -> None:
        df = _make_fhv_df()
        result = FhvGeospatialAnalysis().analyze(df=df)

        expected = _EXPECTED_SUMMARY_KEYS | {"distance_distribution_skipped"}
        assert set(result.summary_data.keys()) == expected

    def test_no_distance_distribution(self) -> None:
        df = _make_fhv_df()
        result = FhvGeospatialAnalysis().analyze(df=df)

        assert result.summary_data["distance_distribution_skipped"] is True
        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        assert "distance_by_zone" not in detail

    def test_dbscan_cluster_assignments(self) -> None:
        df = _make_fhv_df()
        result = FhvGeospatialAnalysis().analyze(df=df)

        assert isinstance(result.summary_data["dbscan_n_clusters"], int)

    def test_top_route_counts(self) -> None:
        df = _make_fhv_df()
        result = FhvGeospatialAnalysis().analyze(df=df)

        routes = result.summary_data["top_routes"]
        assert isinstance(routes, list)
        assert len(routes) > 0

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_fhv_df()
        result = FhvGeospatialAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1


class TestFhvhvGeospatialAnalysis:
    """Tests for FHVHV taxi geospatial analysis — uses trip_miles for distance."""

    def test_summary_data_structure(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvGeospatialAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_distance_uses_trip_miles(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvGeospatialAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        distance = json.loads(detail["distance_by_zone"])
        assert isinstance(distance, list)
        assert len(distance) > 0
        assert "mean_distance" in distance[0]

    def test_top_route_counts(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvGeospatialAnalysis().analyze(df=df)

        routes = result.summary_data["top_routes"]
        assert isinstance(routes, list)
        assert len(routes) > 0

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvGeospatialAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1


class TestEdgeCases:
    """Edge case tests for geospatial analysis."""

    def test_empty_dataframe_yellow(self) -> None:
        df = _make_yellow_df().head(n=0)
        result = YellowGeospatialAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""

    def test_empty_dataframe_fhv(self) -> None:
        df = _make_fhv_df().head(n=0)
        result = FhvGeospatialAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True

    def test_empty_dataframe_fhvhv(self) -> None:
        df = _make_fhvhv_df().head(n=0)
        result = FhvhvGeospatialAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True

    def test_single_row(self) -> None:
        df = _make_yellow_df().head(n=1)
        result = YellowGeospatialAnalysis().analyze(df=df)

        assert result.summary_data["num_rows"] == 1
        assert result.summary_data["num_zones"] == 1

    def test_single_zone(self) -> None:
        """All trips from/to the same zone."""
        df = pl.DataFrame(
            {
                "pulocationid": [100] * 50,
                "dolocationid": [100] * 50,
                "trip_distance": [5.0] * 50,
            }
        )
        result = YellowGeospatialAnalysis().analyze(df=df)

        assert result.summary_data["num_zones"] == 1
        assert result.summary_data["kmeans_n_clusters"] == 1
        routes = result.summary_data["top_routes"]
        assert len(routes) == 1
        assert routes[0]["pickup_zone"] == 100
        assert routes[0]["dropoff_zone"] == 100

    def test_missing_zone_columns(self) -> None:
        df = pl.DataFrame({"some_col": [1, 2, 3]})
        result = YellowGeospatialAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""
