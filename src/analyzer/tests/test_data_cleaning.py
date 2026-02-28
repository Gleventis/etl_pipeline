"""Tests for data cleaning implementations across all taxi types."""

import io

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from src.services.fhv.data_cleaning import FhvDataCleaning
from src.services.fhvhv.data_cleaning import FhvhvDataCleaning
from src.services.green.data_cleaning import GreenDataCleaning
from src.services.yellow.data_cleaning import YellowDataCleaning


def _make_yellow_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "vendorid": rng.integers(low=1, high=3, size=n).tolist(),
            "tpep_pickup_datetime": ["2023-01-01T00:00:00"] * n,
            "tpep_dropoff_datetime": ["2023-01-01T00:30:00"] * n,
            "passenger_count": rng.integers(low=1, high=6, size=n).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=30.0, size=n).tolist(),
            "fare_amount": rng.uniform(low=2.5, high=100.0, size=n).tolist(),
            "total_amount": rng.uniform(low=5.0, high=130.0, size=n).tolist(),
            "tip_amount": rng.uniform(low=0.0, high=20.0, size=n).tolist(),
            "tolls_amount": rng.uniform(low=0.0, high=10.0, size=n).tolist(),
        }
    )


def _make_green_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "vendorid": rng.integers(low=1, high=3, size=n).tolist(),
            "lpep_pickup_datetime": ["2023-01-01T00:00:00"] * n,
            "lpep_dropoff_datetime": ["2023-01-01T00:30:00"] * n,
            "passenger_count": rng.integers(low=1, high=6, size=n).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=30.0, size=n).tolist(),
            "fare_amount": rng.uniform(low=2.5, high=100.0, size=n).tolist(),
            "total_amount": rng.uniform(low=5.0, high=130.0, size=n).tolist(),
            "tip_amount": rng.uniform(low=0.0, high=20.0, size=n).tolist(),
            "tolls_amount": rng.uniform(low=0.0, high=10.0, size=n).tolist(),
            "ehail_fee": [None] * n,
        }
    )


def _make_fhv_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "dispatching_base_num": ["B00001"] * n,
            "pickup_datetime": ["2023-01-01T00:00:00"] * n,
            "dropoff_datetime": ["2023-01-01T00:30:00"] * n,
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "sr_flag": rng.choice(a=[0, 1], size=n).tolist(),
            "affiliated_base_number": ["B00001"] * n,
        }
    )


def _make_fhvhv_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "hvfhs_license_num": ["HV0003"] * n,
            "pickup_datetime": ["2023-01-01T00:00:00"] * n,
            "dropoff_datetime": ["2023-01-01T00:30:00"] * n,
            "trip_miles": rng.uniform(low=0.5, high=30.0, size=n).tolist(),
            "trip_time": rng.integers(low=60, high=3600, size=n).tolist(),
            "base_passenger_fare": rng.uniform(low=5.0, high=80.0, size=n).tolist(),
            "tips": rng.uniform(low=0.0, high=15.0, size=n).tolist(),
            "driver_pay": rng.uniform(low=5.0, high=60.0, size=n).tolist(),
        }
    )


class TestYellowDataCleaning:
    """Tests for yellow taxi data cleaning."""

    def test_outlier_counts_per_method(self) -> None:
        df = _make_yellow_df()
        result = YellowDataCleaning().analyze(df=df)

        outliers = result.summary_data["outlier_counts"]
        for col in ("fare_amount", "trip_distance", "total_amount"):
            assert "iqr" in outliers[col]
            assert "zscore" in outliers[col]
            assert "isolation_forest" in outliers[col]

    def test_cleaned_output_shape(self) -> None:
        df = _make_yellow_df()
        result = YellowDataCleaning().analyze(df=df)

        comparison = result.summary_data["strategy_comparison"]
        assert comparison["removal"]["rows_before"] == 200
        assert comparison["removal"]["rows_after"] <= 200

    def test_quality_violations_present(self) -> None:
        df = _make_yellow_df()
        result = YellowDataCleaning().analyze(df=df)

        violations = result.summary_data["quality_violations"]
        assert "negative_fares" in violations
        assert "zero_distances" in violations
        assert "impossible_durations" in violations
        assert "invalid_passenger_count" in violations

    def test_quality_violations_with_bad_data(self) -> None:
        df = pl.DataFrame(
            {
                "fare_amount": [-5.0, 10.0, 20.0, 15.0, 12.0],
                "trip_distance": [0.0, 2.0, 5.0, 3.0, 4.0],
                "total_amount": [10.0, 12.0, 22.0, 17.0, 14.0],
                "tip_amount": [1.0, 2.0, 3.0, 1.5, 2.5],
                "tolls_amount": [0.0, 0.0, 0.0, 0.0, 0.0],
                "tpep_pickup_datetime": [
                    "2023-01-01T00:00:00",
                    "2023-01-01T01:00:00",
                    "2023-01-01T02:00:00",
                    "2023-01-01T03:00:00",
                    "2023-01-01T04:00:00",
                ],
                "tpep_dropoff_datetime": [
                    "2023-01-01T00:30:00",
                    "2023-01-01T01:30:00",
                    "2023-01-01T01:00:00",
                    "2023-01-01T03:30:00",
                    "2023-01-01T04:30:00",
                ],
                "passenger_count": [0, 1, 2, 10, 1],
            }
        )
        result = YellowDataCleaning().analyze(df=df)

        violations = result.summary_data["quality_violations"]
        assert violations["negative_fares"] == 1
        assert violations["zero_distances"] == 1
        assert violations["impossible_durations"] == 1
        assert violations["invalid_passenger_count"] == 2

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_yellow_df()
        result = YellowDataCleaning().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows <= 200

    def test_summary_data_structure(self) -> None:
        df = _make_yellow_df()
        result = YellowDataCleaning().analyze(df=df)

        expected_keys = {
            "outlier_counts",
            "quality_violations",
            "strategy_comparison",
            "num_rows",
            "num_outlier_columns",
        }
        assert set(result.summary_data.keys()) == expected_keys

    def test_detail_s3_key(self) -> None:
        df = _make_yellow_df()
        result = YellowDataCleaning().analyze(df=df)

        assert result.detail_s3_key == "data_cleaning_detail.parquet"


class TestGreenDataCleaning:
    """Tests for green taxi data cleaning."""

    def test_outlier_counts_present(self) -> None:
        df = _make_green_df()
        result = GreenDataCleaning().analyze(df=df)

        outliers = result.summary_data["outlier_counts"]
        assert "fare_amount" in outliers
        assert "trip_distance" in outliers

    def test_green_specific_quality_rules(self) -> None:
        df = pl.DataFrame(
            {
                "fare_amount": [10.0, 20.0, 15.0, 12.0, 18.0],
                "trip_distance": [2.0, 5.0, 3.0, 4.0, 6.0],
                "total_amount": [12.0, 22.0, 17.0, 14.0, 20.0],
                "tip_amount": [1.0, 2.0, 1.5, 1.0, 2.0],
                "tolls_amount": [0.0, 0.0, 0.0, 0.0, 0.0],
                "ehail_fee": [0.0, -1.0, 0.0, 0.0, 0.0],
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
                    "2023-01-01T02:30:00",
                    "2023-01-01T03:30:00",
                    "2023-01-01T04:30:00",
                ],
                "passenger_count": [1, 2, 3, 1, 2],
            }
        )
        result = GreenDataCleaning().analyze(df=df)

        violations = result.summary_data["quality_violations"]
        assert violations["negative_ehail_fee"] == 1

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_green_df()
        result = GreenDataCleaning().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows <= 200


class TestFhvDataCleaning:
    """Tests for FHV taxi data cleaning — limited behavior."""

    def test_no_outlier_columns(self) -> None:
        df = _make_fhv_df()
        result = FhvDataCleaning().analyze(df=df)

        assert result.summary_data["outlier_counts"] == {}
        assert result.summary_data["num_outlier_columns"] == 0

    def test_no_fare_quality_rules(self) -> None:
        df = _make_fhv_df()
        result = FhvDataCleaning().analyze(df=df)

        violations = result.summary_data["quality_violations"]
        assert "negative_fares" not in violations
        assert "zero_distances" not in violations

    def test_duration_quality_rule(self) -> None:
        df = pl.DataFrame(
            {
                "dispatching_base_num": ["B00001", "B00002", "B00003"],
                "pickup_datetime": [
                    "2023-01-01T00:00:00",
                    "2023-01-01T01:00:00",
                    "2023-01-01T02:00:00",
                ],
                "dropoff_datetime": [
                    "2023-01-01T00:30:00",
                    "2023-01-01T00:30:00",
                    "2023-01-01T02:30:00",
                ],
                "pulocationid": [1, 2, 3],
                "dolocationid": [4, 5, 6],
                "sr_flag": [0, 1, 0],
                "affiliated_base_number": ["B00001", "B00002", "B00003"],
            }
        )
        result = FhvDataCleaning().analyze(df=df)

        violations = result.summary_data["quality_violations"]
        assert violations["impossible_durations"] == 1

    def test_all_rows_retained_no_outlier_removal(self) -> None:
        df = _make_fhv_df()
        result = FhvDataCleaning().analyze(df=df)

        comparison = result.summary_data["strategy_comparison"]
        assert comparison["removal"]["rows_removed"] == 0

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_fhv_df()
        result = FhvDataCleaning().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 200


class TestFhvhvDataCleaning:
    """Tests for FHVHV taxi data cleaning — partial behavior."""

    def test_outlier_columns_are_partial(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvDataCleaning().analyze(df=df)

        outliers = result.summary_data["outlier_counts"]
        assert "trip_miles" in outliers
        assert "base_passenger_fare" in outliers
        assert "fare_amount" not in outliers

    def test_quality_violations_fhvhv_specific(self) -> None:
        df = pl.DataFrame(
            {
                "hvfhs_license_num": ["HV0003"] * 5,
                "trip_miles": [0.0, 2.0, 5.0, 3.0, 4.0],
                "trip_time": [-10, 600, 1200, 900, 800],
                "base_passenger_fare": [-5.0, 10.0, 20.0, 15.0, 12.0],
                "tips": [1.0, 2.0, 3.0, 1.5, 2.5],
                "driver_pay": [8.0, 16.0, 12.0, 10.0, 9.0],
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
        result = FhvhvDataCleaning().analyze(df=df)

        violations = result.summary_data["quality_violations"]
        assert violations["negative_fares"] == 1
        assert violations["zero_distances"] == 1
        assert violations["impossible_durations"] == 1
        assert violations["negative_trip_time"] == 1

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvDataCleaning().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows <= 200

    def test_summary_data_structure(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvDataCleaning().analyze(df=df)

        expected_keys = {
            "outlier_counts",
            "quality_violations",
            "strategy_comparison",
            "num_rows",
            "num_outlier_columns",
        }
        assert set(result.summary_data.keys()) == expected_keys


class TestEdgeCases:
    """Edge case tests for data cleaning."""

    def test_empty_dataframe_yellow(self) -> None:
        df = _make_yellow_df().head(n=0)
        result = YellowDataCleaning().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""

    def test_empty_dataframe_fhv(self) -> None:
        df = _make_fhv_df().head(n=0)
        result = FhvDataCleaning().analyze(df=df)

        assert result.summary_data["skipped"] is True

    def test_single_row(self) -> None:
        df = _make_yellow_df().head(n=1)
        result = YellowDataCleaning().analyze(df=df)

        # Single row: outlier detection needs >= 2 rows, so no outlier columns
        assert result.summary_data["outlier_counts"] == {}
        assert result.summary_data["num_rows"] == 1

    def test_all_clean_data(self) -> None:
        df = pl.DataFrame(
            {
                "fare_amount": [10.0, 12.0, 11.0, 13.0, 10.5] * 40,
                "trip_distance": [2.0, 2.5, 2.1, 2.3, 2.2] * 40,
                "total_amount": [12.0, 14.0, 13.0, 15.0, 12.5] * 40,
                "tip_amount": [2.0, 2.5, 2.1, 2.3, 2.2] * 40,
                "tolls_amount": [0.0, 0.0, 0.0, 0.0, 0.0] * 40,
                "tpep_pickup_datetime": ["2023-01-01T00:00:00"] * 200,
                "tpep_dropoff_datetime": ["2023-01-01T00:30:00"] * 200,
                "passenger_count": [1, 2, 3, 1, 2] * 40,
            }
        )
        result = YellowDataCleaning().analyze(df=df)

        violations = result.summary_data["quality_violations"]
        assert violations["negative_fares"] == 0
        assert violations["zero_distances"] == 0
        assert violations["impossible_durations"] == 0
        assert violations["invalid_passenger_count"] == 0
