"""Tests for descriptive statistics implementations across all taxi types."""

import io
import json

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from src.services.fhv.descriptive_statistics import FhvDescriptiveStatistics
from src.services.fhvhv.descriptive_statistics import FhvhvDescriptiveStatistics
from src.services.green.descriptive_statistics import GreenDescriptiveStatistics
from src.services.yellow.descriptive_statistics import YellowDescriptiveStatistics


def _make_yellow_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "vendorid": rng.integers(low=1, high=3, size=n).tolist(),
            "tpep_pickup_datetime": ["2023-01-01T00:00:00"] * n,
            "tpep_dropoff_datetime": ["2023-01-01T00:30:00"] * n,
            "passenger_count": rng.integers(low=1, high=6, size=n).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=30.0, size=n).tolist(),
            "ratecodeid": rng.integers(low=1, high=7, size=n).tolist(),
            "store_and_fwd_flag": ["N"] * n,
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "payment_type": rng.integers(low=1, high=5, size=n).tolist(),
            "fare_amount": rng.uniform(low=2.5, high=100.0, size=n).tolist(),
            "extra": rng.uniform(low=0.0, high=5.0, size=n).tolist(),
            "mta_tax": [0.5] * n,
            "tip_amount": rng.uniform(low=0.0, high=20.0, size=n).tolist(),
            "tolls_amount": rng.uniform(low=0.0, high=10.0, size=n).tolist(),
            "improvement_surcharge": [0.3] * n,
            "total_amount": rng.uniform(low=5.0, high=130.0, size=n).tolist(),
            "congestion_surcharge": rng.choice(a=[0.0, 2.5], size=n).tolist(),
            "airport_fee": rng.choice(a=[0.0, 1.25], size=n).tolist(),
        }
    )


def _make_green_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "vendorid": rng.integers(low=1, high=3, size=n).tolist(),
            "lpep_pickup_datetime": ["2023-01-01T00:00:00"] * n,
            "lpep_dropoff_datetime": ["2023-01-01T00:30:00"] * n,
            "store_and_fwd_flag": ["N"] * n,
            "ratecodeid": rng.integers(low=1, high=7, size=n).tolist(),
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "passenger_count": rng.integers(low=1, high=6, size=n).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=30.0, size=n).tolist(),
            "fare_amount": rng.uniform(low=2.5, high=100.0, size=n).tolist(),
            "extra": rng.uniform(low=0.0, high=5.0, size=n).tolist(),
            "mta_tax": [0.5] * n,
            "tip_amount": rng.uniform(low=0.0, high=20.0, size=n).tolist(),
            "tolls_amount": rng.uniform(low=0.0, high=10.0, size=n).tolist(),
            "ehail_fee": [None] * n,
            "improvement_surcharge": [0.3] * n,
            "total_amount": rng.uniform(low=5.0, high=130.0, size=n).tolist(),
            "payment_type": rng.integers(low=1, high=5, size=n).tolist(),
            "trip_type": rng.integers(low=1, high=3, size=n).tolist(),
            "congestion_surcharge": rng.choice(a=[0.0, 2.5], size=n).tolist(),
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
            "dispatching_base_num": ["B00001"] * n,
            "originating_base_num": ["B00001"] * n,
            "request_datetime": ["2023-01-01T00:00:00"] * n,
            "on_scene_datetime": ["2023-01-01T00:05:00"] * n,
            "pickup_datetime": ["2023-01-01T00:06:00"] * n,
            "dropoff_datetime": ["2023-01-01T00:30:00"] * n,
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "trip_miles": rng.uniform(low=0.5, high=30.0, size=n).tolist(),
            "trip_time": rng.integers(low=60, high=3600, size=n).tolist(),
            "base_passenger_fare": rng.uniform(low=5.0, high=80.0, size=n).tolist(),
            "tolls": rng.uniform(low=0.0, high=10.0, size=n).tolist(),
            "bcf": rng.uniform(low=0.0, high=3.0, size=n).tolist(),
            "sales_tax": rng.uniform(low=0.0, high=5.0, size=n).tolist(),
            "congestion_surcharge": rng.choice(a=[0.0, 2.75], size=n).tolist(),
            "airport_fee": rng.choice(a=[0.0, 2.5], size=n).tolist(),
            "tips": rng.uniform(low=0.0, high=15.0, size=n).tolist(),
            "driver_pay": rng.uniform(low=5.0, high=60.0, size=n).tolist(),
            "shared_request_flag": ["N"] * n,
            "shared_match_flag": ["N"] * n,
            "access_a_ride_flag": ["N"] * n,
            "wav_request_flag": ["N"] * n,
            "wav_match_flag": ["N"] * n,
        }
    )


class TestYellowDescriptiveStatistics:
    """Tests for yellow taxi descriptive statistics."""

    def test_summary_has_percentiles_for_numeric_columns(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        assert "percentiles" in result.summary_data
        assert "fare_amount" in result.summary_data["percentiles"]
        assert "trip_distance" in result.summary_data["percentiles"]

    def test_percentile_keys(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        fare_pcts = result.summary_data["percentiles"]["fare_amount"]
        expected_keys = {"p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99"}
        assert set(fare_pcts.keys()) == expected_keys

    def test_percentiles_are_monotonically_increasing(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        fare_pcts = result.summary_data["percentiles"]["fare_amount"]
        values = [fare_pcts[f"p{p}"] for p in (1, 5, 10, 25, 50, 75, 90, 95, 99)]
        assert values == sorted(values)

    def test_distribution_has_skewness_and_kurtosis(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        dist = result.summary_data["distribution"]
        assert "fare_amount" in dist
        assert "skewness" in dist["fare_amount"]
        assert "kurtosis" in dist["fare_amount"]

    def test_correlation_columns_present(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        assert "correlation_columns" in result.summary_data
        assert len(result.summary_data["correlation_columns"]) >= 2

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert "histograms" in table.column_names
        assert "correlation" in table.column_names

    def test_detail_histograms_have_100_bins(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        histograms = json.loads(table.column("histograms")[0].as_py())
        for col, hist in histograms.items():
            assert len(hist["counts"]) == 100, f"{col} has {len(hist['counts'])} bins"
            assert len(hist["bin_edges"]) == 101, (
                f"{col} has {len(hist['bin_edges'])} edges"
            )

    def test_num_rows_in_summary(self) -> None:
        df = _make_yellow_df(n=50)
        result = YellowDescriptiveStatistics().analyze(df=df)

        assert result.summary_data["num_rows"] == 50

    def test_excludes_non_numeric_columns(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        assert "store_and_fwd_flag" not in result.summary_data["percentiles"]
        assert "tpep_pickup_datetime" not in result.summary_data["percentiles"]


class TestGreenDescriptiveStatistics:
    """Tests for green taxi descriptive statistics."""

    def test_summary_has_green_specific_columns(self) -> None:
        df = _make_green_df()
        result = GreenDescriptiveStatistics().analyze(df=df)

        assert "trip_type" in result.summary_data["percentiles"]
        assert "trip_distance" in result.summary_data["percentiles"]

    def test_handles_null_ehail_fee(self) -> None:
        df = _make_green_df()
        result = GreenDescriptiveStatistics().analyze(df=df)

        # ehail_fee is all null — should not appear in percentiles
        assert "ehail_fee" not in result.summary_data["percentiles"]

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_green_df()
        result = GreenDescriptiveStatistics().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert "histograms" in table.column_names


class TestFhvDescriptiveStatistics:
    """Tests for FHV taxi descriptive statistics."""

    def test_limited_numeric_columns(self) -> None:
        df = _make_fhv_df()
        result = FhvDescriptiveStatistics().analyze(df=df)

        assert result.summary_data["num_numeric_columns"] == 3

    def test_has_percentiles_for_available_columns(self) -> None:
        df = _make_fhv_df()
        result = FhvDescriptiveStatistics().analyze(df=df)

        assert "sr_flag" in result.summary_data["percentiles"]
        assert "pulocationid" in result.summary_data["percentiles"]

    def test_no_fare_columns_in_output(self) -> None:
        df = _make_fhv_df()
        result = FhvDescriptiveStatistics().analyze(df=df)

        assert "fare_amount" not in result.summary_data["percentiles"]


class TestFhvhvDescriptiveStatistics:
    """Tests for FHVHV taxi descriptive statistics."""

    def test_has_trip_and_fare_columns(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvDescriptiveStatistics().analyze(df=df)

        pcts = result.summary_data["percentiles"]
        assert "trip_miles" in pcts
        assert "trip_time" in pcts
        assert "base_passenger_fare" in pcts
        assert "tips" in pcts
        assert "driver_pay" in pcts

    def test_no_yellow_specific_columns(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvDescriptiveStatistics().analyze(df=df)

        assert "fare_amount" not in result.summary_data["percentiles"]

    def test_correlation_computed(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvDescriptiveStatistics().analyze(df=df)

        assert len(result.summary_data["correlation_columns"]) >= 2


class TestEdgeCases:
    """Edge case tests for descriptive statistics."""

    def test_empty_dataframe(self) -> None:
        df = _make_yellow_df(n=200).head(n=0)
        result = YellowDescriptiveStatistics().analyze(df=df)

        assert result.summary_data["num_rows"] == 0
        assert result.summary_data["percentiles"] == {}

    def test_single_row(self) -> None:
        df = _make_yellow_df(n=200).head(n=1)
        result = YellowDescriptiveStatistics().analyze(df=df)

        assert result.summary_data["num_rows"] == 1
        assert "fare_amount" in result.summary_data["percentiles"]

    def test_detail_s3_key_set(self) -> None:
        df = _make_yellow_df()
        result = YellowDescriptiveStatistics().analyze(df=df)

        assert result.detail_s3_key == "descriptive_statistics_detail.parquet"
