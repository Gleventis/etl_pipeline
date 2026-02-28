"""Tests for fare revenue analysis implementations across all taxi types."""

import io
import json

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from src.services.fhv.fare_revenue_analysis import FhvFareRevenueAnalysis
from src.services.fhvhv.fare_revenue_analysis import FhvhvFareRevenueAnalysis
from src.services.green.fare_revenue_analysis import GreenFareRevenueAnalysis
from src.services.yellow.fare_revenue_analysis import YellowFareRevenueAnalysis

_EXPECTED_SUMMARY_KEYS = {
    "num_rows",
    "num_days",
    "forecast_slope",
    "forecast_r_squared",
    "anomaly_counts",
    "tip_prediction_r_squared",
    "surcharge_breakdown",
}


def _make_yellow_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "tpep_pickup_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:15:00" for i in range(n)
            ],
            "tpep_dropoff_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:45:00" for i in range(n)
            ],
            "fare_amount": rng.uniform(low=5.0, high=60.0, size=n).tolist(),
            "total_amount": rng.uniform(low=8.0, high=80.0, size=n).tolist(),
            "tip_amount": rng.uniform(low=0.0, high=15.0, size=n).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=25.0, size=n).tolist(),
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "extra": rng.choice(a=[0.0, 0.5, 1.0], size=n).tolist(),
            "mta_tax": [0.5] * n,
            "improvement_surcharge": [0.3] * n,
            "congestion_surcharge": rng.choice(a=[0.0, 2.5], size=n).tolist(),
            "airport_fee": rng.choice(a=[0.0, 1.25], size=n).tolist(),
        }
    )


def _make_green_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "lpep_pickup_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:15:00" for i in range(n)
            ],
            "lpep_dropoff_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:45:00" for i in range(n)
            ],
            "fare_amount": rng.uniform(low=5.0, high=60.0, size=n).tolist(),
            "total_amount": rng.uniform(low=8.0, high=80.0, size=n).tolist(),
            "tip_amount": rng.uniform(low=0.0, high=15.0, size=n).tolist(),
            "trip_distance": rng.uniform(low=0.5, high=25.0, size=n).tolist(),
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "extra": rng.choice(a=[0.0, 0.5, 1.0], size=n).tolist(),
            "mta_tax": [0.5] * n,
            "ehail_fee": rng.choice(a=[0.0, 1.0], size=n).tolist(),
            "improvement_surcharge": [0.3] * n,
            "congestion_surcharge": rng.choice(a=[0.0, 2.5], size=n).tolist(),
        }
    )


def _make_fhv_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "dispatching_base_num": [f"B{i:05d}" for i in range(n)],
            "pickup_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:00:00" for i in range(n)
            ],
            "dropoff_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:30:00" for i in range(n)
            ],
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "dolocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "sr_flag": rng.choice(a=[None, 1], size=n).tolist(),
            "affiliated_base_number": [f"B{i:05d}" for i in range(n)],
        }
    )


def _make_fhvhv_df(n: int = 200) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "pickup_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:15:00" for i in range(n)
            ],
            "dropoff_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:45:00" for i in range(n)
            ],
            "base_passenger_fare": rng.uniform(low=5.0, high=60.0, size=n).tolist(),
            "tips": rng.uniform(low=0.0, high=15.0, size=n).tolist(),
            "trip_miles": rng.uniform(low=0.5, high=25.0, size=n).tolist(),
            "driver_pay": rng.uniform(low=3.0, high=50.0, size=n).tolist(),
            "pulocationid": rng.integers(low=1, high=265, size=n).tolist(),
            "tolls": rng.choice(a=[0.0, 2.0, 5.0], size=n).tolist(),
            "bcf": rng.uniform(low=0.0, high=3.0, size=n).tolist(),
            "sales_tax": rng.uniform(low=0.0, high=2.0, size=n).tolist(),
            "congestion_surcharge": rng.choice(a=[0.0, 2.75], size=n).tolist(),
        }
    )


def _read_detail_parquet(detail_bytes: bytes) -> dict[str, str]:
    """Read detail parquet and return column name → value mapping."""
    table = pq.read_table(source=io.BytesIO(detail_bytes))
    return {col: table.column(col)[0].as_py() for col in table.column_names}


class TestYellowFareRevenueAnalysis:
    """Tests for yellow taxi fare revenue analysis."""

    def test_summary_data_structure(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_revenue_forecast_output(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        forecast = json.loads(detail["forecast"])
        assert "slope" in forecast
        assert "intercept" in forecast
        assert "r_squared" in forecast
        assert "predictions" in forecast
        assert isinstance(forecast["predictions"], list)

    def test_fare_anomaly_detection(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        anomalies = result.summary_data["anomaly_counts"]
        assert "fare_amount" in anomalies
        assert "total_amount" in anomalies
        assert isinstance(anomalies["fare_amount"], int)

    def test_tip_prediction_output(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        tip_pred = json.loads(detail["tip_prediction"])
        assert "coefficients" in tip_pred
        assert "r_squared" in tip_pred
        assert "distance" in tip_pred["coefficients"]

    def test_fare_distribution_by_zone(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        distribution = json.loads(detail["distribution"])
        assert "by_zone" in distribution
        assert len(distribution["by_zone"]) > 0
        assert "zone" in distribution["by_zone"][0]
        assert "mean_fare" in distribution["by_zone"][0]

    def test_fare_distribution_by_time(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        distribution = json.loads(detail["distribution"])
        assert "by_time_of_day" in distribution
        assert len(distribution["by_time_of_day"]) > 0
        assert "hour" in distribution["by_time_of_day"][0]

    def test_fare_distribution_by_distance(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        distribution = json.loads(detail["distribution"])
        assert "by_distance_bucket" in distribution
        assert len(distribution["by_distance_bucket"]) > 0
        assert "bucket" in distribution["by_distance_bucket"][0]

    def test_surcharge_breakdown(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        surcharges = result.summary_data["surcharge_breakdown"]
        assert "extra_total" in surcharges
        assert "mta_tax_total" in surcharges
        assert "improvement_surcharge_total" in surcharges
        assert "congestion_surcharge_total" in surcharges
        assert "airport_fee_total" in surcharges

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1

    def test_detail_s3_key(self) -> None:
        df = _make_yellow_df()
        result = YellowFareRevenueAnalysis().analyze(df=df)

        assert result.detail_s3_key == "fare_revenue_analysis_detail.parquet"


class TestGreenFareRevenueAnalysis:
    """Tests for green taxi fare revenue analysis."""

    def test_summary_data_structure(self) -> None:
        df = _make_green_df()
        result = GreenFareRevenueAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_surcharge_includes_ehail_fee(self) -> None:
        df = _make_green_df()
        result = GreenFareRevenueAnalysis().analyze(df=df)

        surcharges = result.summary_data["surcharge_breakdown"]
        assert "ehail_fee_total" in surcharges
        assert "ehail_fee_mean" in surcharges

    def test_tip_prediction_output(self) -> None:
        df = _make_green_df()
        result = GreenFareRevenueAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        tip_pred = json.loads(detail["tip_prediction"])
        assert "coefficients" in tip_pred
        assert "r_squared" in tip_pred

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_green_df()
        result = GreenFareRevenueAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1


class TestFhvFareRevenueAnalysis:
    """Tests for FHV taxi fare revenue analysis — skipped entirely."""

    def test_skip_behavior(self) -> None:
        df = _make_fhv_df()
        result = FhvFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert "no fare columns" in result.summary_data["reason"]

    def test_includes_num_rows(self) -> None:
        df = _make_fhv_df()
        result = FhvFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["num_rows"] == df.height

    def test_empty_detail_bytes(self) -> None:
        df = _make_fhv_df()
        result = FhvFareRevenueAnalysis().analyze(df=df)

        assert result.detail_bytes == b""

    def test_detail_s3_key(self) -> None:
        df = _make_fhv_df()
        result = FhvFareRevenueAnalysis().analyze(df=df)

        assert result.detail_s3_key == "fare_revenue_analysis_detail.parquet"


class TestFhvhvFareRevenueAnalysis:
    """Tests for FHVHV taxi fare revenue analysis — uses base_passenger_fare."""

    def test_summary_data_structure(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvFareRevenueAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_anomaly_detection_uses_driver_pay(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvFareRevenueAnalysis().analyze(df=df)

        anomalies = result.summary_data["anomaly_counts"]
        assert "base_passenger_fare" in anomalies
        assert "driver_pay" in anomalies

    def test_surcharge_breakdown_fhvhv_cols(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvFareRevenueAnalysis().analyze(df=df)

        surcharges = result.summary_data["surcharge_breakdown"]
        assert "tolls_total" in surcharges
        assert "bcf_total" in surcharges
        assert "sales_tax_total" in surcharges
        assert "congestion_surcharge_total" in surcharges

    def test_tip_prediction_output(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvFareRevenueAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        tip_pred = json.loads(detail["tip_prediction"])
        assert "coefficients" in tip_pred
        assert "r_squared" in tip_pred

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvFareRevenueAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1


class TestEdgeCases:
    """Edge case tests for fare revenue analysis."""

    def test_empty_dataframe_yellow(self) -> None:
        df = _make_yellow_df().head(n=0)
        result = YellowFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""

    def test_empty_dataframe_green(self) -> None:
        df = _make_green_df().head(n=0)
        result = GreenFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""

    def test_empty_dataframe_fhv(self) -> None:
        df = _make_fhv_df().head(n=0)
        result = FhvFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True

    def test_empty_dataframe_fhvhv(self) -> None:
        df = _make_fhvhv_df().head(n=0)
        result = FhvhvFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""

    def test_single_row_yellow(self) -> None:
        df = _make_yellow_df().head(n=1)
        result = YellowFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["num_rows"] == 1
        assert result.summary_data["num_days"] == 1
        # forecast with 1 day → slope=0
        assert result.summary_data["forecast_slope"] == 0.0

    def test_all_zero_fares(self) -> None:
        df = pl.DataFrame(
            {
                "tpep_pickup_datetime": [
                    f"2023-01-01T{i:02d}:00:00" for i in range(10)
                ],
                "tpep_dropoff_datetime": [
                    f"2023-01-01T{i:02d}:30:00" for i in range(10)
                ],
                "fare_amount": [0.0] * 10,
                "total_amount": [0.0] * 10,
                "tip_amount": [0.0] * 10,
                "trip_distance": [0.0] * 10,
                "pulocationid": [1] * 10,
            }
        )
        result = YellowFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["num_rows"] == 10
        assert result.summary_data["anomaly_counts"]["fare_amount"] == 0

    def test_missing_fare_column_yellow(self) -> None:
        df = pl.DataFrame({"some_col": [1, 2, 3]})
        result = YellowFareRevenueAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""
