"""Tests for temporal analysis implementations across all taxi types."""

import io
import json

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from src.services.fhv.temporal_analysis import FhvTemporalAnalysis
from src.services.fhvhv.temporal_analysis import FhvhvTemporalAnalysis
from src.services.green.temporal_analysis import GreenTemporalAnalysis
from src.services.yellow.temporal_analysis import YellowTemporalAnalysis

_EXPECTED_SUMMARY_KEYS = {
    "num_rows",
    "num_hours",
    "peak_hours",
    "top_frequencies",
    "decomposition_length",
}


def _make_yellow_df(n: int = 500) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "tpep_pickup_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:00:00" for i in range(n)
            ],
            "tpep_dropoff_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:30:00" for i in range(n)
            ],
            "fare_amount": rng.uniform(low=5.0, high=50.0, size=n).tolist(),
        }
    )


def _make_green_df(n: int = 500) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "lpep_pickup_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:00:00" for i in range(n)
            ],
            "lpep_dropoff_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:30:00" for i in range(n)
            ],
            "fare_amount": rng.uniform(low=5.0, high=50.0, size=n).tolist(),
        }
    )


def _make_fhv_df(n: int = 500) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    return pl.DataFrame(
        {
            "pickup_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:00:00" for i in range(n)
            ],
            "dropoff_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:30:00" for i in range(n)
            ],
            "sr_flag": rng.integers(low=0, high=2, size=n).tolist(),
        }
    )


def _make_fhvhv_df(n: int = 500) -> pl.DataFrame:
    rng = np.random.default_rng(seed=42)
    base_times = [f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:00:00" for i in range(n)]
    request_times = [
        f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:{rng.integers(low=0, high=30):02d}:00"
        for i in range(n)
    ]
    return pl.DataFrame(
        {
            "request_datetime": request_times,
            "on_scene_datetime": base_times,
            "pickup_datetime": base_times,
            "dropoff_datetime": [
                f"2023-01-{(i // 24) + 1:02d}T{i % 24:02d}:30:00" for i in range(n)
            ],
            "base_passenger_fare": rng.uniform(low=5.0, high=50.0, size=n).tolist(),
        }
    )


def _read_detail_parquet(detail_bytes: bytes) -> dict[str, str]:
    """Read detail parquet and return column name → value mapping."""
    table = pq.read_table(source=io.BytesIO(detail_bytes))
    return {col: table.column(col)[0].as_py() for col in table.column_names}


class TestYellowTemporalAnalysis:
    """Tests for yellow taxi temporal analysis."""

    def test_summary_data_structure(self) -> None:
        df = _make_yellow_df()
        result = YellowTemporalAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_decomposition_output(self) -> None:
        df = _make_yellow_df()
        result = YellowTemporalAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        decomposition = json.loads(detail["decomposition"])
        assert "trend" in decomposition
        assert "seasonal" in decomposition
        assert "residual" in decomposition
        assert (
            len(decomposition["trend"]) == result.summary_data["decomposition_length"]
        )

    def test_fourier_output(self) -> None:
        df = _make_yellow_df()
        result = YellowTemporalAnalysis().analyze(df=df)

        freqs = result.summary_data["top_frequencies"]
        assert isinstance(freqs, list)
        assert len(freqs) > 0
        assert "frequency" in freqs[0]
        assert "magnitude" in freqs[0]

    def test_rolling_stats(self) -> None:
        df = _make_yellow_df()
        result = YellowTemporalAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        rolling = json.loads(detail["rolling_stats"])
        assert "hourly" in rolling
        assert "daily" in rolling
        assert "weekly" in rolling

    def test_peak_hours(self) -> None:
        df = _make_yellow_df()
        result = YellowTemporalAnalysis().analyze(df=df)

        peak_hours = result.summary_data["peak_hours"]
        assert isinstance(peak_hours, list)
        assert all(0 <= h <= 23 for h in peak_hours)

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_yellow_df()
        result = YellowTemporalAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1

    def test_detail_s3_key(self) -> None:
        df = _make_yellow_df()
        result = YellowTemporalAnalysis().analyze(df=df)

        assert result.detail_s3_key == "temporal_analysis_detail.parquet"


class TestGreenTemporalAnalysis:
    """Tests for green taxi temporal analysis."""

    def test_summary_data_structure(self) -> None:
        df = _make_green_df()
        result = GreenTemporalAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_decomposition_output(self) -> None:
        df = _make_green_df()
        result = GreenTemporalAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        decomposition = json.loads(detail["decomposition"])
        assert "trend" in decomposition
        assert "seasonal" in decomposition
        assert "residual" in decomposition

    def test_peak_hours(self) -> None:
        df = _make_green_df()
        result = GreenTemporalAnalysis().analyze(df=df)

        peak_hours = result.summary_data["peak_hours"]
        assert isinstance(peak_hours, list)
        assert all(0 <= h <= 23 for h in peak_hours)

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_green_df()
        result = GreenTemporalAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1


class TestFhvTemporalAnalysis:
    """Tests for FHV taxi temporal analysis — no fare aggregations."""

    def test_summary_data_structure(self) -> None:
        df = _make_fhv_df()
        result = FhvTemporalAnalysis().analyze(df=df)

        assert set(result.summary_data.keys()) == _EXPECTED_SUMMARY_KEYS

    def test_no_fare_aggregations_in_detail(self) -> None:
        df = _make_fhv_df()
        result = FhvTemporalAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        rolling = json.loads(detail["rolling_stats"])
        # FHV rolling stats have trip counts only, no avg_fare
        for window in ("hourly", "daily", "weekly"):
            if rolling[window]:
                assert "rolling_mean_trips" in rolling[window][0]
                assert "avg_fare" not in rolling[window][0]

    def test_peak_hours(self) -> None:
        df = _make_fhv_df()
        result = FhvTemporalAnalysis().analyze(df=df)

        peak_hours = result.summary_data["peak_hours"]
        assert isinstance(peak_hours, list)

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_fhv_df()
        result = FhvTemporalAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1


class TestFhvhvTemporalAnalysis:
    """Tests for FHVHV taxi temporal analysis — includes wait time analysis."""

    def test_summary_data_structure(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvTemporalAnalysis().analyze(df=df)

        expected = _EXPECTED_SUMMARY_KEYS | {"wait_times"}
        assert set(result.summary_data.keys()) == expected

    def test_wait_time_analysis(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvTemporalAnalysis().analyze(df=df)

        wait_times = result.summary_data["wait_times"]
        assert "mean_request_to_pickup_s" in wait_times
        assert "median_request_to_pickup_s" in wait_times
        assert "mean_request_to_scene_s" in wait_times
        assert "mean_scene_to_pickup_s" in wait_times

    def test_wait_times_in_detail_parquet(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvTemporalAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        assert "wait_times" in detail
        wait_times = json.loads(detail["wait_times"])
        assert isinstance(wait_times, dict)

    def test_decomposition_output(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvTemporalAnalysis().analyze(df=df)

        detail = _read_detail_parquet(detail_bytes=result.detail_bytes)
        decomposition = json.loads(detail["decomposition"])
        assert "trend" in decomposition
        assert "seasonal" in decomposition
        assert "residual" in decomposition

    def test_detail_bytes_is_valid_parquet(self) -> None:
        df = _make_fhvhv_df()
        result = FhvhvTemporalAnalysis().analyze(df=df)

        table = pq.read_table(source=io.BytesIO(result.detail_bytes))
        assert table.num_rows == 1


class TestEdgeCases:
    """Edge case tests for temporal analysis."""

    def test_empty_dataframe_yellow(self) -> None:
        df = _make_yellow_df().head(n=0)
        result = YellowTemporalAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""

    def test_empty_dataframe_fhv(self) -> None:
        df = _make_fhv_df().head(n=0)
        result = FhvTemporalAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True

    def test_empty_dataframe_fhvhv(self) -> None:
        df = _make_fhvhv_df().head(n=0)
        result = FhvhvTemporalAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True

    def test_single_row(self) -> None:
        df = _make_yellow_df().head(n=1)
        result = YellowTemporalAnalysis().analyze(df=df)

        assert result.summary_data["num_rows"] == 1
        assert result.summary_data["num_hours"] == 1
        # With 1 hour, decomposition falls back to short-series path
        assert result.summary_data["decomposition_length"] == 1

    def test_single_hour_of_data(self) -> None:
        """Multiple rows but all in the same hour."""
        df = pl.DataFrame(
            {
                "tpep_pickup_datetime": ["2023-01-01T10:00:00"] * 50,
                "tpep_dropoff_datetime": ["2023-01-01T10:30:00"] * 50,
                "fare_amount": [15.0] * 50,
            }
        )
        result = YellowTemporalAnalysis().analyze(df=df)

        assert result.summary_data["num_rows"] == 50
        assert result.summary_data["num_hours"] == 1
        assert result.summary_data["decomposition_length"] == 1

    def test_missing_pickup_column(self) -> None:
        df = pl.DataFrame({"some_col": [1, 2, 3]})
        result = YellowTemporalAnalysis().analyze(df=df)

        assert result.summary_data["skipped"] is True
        assert result.detail_bytes == b""
