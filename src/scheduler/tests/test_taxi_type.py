"""Tests for taxi type extraction from object paths."""

import pytest

from src.services.taxi_type import extract_taxi_type


class TestExtractTaxiType:
    """Tests for extract_taxi_type function."""

    @pytest.mark.parametrize(
        "object_name,expected",
        [
            ("yellow/2022/01/yellow_tripdata_2022-01.parquet", "yellow"),
            ("green/2023/06/green_tripdata_2023-06.parquet", "green"),
            ("fhv/2022/03/fhv_tripdata_2022-03.parquet", "fhv"),
            ("fhvhv/2024/12/fhvhv_tripdata_2024-12.parquet", "fhvhv"),
        ],
    )
    def test_extracts_all_four_types(self, object_name: str, expected: str):
        assert extract_taxi_type(object_name=object_name) == expected

    def test_extracts_from_deeply_nested_path(self):
        result = extract_taxi_type(object_name="yellow/2022/01/subdir/file.parquet")
        assert result == "yellow"

    def test_case_insensitive(self):
        assert extract_taxi_type(object_name="YELLOW/2022/file.parquet") == "yellow"

    def test_raises_for_unknown_prefix(self):
        with pytest.raises(ValueError, match="unrecognized taxi type"):
            extract_taxi_type(object_name="uber/2022/01/file.parquet")

    def test_raises_for_empty_string(self):
        with pytest.raises(ValueError, match="unrecognized taxi type"):
            extract_taxi_type(object_name="")

    def test_raises_for_no_slash(self):
        with pytest.raises(ValueError, match="unrecognized taxi type"):
            extract_taxi_type(object_name="somefile.parquet")

    def test_fhvhv_not_confused_with_fhv(self):
        assert extract_taxi_type(object_name="fhvhv/file.parquet") == "fhvhv"
        assert extract_taxi_type(object_name="fhv/file.parquet") == "fhv"
