"""Tests for parquet schema validation."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.server.models import TaxiType
from src.services.schemas import EXPECTED_COLUMNS, validate_parquet_schema


def _make_parquet_bytes(column_names: list[str]) -> bytes:
    """Create a minimal parquet file in memory with the given column names."""
    import io

    arrays = [pa.array([1]) for _ in column_names]
    table = pa.table(dict(zip(column_names, arrays)))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


class TestExpectedColumns:
    """Tests for EXPECTED_COLUMNS definitions."""

    def test_all_taxi_types_defined(self):
        for taxi_type in [
            TaxiType.YELLOW,
            TaxiType.GREEN,
            TaxiType.FHV,
            TaxiType.FHVHV,
        ]:
            assert taxi_type in EXPECTED_COLUMNS

    def test_all_type_not_in_expected(self):
        assert TaxiType.ALL not in EXPECTED_COLUMNS

    def test_columns_are_lowercase(self):
        for taxi_type, columns in EXPECTED_COLUMNS.items():
            for col in columns:
                assert col == col.lower(), (
                    f"{taxi_type}: column '{col}' is not lowercase"
                )


class TestValidateParquetSchema:
    """Tests for validate_parquet_schema function."""

    def test_valid_yellow_schema(self):
        columns = [col.title() for col in EXPECTED_COLUMNS[TaxiType.YELLOW]]
        file_bytes = _make_parquet_bytes(columns)
        errors = validate_parquet_schema(
            file_bytes=file_bytes, taxi_type=TaxiType.YELLOW
        )
        assert errors == []

    def test_valid_schema_with_extra_columns(self):
        columns = list(EXPECTED_COLUMNS[TaxiType.FHV]) + ["extra_new_column"]
        file_bytes = _make_parquet_bytes(columns)
        errors = validate_parquet_schema(file_bytes=file_bytes, taxi_type=TaxiType.FHV)
        assert errors == []

    def test_missing_columns_detected(self):
        columns = list(EXPECTED_COLUMNS[TaxiType.YELLOW])[:5]
        file_bytes = _make_parquet_bytes(columns)
        errors = validate_parquet_schema(
            file_bytes=file_bytes, taxi_type=TaxiType.YELLOW
        )
        assert len(errors) == 1
        assert "missing columns" in errors[0]

    def test_case_insensitive_matching(self):
        columns = [col.upper() for col in EXPECTED_COLUMNS[TaxiType.GREEN]]
        file_bytes = _make_parquet_bytes(columns)
        errors = validate_parquet_schema(
            file_bytes=file_bytes, taxi_type=TaxiType.GREEN
        )
        assert errors == []

    def test_invalid_parquet_bytes(self):
        errors = validate_parquet_schema(
            file_bytes=b"not a parquet file", taxi_type=TaxiType.YELLOW
        )
        assert len(errors) == 1
        assert "not a valid parquet file" in errors[0]

    def test_empty_bytes(self):
        errors = validate_parquet_schema(file_bytes=b"", taxi_type=TaxiType.YELLOW)
        assert len(errors) == 1
        assert "not a valid parquet file" in errors[0]

    @pytest.mark.parametrize(
        "taxi_type", [TaxiType.YELLOW, TaxiType.GREEN, TaxiType.FHV, TaxiType.FHVHV]
    )
    def test_exact_expected_columns_pass(self, taxi_type: TaxiType):
        columns = list(EXPECTED_COLUMNS[taxi_type])
        file_bytes = _make_parquet_bytes(columns)
        errors = validate_parquet_schema(file_bytes=file_bytes, taxi_type=taxi_type)
        assert errors == []
