"""Tests for the download service."""

import io
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.server.models import TaxiType
from src.services.downloader import (
    download_batch,
    download_one,
    parse_url_metadata,
)
from src.services.schemas import EXPECTED_COLUMNS

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _make_parquet_bytes(column_names: list[str]) -> bytes:
    """Create a minimal parquet file in memory with the given column names."""
    arrays = [pa.array([1]) for _ in column_names]
    table = pa.table(dict(zip(column_names, arrays)))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


class TestParseUrlMetadata:
    """Tests for parse_url_metadata."""

    def test_yellow_url(self):
        file_name, taxi_type, year, month = parse_url_metadata(
            url=f"{BASE}/yellow_tripdata_2023-01.parquet"
        )
        assert file_name == "yellow_tripdata_2023-01.parquet"
        assert taxi_type == TaxiType.YELLOW
        assert year == 2023
        assert month == 1

    def test_fhvhv_url(self):
        file_name, taxi_type, year, month = parse_url_metadata(
            url=f"{BASE}/fhvhv_tripdata_2020-12.parquet"
        )
        assert file_name == "fhvhv_tripdata_2020-12.parquet"
        assert taxi_type == TaxiType.FHVHV
        assert year == 2020
        assert month == 12

    def test_invalid_url_raises(self):
        with pytest.raises(ValueError, match="does not match"):
            parse_url_metadata(url="https://example.com/bad.csv")


class TestDownloadOne:
    """Tests for download_one with mocked HTTP."""

    def _mock_response(
        self,
        status_code: int = 200,
        content: bytes = b"",
    ) -> httpx.Response:
        """Build a mock httpx.Response."""
        return httpx.Response(
            status_code=status_code,
            content=content,
            request=httpx.Request(
                method="GET", url=f"{BASE}/yellow_tripdata_2023-01.parquet"
            ),
        )

    def test_successful_download(self):
        valid_bytes = _make_parquet_bytes(list(EXPECTED_COLUMNS[TaxiType.YELLOW]))
        mock_resp = self._mock_response(status_code=200, content=valid_bytes)

        with patch("src.services.downloader.httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            mock_client.get.return_value = mock_resp

            result = download_one(url=f"{BASE}/yellow_tripdata_2023-01.parquet")

        assert result.success is True
        assert result.file_name == "yellow_tripdata_2023-01.parquet"
        assert result.taxi_type == TaxiType.YELLOW
        assert result.year == 2023
        assert result.month == 1
        assert result.file_bytes == valid_bytes
        assert result.error is None

    def test_http_404(self):
        mock_resp = self._mock_response(status_code=404, content=b"Not Found")

        with patch("src.services.downloader.httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            mock_client.get.return_value = mock_resp

            result = download_one(url=f"{BASE}/yellow_tripdata_2023-01.parquet")

        assert result.success is False
        assert "HTTP 404" in result.error

    def test_empty_response_body(self):
        mock_resp = self._mock_response(status_code=200, content=b"")

        with patch("src.services.downloader.httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            mock_client.get.return_value = mock_resp

            result = download_one(url=f"{BASE}/yellow_tripdata_2023-01.parquet")

        assert result.success is False
        assert "empty response body" in result.error

    def test_invalid_parquet_content(self):
        mock_resp = self._mock_response(status_code=200, content=b"not parquet data")

        with patch("src.services.downloader.httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            mock_client.get.return_value = mock_resp

            result = download_one(url=f"{BASE}/yellow_tripdata_2023-01.parquet")

        assert result.success is False
        assert "schema validation failed" in result.error

    def test_schema_missing_columns(self):
        partial_bytes = _make_parquet_bytes(["vendorid", "fare_amount"])
        mock_resp = self._mock_response(status_code=200, content=partial_bytes)

        with patch("src.services.downloader.httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            mock_client.get.return_value = mock_resp

            result = download_one(url=f"{BASE}/yellow_tripdata_2023-01.parquet")

        assert result.success is False
        assert "missing columns" in result.error

    def test_network_error(self):
        with patch("src.services.downloader.httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            mock_client.get.side_effect = httpx.ConnectError("connection refused")

            result = download_one(url=f"{BASE}/yellow_tripdata_2023-01.parquet")

        assert result.success is False
        assert "connection refused" in result.error

    def test_invalid_url_pattern(self):
        result = download_one(url="https://example.com/bad.csv")
        assert result.success is False
        assert "does not match" in result.error


class TestDownloadBatch:
    """Tests for download_batch with mocked HTTP."""

    def test_empty_urls(self):
        results = download_batch(urls=[])
        assert results == []

    def test_multiple_urls(self):
        valid_yellow = _make_parquet_bytes(list(EXPECTED_COLUMNS[TaxiType.YELLOW]))
        valid_green = _make_parquet_bytes(list(EXPECTED_COLUMNS[TaxiType.GREEN]))

        def _mock_get(url: str) -> httpx.Response:
            if "yellow" in url:
                content = valid_yellow
            else:
                content = valid_green
            return httpx.Response(
                status_code=200,
                content=content,
                request=httpx.Request(method="GET", url=url),
            )

        with patch("src.services.downloader.httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            mock_client.get.side_effect = _mock_get

            results = download_batch(
                urls=[
                    f"{BASE}/yellow_tripdata_2023-01.parquet",
                    f"{BASE}/green_tripdata_2023-01.parquet",
                ],
                pool_size=2,
            )

        assert len(results) == 2
        assert all(r.success for r in results)

    def test_partial_failure(self):
        valid_yellow = _make_parquet_bytes(list(EXPECTED_COLUMNS[TaxiType.YELLOW]))

        def _mock_get(url: str) -> httpx.Response:
            if "yellow" in url:
                return httpx.Response(
                    status_code=200,
                    content=valid_yellow,
                    request=httpx.Request(method="GET", url=url),
                )
            return httpx.Response(
                status_code=404,
                content=b"Not Found",
                request=httpx.Request(method="GET", url=url),
            )

        with patch("src.services.downloader.httpx.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value.__enter__.return_value
            mock_client.get.side_effect = _mock_get

            results = download_batch(
                urls=[
                    f"{BASE}/yellow_tripdata_2023-01.parquet",
                    f"{BASE}/green_tripdata_2023-01.parquet",
                ],
                pool_size=2,
            )

        assert len(results) == 2
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 1
        assert len(failures) == 1
