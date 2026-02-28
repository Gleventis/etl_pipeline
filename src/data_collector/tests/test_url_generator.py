"""Tests for URL generation."""

import pytest

from src.server.models import TaxiType
from src.services.url_generator import generate_urls

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


class TestGenerateUrls:
    """Tests for generate_urls function."""

    def test_single_year_month_type(self):
        urls = generate_urls(years=[2023], months=[1], taxi_type=TaxiType.YELLOW)
        assert urls == [f"{BASE}/yellow_tripdata_2023-01.parquet"]

    def test_month_zero_padded(self):
        urls = generate_urls(years=[2023], months=[3], taxi_type=TaxiType.GREEN)
        assert urls == [f"{BASE}/green_tripdata_2023-03.parquet"]

    def test_multiple_months(self):
        urls = generate_urls(years=[2023], months=[1, 2, 3], taxi_type=TaxiType.FHV)
        assert len(urls) == 3
        assert urls[0] == f"{BASE}/fhv_tripdata_2023-01.parquet"
        assert urls[2] == f"{BASE}/fhv_tripdata_2023-03.parquet"

    def test_multiple_years(self):
        urls = generate_urls(years=[2022, 2023], months=[6], taxi_type=TaxiType.FHVHV)
        assert len(urls) == 2
        assert urls[0] == f"{BASE}/fhvhv_tripdata_2022-06.parquet"
        assert urls[1] == f"{BASE}/fhvhv_tripdata_2023-06.parquet"

    def test_all_taxi_types(self):
        urls = generate_urls(years=[2023], months=[1], taxi_type=TaxiType.ALL)
        assert len(urls) == 4
        assert f"{BASE}/yellow_tripdata_2023-01.parquet" in urls
        assert f"{BASE}/green_tripdata_2023-01.parquet" in urls
        assert f"{BASE}/fhv_tripdata_2023-01.parquet" in urls
        assert f"{BASE}/fhvhv_tripdata_2023-01.parquet" in urls

    def test_all_types_ordering(self):
        """Verify order is year -> month -> type."""
        urls = generate_urls(years=[2023], months=[1, 2], taxi_type=TaxiType.ALL)
        assert len(urls) == 8
        # First 4 should be month 1 (all types), next 4 month 2
        assert all("2023-01" in u for u in urls[:4])
        assert all("2023-02" in u for u in urls[4:])

    def test_empty_years(self):
        urls = generate_urls(years=[], months=[1], taxi_type=TaxiType.YELLOW)
        assert urls == []

    def test_empty_months(self):
        urls = generate_urls(years=[2023], months=[], taxi_type=TaxiType.YELLOW)
        assert urls == []

    @pytest.mark.parametrize(
        "taxi_type,prefix",
        [
            (TaxiType.YELLOW, "yellow"),
            (TaxiType.GREEN, "green"),
            (TaxiType.FHV, "fhv"),
            (TaxiType.FHVHV, "fhvhv"),
        ],
    )
    def test_each_taxi_type_prefix(self, taxi_type: TaxiType, prefix: str):
        urls = generate_urls(years=[2024], months=[12], taxi_type=taxi_type)
        assert urls == [f"{BASE}/{prefix}_tripdata_2024-12.parquet"]
