"""Tests for analyzer service configuration."""

import pytest

from src.services.config import Settings

# All env vars that Settings reads, used to clear docker-compose injections.
_ALL_ENV_VARS = [
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "API_SERVER_URL",
    "OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS",
    "OUTPUT_BUCKET_DATA_CLEANING",
    "OUTPUT_BUCKET_TEMPORAL_ANALYSIS",
    "OUTPUT_BUCKET_GEOSPATIAL_ANALYSIS",
    "OUTPUT_BUCKET_FARE_REVENUE_ANALYSIS",
    "SERVER_HOST",
    "SERVER_PORT",
]


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove all Settings-related env vars so defaults apply."""
    for var in _ALL_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


class TestSettings:
    """Tests for Settings configuration model."""

    def test_defaults(self, monkeypatch: pytest.MonkeyPatch):
        _clear_env(monkeypatch)

        settings = Settings()

        assert settings.MINIO_ENDPOINT == "localhost:9000"
        assert settings.MINIO_ACCESS_KEY == "minioadmin"
        assert settings.MINIO_SECRET_KEY == "minioadmin"
        assert settings.API_SERVER_URL == "http://localhost:8000"
        assert (
            settings.OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS
            == "descriptive-statistics-results"
        )
        assert settings.OUTPUT_BUCKET_DATA_CLEANING == "cleaned-data"
        assert settings.OUTPUT_BUCKET_TEMPORAL_ANALYSIS == "temporal-analysis-results"
        assert (
            settings.OUTPUT_BUCKET_GEOSPATIAL_ANALYSIS == "geospatial-analysis-results"
        )
        assert (
            settings.OUTPUT_BUCKET_FARE_REVENUE_ANALYSIS
            == "fare-revenue-analysis-results"
        )
        assert settings.SERVER_HOST == "0.0.0.0"
        assert settings.SERVER_PORT == 8002

    def test_override_from_env(self, monkeypatch: pytest.MonkeyPatch):
        _clear_env(monkeypatch)
        monkeypatch.setenv("MINIO_ENDPOINT", "minio:9000")
        monkeypatch.setenv("API_SERVER_URL", "http://api_server:8000")
        monkeypatch.setenv("OUTPUT_BUCKET_DATA_CLEANING", "custom-cleaned")
        monkeypatch.setenv("SERVER_PORT", "9090")

        settings = Settings()

        assert settings.MINIO_ENDPOINT == "minio:9000"
        assert settings.API_SERVER_URL == "http://api_server:8000"
        assert settings.OUTPUT_BUCKET_DATA_CLEANING == "custom-cleaned"
        assert settings.SERVER_PORT == 9090

    def test_server_port_must_be_valid(self):
        with pytest.raises(ValueError):
            Settings(SERVER_PORT=0)

        with pytest.raises(ValueError):
            Settings(SERVER_PORT=70000)
