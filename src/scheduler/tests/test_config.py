"""Tests for scheduler configuration."""

import pytest

from src.services.config import Settings


class TestSettings:
    """Tests for Settings model."""

    def test_default_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ANALYZER_URL", raising=False)
        monkeypatch.delenv("API_SERVER_URL", raising=False)
        monkeypatch.delenv("PREFECT_API_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("SERVER_HOST", raising=False)
        monkeypatch.delenv("SERVER_PORT", raising=False)
        monkeypatch.delenv("STEP_DESCRIPTIVE_STATISTICS_BUCKET", raising=False)
        monkeypatch.delenv("STEP_DATA_CLEANING_BUCKET", raising=False)
        monkeypatch.delenv("STEP_TEMPORAL_ANALYSIS_BUCKET", raising=False)
        monkeypatch.delenv("STEP_GEOSPATIAL_ANALYSIS_BUCKET", raising=False)
        monkeypatch.delenv("STEP_FARE_REVENUE_ANALYSIS_BUCKET", raising=False)
        monkeypatch.delenv("ANALYZER_TIMEOUT", raising=False)

        settings = Settings()

        assert settings.ANALYZER_URL == "http://localhost:8002"
        assert settings.API_SERVER_URL == "http://localhost:8000"
        assert settings.PREFECT_API_URL == "http://localhost:4200/api"
        assert (
            settings.DATABASE_URL
            == "postgresql://scheduler:scheduler@localhost:5432/scheduler"
        )
        assert settings.SERVER_HOST == "0.0.0.0"
        assert settings.SERVER_PORT == 8001
        assert settings.ANALYZER_TIMEOUT == 300.0
        assert settings.STEP_DESCRIPTIVE_STATISTICS_BUCKET == "raw-data"
        assert settings.STEP_DATA_CLEANING_BUCKET == "raw-data"
        assert settings.STEP_TEMPORAL_ANALYSIS_BUCKET == "cleaned-data"
        assert settings.STEP_GEOSPATIAL_ANALYSIS_BUCKET == "cleaned-data"
        assert settings.STEP_FARE_REVENUE_ANALYSIS_BUCKET == "cleaned-data"

    def test_override_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ANALYZER_URL", "http://custom:9999")
        monkeypatch.setenv("API_SERVER_URL", "http://custom:8000")
        monkeypatch.setenv("PREFECT_API_URL", "http://custom:4200/api")
        monkeypatch.setenv("SERVER_PORT", "9000")

        settings = Settings()

        assert settings.ANALYZER_URL == "http://custom:9999"
        assert settings.API_SERVER_URL == "http://custom:8000"
        assert settings.PREFECT_API_URL == "http://custom:4200/api"
        assert settings.SERVER_PORT == 9000

    def test_server_port_range(self) -> None:
        with pytest.raises(Exception):
            Settings(SERVER_PORT=0)
        with pytest.raises(Exception):
            Settings(SERVER_PORT=70000)
