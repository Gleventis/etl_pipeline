"""Tests for API server configuration."""

import pytest

from src.services.config import Settings


class TestSettings:
    """Tests for Settings model."""

    def test_default_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("SERVER_HOST", raising=False)
        monkeypatch.delenv("SERVER_PORT", raising=False)
        monkeypatch.delenv("LOG_LEVEL", raising=False)

        settings = Settings()

        assert (
            settings.DATABASE_URL
            == "postgresql://api_server:api_server@localhost:5432/api_server"
        )
        assert settings.SERVER_HOST == "0.0.0.0"
        assert settings.SERVER_PORT == 8000
        assert settings.LOG_LEVEL == "INFO"

    def test_override_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL", "postgresql://custom:custom@db:5432/custom")
        monkeypatch.setenv("SERVER_PORT", "9000")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        settings = Settings()

        assert settings.DATABASE_URL == "postgresql://custom:custom@db:5432/custom"
        assert settings.SERVER_PORT == 9000
        assert settings.LOG_LEVEL == "DEBUG"

    def test_server_port_range(self) -> None:
        with pytest.raises(Exception):
            Settings(SERVER_PORT=0)
        with pytest.raises(Exception):
            Settings(SERVER_PORT=70000)
