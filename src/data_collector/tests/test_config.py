"""Tests for data collector configuration."""

import pytest

from src.services.config import Settings


class TestSettings:
    """Tests for Settings configuration model."""

    def test_defaults(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
        monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
        monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
        monkeypatch.delenv("MINIO_BUCKET", raising=False)
        monkeypatch.delenv("THREAD_POOL_SIZE", raising=False)
        monkeypatch.delenv("SERVER_HOST", raising=False)
        monkeypatch.delenv("SERVER_PORT", raising=False)

        settings = Settings()

        assert settings.THREAD_POOL_SIZE == 4
        assert settings.MINIO_ENDPOINT == "localhost:9000"
        assert settings.MINIO_ACCESS_KEY == "minioadmin"
        assert settings.MINIO_SECRET_KEY == "minioadmin"
        assert settings.MINIO_BUCKET == "data-collector"
        assert settings.SERVER_HOST == "0.0.0.0"
        assert settings.SERVER_PORT == 8000

    def test_override_from_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("THREAD_POOL_SIZE", "8")
        monkeypatch.setenv("MINIO_ENDPOINT", "minio:9000")
        monkeypatch.setenv("MINIO_BUCKET", "custom-bucket")
        monkeypatch.setenv("SERVER_PORT", "9090")

        settings = Settings()

        assert settings.THREAD_POOL_SIZE == 8
        assert settings.MINIO_ENDPOINT == "minio:9000"
        assert settings.MINIO_BUCKET == "custom-bucket"
        assert settings.SERVER_PORT == 9090

    def test_thread_pool_size_must_be_positive(self):
        with pytest.raises(ValueError):
            Settings(THREAD_POOL_SIZE=0)

    def test_server_port_must_be_valid(self):
        with pytest.raises(ValueError):
            Settings(SERVER_PORT=0)

        with pytest.raises(ValueError):
            Settings(SERVER_PORT=70000)
