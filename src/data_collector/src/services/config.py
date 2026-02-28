"""Configuration for the data collector service."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Data collector service configuration loaded from environment variables."""

    THREAD_POOL_SIZE: int = Field(default=4, ge=1)
    MINIO_ENDPOINT: str = Field(default="localhost:9000")
    MINIO_ACCESS_KEY: str = Field(default="minioadmin")
    MINIO_SECRET_KEY: str = Field(default="minioadmin")
    MINIO_BUCKET: str = Field(default="data-collector")
    SCHEDULER_URL: str = Field(default="http://localhost:8001")
    SERVER_HOST: str = Field(default="0.0.0.0")
    SERVER_PORT: int = Field(default=8000, ge=1, le=65535)


SETTINGS = Settings()


if __name__ == "__main__":
    settings = Settings()
    print(f"Config: {settings.model_dump()}")
