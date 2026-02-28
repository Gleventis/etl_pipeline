"""Configuration for the aggregator service."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Aggregator service configuration loaded from environment variables."""

    API_SERVER_URL: str = Field(default="http://localhost:8000")
    SERVER_HOST: str = Field(default="0.0.0.0")
    SERVER_PORT: int = Field(default=8003, ge=1, le=65535)
    LOG_LEVEL: str = Field(default="INFO")
    REQUEST_TIMEOUT: float = Field(default=30.0, gt=0)


SETTINGS = Settings()


if __name__ == "__main__":
    settings = Settings()
    print(f"Config: {settings.model_dump()}")
