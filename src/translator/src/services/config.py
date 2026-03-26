"""Translator service configuration."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Translator service settings loaded from environment variables."""

    DATABASE_URL: str = Field(
        default="postgresql://translator:translator@localhost:5436/translator"
    )
    SERVER_HOST: str = Field(default="0.0.0.0")
    SERVER_PORT: int = Field(default=8015, ge=1, le=65535)
    COLLECTOR_URL: str = Field(default="http://localhost:8000")
    SCHEDULER_URL: str = Field(default="http://localhost:8001")
    AGGREGATOR_URL: str = Field(default="http://localhost:8003")
    HTTP_TIMEOUT: int = Field(default=300, ge=1)


SETTINGS = Settings()

if __name__ == "__main__":
    print(SETTINGS.model_dump())
