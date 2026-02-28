"""Configuration for the API server service."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """API server configuration loaded from environment variables."""

    DATABASE_URL: str = Field(
        default="postgresql://api_server:api_server@localhost:5432/api_server"
    )
    SERVER_HOST: str = Field(default="0.0.0.0")
    SERVER_PORT: int = Field(default=8000, ge=1, le=65535)
    LOG_LEVEL: str = Field(default="INFO")


SETTINGS = Settings()


if __name__ == "__main__":
    settings = Settings()
    print(f"Config: {settings.model_dump()}")
