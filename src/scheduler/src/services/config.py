"""Configuration for the scheduler service."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Scheduler service configuration loaded from environment variables."""

    ANALYZER_URL: str = Field(default="http://localhost:8002")
    API_SERVER_URL: str = Field(default="http://localhost:8000")
    PREFECT_API_URL: str = Field(default="http://localhost:4200/api")
    DATABASE_URL: str = Field(
        default="postgresql://scheduler:scheduler@localhost:5432/scheduler"
    )
    SERVER_HOST: str = Field(default="0.0.0.0")
    SERVER_PORT: int = Field(default=8001, ge=1, le=65535)
    ANALYZER_TIMEOUT: float = Field(default=300.0)
    STEP_DESCRIPTIVE_STATISTICS_BUCKET: str = Field(default="raw-data")
    STEP_DATA_CLEANING_BUCKET: str = Field(default="raw-data")
    STEP_TEMPORAL_ANALYSIS_BUCKET: str = Field(default="cleaned-data")
    STEP_GEOSPATIAL_ANALYSIS_BUCKET: str = Field(default="cleaned-data")
    STEP_FARE_REVENUE_ANALYSIS_BUCKET: str = Field(default="cleaned-data")


SETTINGS = Settings()


if __name__ == "__main__":
    settings = Settings()
    print(f"Config: {settings.model_dump()}")
