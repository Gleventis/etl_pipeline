"""Configuration for the analyzer service."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Analyzer service configuration loaded from environment variables."""

    MINIO_ENDPOINT: str = Field(default="localhost:9000")
    MINIO_ACCESS_KEY: str = Field(default="minioadmin")
    MINIO_SECRET_KEY: str = Field(default="minioadmin")
    API_SERVER_URL: str = Field(default="http://localhost:8000")
    OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS: str = Field(
        default="descriptive-statistics-results"
    )
    OUTPUT_BUCKET_DATA_CLEANING: str = Field(default="cleaned-data")
    OUTPUT_BUCKET_TEMPORAL_ANALYSIS: str = Field(default="temporal-analysis-results")
    OUTPUT_BUCKET_GEOSPATIAL_ANALYSIS: str = Field(
        default="geospatial-analysis-results"
    )
    OUTPUT_BUCKET_FARE_REVENUE_ANALYSIS: str = Field(
        default="fare-revenue-analysis-results"
    )
    SERVER_HOST: str = Field(default="0.0.0.0")
    SERVER_PORT: int = Field(default=8002, ge=1, le=65535)


SETTINGS = Settings()


if __name__ == "__main__":
    settings = Settings()
    print(f"Config: {settings.model_dump()}")
