"""Request, response, and internal models for the analyzer service."""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class TaxiType(StrEnum):
    """Supported NYC TLC taxi types."""

    YELLOW = "yellow"
    GREEN = "green"
    FHV = "fhv"
    FHVHV = "fhvhv"


class StepName(StrEnum):
    """Analytical pipeline step identifiers."""

    DESCRIPTIVE_STATISTICS = "descriptive_statistics"
    DATA_CLEANING = "data_cleaning"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    GEOSPATIAL_ANALYSIS = "geospatial_analysis"
    FARE_REVENUE_ANALYSIS = "fare_revenue_analysis"


class AnalyzeRequest(BaseModel):
    """Payload received from the scheduler for a single analytical step."""

    model_config = ConfigDict(frozen=True)

    input_bucket: str = Field(min_length=1)
    input_object: str = Field(min_length=1)
    taxi_type: TaxiType
    job_execution_id: int = Field(ge=1)


class AnalyzeResponse(BaseModel):
    """Response returned to the scheduler after step execution."""

    model_config = ConfigDict(frozen=True)

    success: bool
    error: str | None = None


class StepResult(BaseModel):
    """Internal model carrying outputs from an analytical step."""

    summary_data: dict
    detail_bytes: bytes
    detail_s3_key: str


if __name__ == "__main__":
    request = AnalyzeRequest(
        input_bucket="raw-data",
        input_object="yellow/2023/01/yellow_tripdata_2023-01.parquet",
        taxi_type=TaxiType.YELLOW,
        job_execution_id=42,
    )
    print(f"Request: {request.model_dump()}")

    response = AnalyzeResponse(success=True)
    print(f"Response: {response.model_dump()}")
