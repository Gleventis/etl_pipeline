"""API routes for the analyzer service."""

from fastapi import APIRouter, status

from src.server.models import AnalyzeRequest, AnalyzeResponse, StepName
from src.services.config import SETTINGS
from src.services.step_executor import execute_step

router = APIRouter(prefix="/analyze", tags=["Analyzer"])


@router.post(
    "/descriptive-statistics",
    status_code=status.HTTP_200_OK,
    response_model=AnalyzeResponse,
)
def descriptive_statistics(request: AnalyzeRequest) -> AnalyzeResponse:
    """Run descriptive statistics analysis on the input parquet file."""
    return execute_step(
        step_name=StepName.DESCRIPTIVE_STATISTICS,
        request=request,
        settings=SETTINGS,
    )


@router.post(
    "/data-cleaning",
    status_code=status.HTTP_200_OK,
    response_model=AnalyzeResponse,
)
def data_cleaning(request: AnalyzeRequest) -> AnalyzeResponse:
    """Run data cleaning analysis on the input parquet file."""
    return execute_step(
        step_name=StepName.DATA_CLEANING,
        request=request,
        settings=SETTINGS,
    )


@router.post(
    "/temporal-analysis",
    status_code=status.HTTP_200_OK,
    response_model=AnalyzeResponse,
)
def temporal_analysis(request: AnalyzeRequest) -> AnalyzeResponse:
    """Run temporal analysis on the input parquet file."""
    return execute_step(
        step_name=StepName.TEMPORAL_ANALYSIS,
        request=request,
        settings=SETTINGS,
    )


@router.post(
    "/geospatial-analysis",
    status_code=status.HTTP_200_OK,
    response_model=AnalyzeResponse,
)
def geospatial_analysis(request: AnalyzeRequest) -> AnalyzeResponse:
    """Run geospatial analysis on the input parquet file."""
    return execute_step(
        step_name=StepName.GEOSPATIAL_ANALYSIS,
        request=request,
        settings=SETTINGS,
    )


@router.post(
    "/fare-revenue-analysis",
    status_code=status.HTTP_200_OK,
    response_model=AnalyzeResponse,
)
def fare_revenue_analysis(request: AnalyzeRequest) -> AnalyzeResponse:
    """Run fare revenue analysis on the input parquet file."""
    return execute_step(
        step_name=StepName.FARE_REVENUE_ANALYSIS,
        request=request,
        settings=SETTINGS,
    )
