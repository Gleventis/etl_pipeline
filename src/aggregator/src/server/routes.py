"""API routes for the aggregator service."""

import logging

import httpx
from fastapi import APIRouter, Query, status
from fastapi.responses import JSONResponse

from src.server.models import (
    DataQualityResponse,
    DescriptiveStatsResponse,
    FiltersApplied,
    PipelineFiltersApplied,
    PipelinePerformanceResponse,
    TaxiComparisonResponse,
    TemporalPatternsResponse,
)
from src.services.api_client import fetch_analytical_results, fetch_pipeline_summary
from src.services.data_quality import aggregate_data_quality
from src.services.descriptive_stats import aggregate_descriptive_stats
from src.services.pipeline_performance import aggregate_pipeline_performance
from src.services.taxi_comparison import aggregate_taxi_comparison
from src.services.temporal_patterns import aggregate_temporal_patterns

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/aggregations", tags=["Aggregations"])

health_router = APIRouter(tags=["Health"])


@health_router.get("/health", status_code=status.HTTP_200_OK)
def health() -> dict[str, str]:
    """Return service health status."""
    return {"status": "ok"}


@router.get(
    "/descriptive-stats",
    status_code=status.HTTP_200_OK,
    response_model=DescriptiveStatsResponse,
)
def get_descriptive_stats(
    taxi_type: str | None = Query(default=None),
    start_year: str | None = Query(default=None),
    start_month: str | None = Query(default=None),
    end_year: str | None = Query(default=None),
    end_month: str | None = Query(default=None),
) -> DescriptiveStatsResponse:
    """Aggregate descriptive statistics across all files matching filters."""
    filters = FiltersApplied(
        taxi_type=taxi_type,
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    )

    try:
        results = fetch_analytical_results(
            result_type="descriptive_statistics",
            taxi_type=taxi_type,
            year=start_year,
            month=start_month,
        )
    except httpx.HTTPStatusError as exc:
        logger.error("api server returned error: status=%s", exc.response.status_code)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": f"API Server returned status {exc.response.status_code}",
                "status_code": 502,
            },
        )
    except httpx.ConnectError:
        logger.error("api server unreachable")
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": "API Server unreachable",
                "status_code": 502,
            },
        )

    return aggregate_descriptive_stats(results=results, filters=filters)


TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]


@router.get(
    "/taxi-comparison",
    status_code=status.HTTP_200_OK,
    response_model=TaxiComparisonResponse,
)
def get_taxi_comparison(
    start_year: str | None = Query(default=None),
    start_month: str | None = Query(default=None),
    end_year: str | None = Query(default=None),
    end_month: str | None = Query(default=None),
) -> TaxiComparisonResponse:
    """Compare key metrics between taxi types."""
    filters = FiltersApplied(
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    )

    results_by_type: dict[str, list[dict]] = {}
    try:
        for taxi_type in TAXI_TYPES:
            results_by_type[taxi_type] = fetch_analytical_results(
                result_type="descriptive_statistics",
                taxi_type=taxi_type,
                year=start_year,
                month=start_month,
            )
    except httpx.HTTPStatusError as exc:
        logger.error("api server returned error: status=%s", exc.response.status_code)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": f"API Server returned status {exc.response.status_code}",
                "status_code": 502,
            },
        )
    except httpx.ConnectError:
        logger.error("api server unreachable")
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": "API Server unreachable",
                "status_code": 502,
            },
        )

    return aggregate_taxi_comparison(
        results_by_type=results_by_type,
        filters=filters,
    )


@router.get(
    "/temporal-patterns",
    status_code=status.HTTP_200_OK,
    response_model=TemporalPatternsResponse,
)
def get_temporal_patterns(
    taxi_type: str | None = Query(default=None),
    start_year: str | None = Query(default=None),
    start_month: str | None = Query(default=None),
    end_year: str | None = Query(default=None),
    end_month: str | None = Query(default=None),
) -> TemporalPatternsResponse:
    """Aggregate temporal patterns across all files matching filters."""
    filters = FiltersApplied(
        taxi_type=taxi_type,
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    )

    try:
        results = fetch_analytical_results(
            result_type="temporal_analysis",
            taxi_type=taxi_type,
            year=start_year,
            month=start_month,
        )
    except httpx.HTTPStatusError as exc:
        logger.error("api server returned error: status=%s", exc.response.status_code)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": f"API Server returned status {exc.response.status_code}",
                "status_code": 502,
            },
        )
    except httpx.ConnectError:
        logger.error("api server unreachable")
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": "API Server unreachable",
                "status_code": 502,
            },
        )

    return aggregate_temporal_patterns(results=results, filters=filters)


@router.get(
    "/data-quality",
    status_code=status.HTTP_200_OK,
    response_model=DataQualityResponse,
)
def get_data_quality(
    taxi_type: str | None = Query(default=None),
    start_year: str | None = Query(default=None),
    start_month: str | None = Query(default=None),
    end_year: str | None = Query(default=None),
    end_month: str | None = Query(default=None),
) -> DataQualityResponse:
    """Aggregate data quality and cleaning metrics across files."""
    filters = FiltersApplied(
        taxi_type=taxi_type,
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    )

    try:
        results = fetch_analytical_results(
            result_type="data_cleaning",
            taxi_type=taxi_type,
            year=start_year,
            month=start_month,
        )
    except httpx.HTTPStatusError as exc:
        logger.error("api server returned error: status=%s", exc.response.status_code)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": f"API Server returned status {exc.response.status_code}",
                "status_code": 502,
            },
        )
    except httpx.ConnectError:
        logger.error("api server unreachable")
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": "API Server unreachable",
                "status_code": 502,
            },
        )

    return aggregate_data_quality(results=results, filters=filters)


@router.get(
    "/pipeline-performance",
    status_code=status.HTTP_200_OK,
    response_model=PipelinePerformanceResponse,
)
def get_pipeline_performance(
    taxi_type: str | None = Query(default=None),
    analytical_step: str | None = Query(default=None),
    start_year: str | None = Query(default=None),
    start_month: str | None = Query(default=None),
    end_year: str | None = Query(default=None),
    end_month: str | None = Query(default=None),
) -> PipelinePerformanceResponse:
    """Report computation time per analytical step with checkpoint savings."""
    filters = PipelineFiltersApplied(
        taxi_type=taxi_type,
        analytical_step=analytical_step,
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    )

    try:
        results = fetch_analytical_results(
            result_type=analytical_step,
            taxi_type=taxi_type,
            year=start_year,
            month=start_month,
        )
        pipeline_summary = fetch_pipeline_summary()
    except httpx.HTTPStatusError as exc:
        logger.error("api server returned error: status=%s", exc.response.status_code)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": f"API Server returned status {exc.response.status_code}",
                "status_code": 502,
            },
        )
    except httpx.ConnectError:
        logger.error("api server unreachable")
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "Bad Gateway",
                "detail": "API Server unreachable",
                "status_code": 502,
            },
        )

    return aggregate_pipeline_performance(
        results=results,
        pipeline_summary=pipeline_summary,
        filters=filters,
    )


if __name__ == "__main__":
    print(f"Aggregation router prefix: {router.prefix}")
    print(f"Health router routes: {health_router.routes}")
