"""Shared orchestration logic for analytical step execution."""

import io
import logging
import time

import polars as pl

from src.server.models import AnalyzeRequest, AnalyzeResponse, StepName
from src.services.api_server_client import post_analytical_result
from src.services.config import Settings
from src.services.registry import get_analyzer
from utilities.s3 import create_s3_client, download_object, ensure_bucket, upload_object

logger = logging.getLogger(__name__)

_STEP_TO_BUCKET_ATTR: dict[StepName, str] = {
    StepName.DESCRIPTIVE_STATISTICS: "OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS",
    StepName.DATA_CLEANING: "OUTPUT_BUCKET_DATA_CLEANING",
    StepName.TEMPORAL_ANALYSIS: "OUTPUT_BUCKET_TEMPORAL_ANALYSIS",
    StepName.GEOSPATIAL_ANALYSIS: "OUTPUT_BUCKET_GEOSPATIAL_ANALYSIS",
    StepName.FARE_REVENUE_ANALYSIS: "OUTPUT_BUCKET_FARE_REVENUE_ANALYSIS",
}


def execute_step(
    *,
    step_name: StepName,
    request: AnalyzeRequest,
    settings: Settings,
) -> AnalyzeResponse:
    """Execute a single analytical step end-to-end.

    Orchestrates: download parquet → run analyzer → upload detail → post summary.

    Args:
        step_name: Which analytical step to run.
        request: Incoming request with bucket, object, taxi_type, job_execution_id.
        settings: Service configuration.

    Returns:
        AnalyzeResponse indicating success or failure.
    """
    try:
        s3 = create_s3_client(
            endpoint_url=f"http://{settings.MINIO_ENDPOINT}",
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
        )

        raw_bytes = download_object(
            client=s3,
            bucket=request.input_bucket,
            key=request.input_object,
        )
        df = pl.read_parquet(source=io.BytesIO(raw_bytes))
        logger.info(
            "loaded parquet: rows=%d, cols=%d, step=%s",
            len(df),
            len(df.columns),
            step_name,
        )

        analyzer = get_analyzer(step_name=step_name, taxi_type=request.taxi_type)

        start = time.monotonic()
        result = analyzer.analyze(df=df)
        computation_time = time.monotonic() - start
        logger.info(
            "analysis complete: step=%s, taxi_type=%s, time=%.2fs",
            step_name,
            request.taxi_type,
            computation_time,
        )

        output_bucket = getattr(settings, _STEP_TO_BUCKET_ATTR[step_name])
        ensure_bucket(client=s3, bucket=output_bucket)
        detail_key = (
            f"{request.taxi_type}/{request.job_execution_id}/{result.detail_s3_key}"
        )
        upload_object(
            client=s3,
            bucket=output_bucket,
            key=detail_key,
            data=result.detail_bytes,
        )

        detail_s3_path = f"s3://{output_bucket}/{detail_key}"
        posted = post_analytical_result(
            api_server_url=settings.API_SERVER_URL,
            job_execution_id=request.job_execution_id,
            result_type=step_name.value,
            summary_data=result.summary_data,
            detail_s3_path=detail_s3_path,
            computation_time_seconds=computation_time,
        )
        if not posted:
            logger.warning(
                "failed to post result to api server: step=%s, job_execution_id=%s",
                step_name,
                request.job_execution_id,
            )

        return AnalyzeResponse(success=True)

    except Exception as exc:
        logger.error(
            "step execution failed: step=%s, taxi_type=%s, error=%s",
            step_name,
            request.taxi_type,
            exc,
        )
        return AnalyzeResponse(success=False, error=str(exc))


if __name__ == "__main__":
    print(f"step_executor loaded, steps: {list(_STEP_TO_BUCKET_ATTR.keys())}")
