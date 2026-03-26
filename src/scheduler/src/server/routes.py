"""Routes for the scheduler API."""

import logging

from fastapi import APIRouter, HTTPException, Request, status

from src.server.models import ResumeResponse, ScheduleRequest, ScheduleResponse
from src.services.pipeline import validate_step_names
from src.services.scheduler import SchedulerService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/scheduler", tags=["Scheduler"])


def _get_service(request: Request) -> SchedulerService:
    """Retrieve the SchedulerService from app state.

    Args:
        request: FastAPI request object.

    Returns:
        The SchedulerService instance.
    """
    return request.app.state.scheduler_service


@router.post(
    "/schedule",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ScheduleResponse,
)
def schedule(request: ScheduleRequest, raw_request: Request) -> ScheduleResponse:
    """Schedule analytical pipeline for a batch of files.

    Args:
        request: Batch of object paths and bucket to process.
        raw_request: FastAPI request for accessing app state.

    Returns:
        ScheduleResponse with per-file status.
    """
    invalid = validate_step_names(step_names=request.skip_checkpoints)
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"invalid step names in skip_checkpoints: {invalid}",
        )
    logger.info(
        "schedule request: bucket=%s, objects=%d", request.bucket, len(request.objects)
    )
    service = _get_service(request=raw_request)
    files = service.schedule_batch(
        bucket=request.bucket,
        objects=request.objects,
        skip_checkpoints=request.skip_checkpoints,
    )
    return ScheduleResponse(files=files)


@router.post(
    "/resume",
    status_code=status.HTTP_200_OK,
    response_model=ResumeResponse,
)
def resume(raw_request: Request) -> ResumeResponse:
    """Resume all failed jobs from where they left off.

    Args:
        raw_request: FastAPI request for accessing app state.

    Returns:
        ResumeResponse with list of resumed jobs.
    """
    logger.info("resume request received")
    service = _get_service(request=raw_request)
    resumed = service.resume_failed()
    return ResumeResponse(resumed=resumed)


if __name__ == "__main__":
    print(f"Router prefix: {router.prefix}")
    print(f"Router routes: {[r.path for r in router.routes]}")
