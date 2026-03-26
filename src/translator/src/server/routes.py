"""API routes for the translator service."""

import logging
from threading import Thread
from uuid import UUID

from fastapi import APIRouter, HTTPException, status

from src.server.models import RunStatusResponse, TranslateRequest, TranslateResponse
from src.services.db import create_run, get_connection, get_run
from src.services.executor import execute_run
from src.services.parser import parse_dsl

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/translator", tags=["Translator"])

health_router = APIRouter(tags=["Health"])


@health_router.get("/health", status_code=status.HTTP_200_OK)
def health() -> dict[str, str]:
    """Return service health status."""
    return {"status": "ok"}


@router.post(
    "/translate",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TranslateResponse,
)
def translate(request: TranslateRequest) -> TranslateResponse:
    """Parse DSL and start async pipeline execution.

    Args:
        request: DSL translation request body.

    Returns:
        Run ID for polling status.

    Raises:
        HTTPException: 400 if DSL parsing fails.
    """
    try:
        parsed = parse_dsl(dsl=request.dsl)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e

    with get_connection() as conn:
        run_id = create_run(conn=conn, dsl=request.dsl)

    thread = Thread(
        target=execute_run,
        kwargs={"run_id": run_id, "parsed": parsed},
        daemon=True,
    )
    thread.start()

    logger.info("started run: run_id=%s", run_id)
    return TranslateResponse(run_id=run_id)


@router.get(
    "/runs/{run_id}",
    status_code=status.HTTP_200_OK,
    response_model=RunStatusResponse,
)
def get_run_status(run_id: UUID) -> RunStatusResponse:
    """Fetch the current status of a pipeline run.

    Args:
        run_id: UUID of the run to query.

    Returns:
        Current run status.

    Raises:
        HTTPException: 404 if run_id not found.
    """
    with get_connection() as conn:
        row = get_run(conn=conn, run_id=run_id)

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"run {run_id} not found",
        )

    return RunStatusResponse(
        run_id=row["run_id"],
        phase=row["phase"],
        error=row["error"],
    )


if __name__ == "__main__":
    print(f"Translator router prefix: {router.prefix}")
    print(f"Health router routes: {health_router.routes}")
