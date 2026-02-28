"""FastAPI application for the scheduler service."""

import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.server.routes import router
from src.services.config import SETTINGS
from src.services.database import get_connection, init_schema
from src.services.scheduler import SchedulerService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifecycle — initialize DB and scheduler service.

    Configures Prefect API URL, ensures the job_state table exists,
    and wires up the SchedulerService with a db_url string so that
    Prefect flows can open their own connections.

    Args:
        application: The FastAPI app instance.

    Yields:
        None after setup is complete.
    """
    os.environ["PREFECT_API_URL"] = SETTINGS.PREFECT_API_URL
    logger.info("prefect api url configured: %s", SETTINGS.PREFECT_API_URL)

    with get_connection(database_url=SETTINGS.DATABASE_URL) as conn:
        init_schema(conn=conn)

    application.state.scheduler_service = SchedulerService(
        settings=SETTINGS,
        db_url=SETTINGS.DATABASE_URL,
    )
    yield


app = FastAPI(version="0.1.0", title="Scheduler", lifespan=lifespan)

app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app=app, host="0.0.0.0", port=8001)
