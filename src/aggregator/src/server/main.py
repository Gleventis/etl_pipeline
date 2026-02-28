"""FastAPI application for the aggregator service."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.server.routes import health_router, router
from src.services.config import SETTINGS

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifecycle.

    Args:
        application: The FastAPI app instance.

    Yields:
        None after setup is complete.
    """
    logging.basicConfig(level=SETTINGS.LOG_LEVEL)
    logger.info("aggregator service starting")
    yield
    logger.info("aggregator service shutting down")


app = FastAPI(version="0.1.0", title="Aggregator", lifespan=lifespan)
app.include_router(health_router)
app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app=app, host=SETTINGS.SERVER_HOST, port=SETTINGS.SERVER_PORT)
