"""FastAPI application for the translator service."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.server.routes import health_router, router
from src.services.config import SETTINGS
from src.services.db import get_connection, init_db

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize database schema on startup."""
    logging.basicConfig(level=logging.INFO)
    with get_connection() as conn:
        init_db(conn=conn)
    logger.info("translator service starting")
    yield
    logger.info("translator service shutting down")


app = FastAPI(version="0.1.0", title="Translator", lifespan=lifespan)
app.include_router(health_router)
app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app=app, host=SETTINGS.SERVER_HOST, port=SETTINGS.SERVER_PORT)
