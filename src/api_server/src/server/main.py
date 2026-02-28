"""FastAPI application for the API server service."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.server.routes import router
from src.services.config import SETTINGS
from src.services.database import init_schema

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifecycle — initialize database schema on startup.

    Args:
        application: The FastAPI app instance.

    Yields:
        None after setup is complete.
    """
    init_schema(database_url=SETTINGS.DATABASE_URL)
    yield


app = FastAPI(version="0.1.0", title="API Server", lifespan=lifespan)
app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app=app, host="0.0.0.0", port=8000)
