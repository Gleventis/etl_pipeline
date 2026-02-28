"""Entrypoint for the aggregator service."""

import uvicorn

from src.server.main import app
from src.services.config import SETTINGS

if __name__ == "__main__":
    uvicorn.run(app=app, host=SETTINGS.SERVER_HOST, port=SETTINGS.SERVER_PORT)
