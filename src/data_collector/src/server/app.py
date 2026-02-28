"""FastAPI application for the data collector service."""

from fastapi import FastAPI

from src.server.routes import router

app = FastAPI(version="0.1.0", title="Data Collector")

app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app=app, host="0.0.0.0", port=8000)
