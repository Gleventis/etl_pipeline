"""Shared test fixtures for the aggregator service."""

from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.server.main import app
from src.server.models import FiltersApplied, PipelineFiltersApplied


@pytest.fixture()
def client() -> TestClient:
    """FastAPI test client."""
    return TestClient(app=app)


@pytest.fixture()
def mock_fetch_results() -> Generator[patch, None, None]:
    """Mock api_client.fetch_analytical_results in routes."""
    with patch("src.server.routes.fetch_analytical_results") as mock:
        yield mock


@pytest.fixture()
def mock_fetch_pipeline_summary() -> Generator[patch, None, None]:
    """Mock api_client.fetch_pipeline_summary in routes."""
    with patch("src.server.routes.fetch_pipeline_summary") as mock:
        yield mock


def make_filters(**kwargs: str | None) -> FiltersApplied:
    """Create a FiltersApplied instance with given overrides."""
    return FiltersApplied(**kwargs)


def make_pipeline_filters(**kwargs: str | None) -> PipelineFiltersApplied:
    """Create a PipelineFiltersApplied instance with given overrides."""
    return PipelineFiltersApplied(**kwargs)
