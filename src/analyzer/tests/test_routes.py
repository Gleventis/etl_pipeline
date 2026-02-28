"""Tests for the analyzer FastAPI route handlers."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.server.main import app
from src.server.models import AnalyzeResponse, StepName

client = TestClient(app=app)

_VALID_PAYLOAD = {
    "input_bucket": "raw-data",
    "input_object": "yellow/2023/01/data.parquet",
    "taxi_type": "yellow",
    "job_execution_id": 1,
}

_ENDPOINTS: list[tuple[str, StepName]] = [
    ("/analyze/descriptive-statistics", StepName.DESCRIPTIVE_STATISTICS),
    ("/analyze/data-cleaning", StepName.DATA_CLEANING),
    ("/analyze/temporal-analysis", StepName.TEMPORAL_ANALYSIS),
    ("/analyze/geospatial-analysis", StepName.GEOSPATIAL_ANALYSIS),
    ("/analyze/fare-revenue-analysis", StepName.FARE_REVENUE_ANALYSIS),
]


class TestRoutesDelegation:
    """Each endpoint delegates to execute_step with the correct StepName."""

    @pytest.mark.parametrize(
        ("url", "expected_step"),
        _ENDPOINTS,
        ids=[s.value for _, s in _ENDPOINTS],
    )
    def test_passes_correct_step_name(self, url: str, expected_step: StepName):
        with patch("src.server.routes.execute_step") as mock_exec:
            mock_exec.return_value = AnalyzeResponse(success=True)

            response = client.post(url, json=_VALID_PAYLOAD)

            assert response.status_code == 200
            mock_exec.assert_called_once()
            assert mock_exec.call_args.kwargs["step_name"] == expected_step

    @pytest.mark.parametrize(
        ("url", "_step"),
        _ENDPOINTS,
        ids=[s.value for _, s in _ENDPOINTS],
    )
    def test_returns_success_response(self, url: str, _step: StepName):
        with patch("src.server.routes.execute_step") as mock_exec:
            mock_exec.return_value = AnalyzeResponse(success=True)

            response = client.post(url, json=_VALID_PAYLOAD)

            body = response.json()
            assert body == {"success": True, "error": None}

    @pytest.mark.parametrize(
        ("url", "_step"),
        _ENDPOINTS,
        ids=[s.value for _, s in _ENDPOINTS],
    )
    def test_returns_error_response(self, url: str, _step: StepName):
        with patch("src.server.routes.execute_step") as mock_exec:
            mock_exec.return_value = AnalyzeResponse(success=False, error="boom")

            response = client.post(url, json=_VALID_PAYLOAD)

            body = response.json()
            assert body == {"success": False, "error": "boom"}


class TestRoutesValidation:
    """Request validation returns 422 for invalid payloads."""

    def test_missing_required_field(self):
        payload = {
            "input_bucket": "raw-data",
            "taxi_type": "yellow",
            "job_execution_id": 1,
        }

        response = client.post("/analyze/descriptive-statistics", json=payload)

        assert response.status_code == 422

    def test_invalid_taxi_type(self):
        payload = {**_VALID_PAYLOAD, "taxi_type": "invalid"}

        response = client.post("/analyze/descriptive-statistics", json=payload)

        assert response.status_code == 422

    def test_job_execution_id_zero(self):
        payload = {**_VALID_PAYLOAD, "job_execution_id": 0}

        response = client.post("/analyze/descriptive-statistics", json=payload)

        assert response.status_code == 422

    def test_empty_input_bucket(self):
        payload = {**_VALID_PAYLOAD, "input_bucket": ""}

        response = client.post("/analyze/descriptive-statistics", json=payload)

        assert response.status_code == 422

    def test_empty_body(self):
        response = client.post("/analyze/descriptive-statistics", json={})

        assert response.status_code == 422
