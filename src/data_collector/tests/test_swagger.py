"""Tests for Swagger UI accessibility."""

from fastapi.testclient import TestClient

from src.server.app import app

client = TestClient(app=app)


class TestSwaggerPage:
    """Tests for the /docs Swagger UI endpoint."""

    def test_docs_returns_200(self) -> None:
        """GET /docs returns HTTP 200."""
        response = client.get("/docs")

        assert response.status_code == 200

    def test_docs_contains_swagger_ui(self) -> None:
        """GET /docs serves Swagger UI HTML."""
        response = client.get("/docs")

        assert "swagger-ui" in response.text

    def test_openapi_json_returns_200(self) -> None:
        """GET /openapi.json returns HTTP 200 with valid schema."""
        response = client.get("/openapi.json")

        assert response.status_code == 200
        body = response.json()
        assert body["info"]["title"] == "Data Collector"
        assert body["info"]["version"] == "0.1.0"


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
