"""Tests for translator API routes."""

import json
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

# Patch lifespan DB calls before importing app
with patch("src.server.main.get_connection"), patch("src.server.main.init_db"):
    from src.server.main import app

VALID_DSL = json.dumps({"collect": {"year": 2024, "month": 1, "taxi_type": "yellow"}})
FIXED_RUN_ID = uuid4()


@pytest.fixture()
def client():
    """TestClient with DB and executor mocked out."""
    with (
        patch("src.server.routes.get_connection") as mock_conn,
        patch("src.server.routes.create_run", return_value=FIXED_RUN_ID),
        patch("src.server.routes.Thread") as mock_thread,
    ):
        mock_conn.return_value.__enter__ = MagicMock()
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_thread.return_value.start = MagicMock()
        yield TestClient(app=app, raise_server_exceptions=False)


class TestPostTranslate:
    """Tests for POST /translator/translate."""

    def test_valid_dsl_returns_202_with_run_id(self, client: TestClient):
        """Valid DSL returns 202 Accepted with a run_id."""
        response = client.post("/translator/translate", json={"dsl": VALID_DSL})

        assert response.status_code == 202
        body = response.json()
        assert body["run_id"] == str(FIXED_RUN_ID)

    def test_invalid_json_dsl_returns_400(self, client: TestClient):
        """Non-JSON DSL string returns 400 with parse error."""
        response = client.post("/translator/translate", json={"dsl": "not json"})

        assert response.status_code == 400
        assert "invalid JSON" in response.json()["detail"]

    def test_no_sections_returns_400(self, client: TestClient):
        """JSON with no recognized sections returns 400."""
        response = client.post("/translator/translate", json={"dsl": '{"foo": "bar"}'})

        assert response.status_code == 400
        assert "at least one section" in response.json()["detail"]

    def test_empty_dsl_returns_422(self, client: TestClient):
        """Empty dsl string fails pydantic min_length validation."""
        response = client.post("/translator/translate", json={"dsl": ""})

        assert response.status_code == 422

    def test_missing_dsl_field_returns_422(self, client: TestClient):
        """Missing dsl field in request body returns 422."""
        response = client.post("/translator/translate", json={})

        assert response.status_code == 422

    def test_spawns_background_thread(self):
        """Valid DSL spawns a daemon thread with execute_run."""
        with (
            patch("src.server.routes.get_connection") as mock_conn,
            patch("src.server.routes.create_run", return_value=FIXED_RUN_ID),
            patch("src.server.routes.Thread") as mock_thread,
        ):
            mock_conn.return_value.__enter__ = MagicMock()
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_thread.return_value.start = MagicMock()

            test_client = TestClient(app=app, raise_server_exceptions=False)
            test_client.post("/translator/translate", json={"dsl": VALID_DSL})

            mock_thread.assert_called_once()
            call_kwargs = mock_thread.call_args.kwargs
            assert call_kwargs["daemon"] is True
            mock_thread.return_value.start.assert_called_once()


class TestGetRunStatus:
    """Tests for GET /translator/runs/{run_id}."""

    def test_existing_run_returns_status(self):
        """Known run_id returns 200 with phase and error fields."""
        run_id = uuid4()
        row = {"run_id": run_id, "phase": "analyzing", "error": None}

        with (
            patch("src.server.routes.get_connection") as mock_conn,
            patch("src.server.routes.get_run", return_value=row),
        ):
            mock_conn.return_value.__enter__ = MagicMock()
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)

            test_client = TestClient(app=app, raise_server_exceptions=False)
            response = test_client.get(f"/translator/runs/{run_id}")

        assert response.status_code == 200
        body = response.json()
        assert body["run_id"] == str(run_id)
        assert body["phase"] == "analyzing"
        assert body["error"] is None

    def test_unknown_run_id_returns_404(self):
        """Unknown run_id returns 404 with detail message."""
        run_id = uuid4()

        with (
            patch("src.server.routes.get_connection") as mock_conn,
            patch("src.server.routes.get_run", return_value=None),
        ):
            mock_conn.return_value.__enter__ = MagicMock()
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)

            test_client = TestClient(app=app, raise_server_exceptions=False)
            response = test_client.get(f"/translator/runs/{run_id}")

        assert response.status_code == 404
        assert str(run_id) in response.json()["detail"]
