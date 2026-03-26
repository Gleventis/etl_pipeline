"""Tests for translator request/response models."""

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from src.server.models import RunStatusResponse, TranslateRequest, TranslateResponse


class TestTranslateRequest:
    """Tests for TranslateRequest model."""

    def test_valid_dsl(self):
        req = TranslateRequest(dsl='{"collect": {}}')
        assert req.dsl == '{"collect": {}}'

    def test_empty_dsl_rejected(self):
        with pytest.raises(ValidationError):
            TranslateRequest(dsl="")

    def test_whitespace_only_dsl_accepted(self):
        req = TranslateRequest(dsl=" ")
        assert req.dsl == " "

    def test_frozen(self):
        req = TranslateRequest(dsl="test")
        with pytest.raises(ValidationError):
            req.dsl = "other"


class TestTranslateResponse:
    """Tests for TranslateResponse model."""

    def test_instantiation(self):
        rid = uuid4()
        resp = TranslateResponse(run_id=rid)
        assert resp.run_id == rid

    def test_string_uuid_coercion(self):
        rid = uuid4()
        resp = TranslateResponse(run_id=str(rid))
        assert resp.run_id == rid
        assert isinstance(resp.run_id, UUID)

    def test_serialization(self):
        rid = uuid4()
        resp = TranslateResponse(run_id=rid)
        data = resp.model_dump()
        assert data == {"run_id": rid}

    def test_frozen(self):
        resp = TranslateResponse(run_id=uuid4())
        with pytest.raises(ValidationError):
            resp.run_id = uuid4()


class TestRunStatusResponse:
    """Tests for RunStatusResponse model."""

    @pytest.mark.parametrize(
        "phase",
        ["pending", "collecting", "analyzing", "aggregating", "completed", "failed"],
    )
    def test_all_valid_phases(self, phase: str):
        resp = RunStatusResponse(run_id=uuid4(), phase=phase)
        assert resp.phase == phase

    def test_invalid_phase_rejected(self):
        with pytest.raises(ValidationError):
            RunStatusResponse(run_id=uuid4(), phase="unknown")

    def test_error_default_none(self):
        resp = RunStatusResponse(run_id=uuid4(), phase="pending")
        assert resp.error is None

    def test_error_field(self):
        resp = RunStatusResponse(
            run_id=uuid4(), phase="failed", error="something broke"
        )
        assert resp.error == "something broke"

    def test_serialization(self):
        rid = uuid4()
        resp = RunStatusResponse(run_id=rid, phase="completed")
        data = resp.model_dump()
        assert data == {"run_id": rid, "phase": "completed", "error": None}

    def test_serialization_with_error(self):
        rid = uuid4()
        resp = RunStatusResponse(run_id=rid, phase="failed", error="timeout")
        data = resp.model_dump()
        assert data == {"run_id": rid, "phase": "failed", "error": "timeout"}

    def test_frozen(self):
        resp = RunStatusResponse(run_id=uuid4(), phase="pending")
        with pytest.raises(ValidationError):
            resp.phase = "completed"

    def test_string_uuid_coercion(self):
        rid = uuid4()
        resp = RunStatusResponse(run_id=str(rid), phase="pending")
        assert resp.run_id == rid
        assert isinstance(resp.run_id, UUID)
