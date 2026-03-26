"""Request and response models for the translator service."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

RunPhase = Literal[
    "pending",
    "collecting",
    "analyzing",
    "aggregating",
    "completed",
    "failed",
]


class TranslateRequest(BaseModel):
    """DSL translation request body."""

    model_config = ConfigDict(frozen=True)

    dsl: str = Field(min_length=1)


class TranslateResponse(BaseModel):
    """Response body for POST /translator/translate."""

    model_config = ConfigDict(frozen=True)

    run_id: UUID


class RunStatusResponse(BaseModel):
    """Response body for GET /translator/runs/{run_id}."""

    model_config = ConfigDict(frozen=True)

    run_id: UUID
    phase: RunPhase
    error: str | None = None
