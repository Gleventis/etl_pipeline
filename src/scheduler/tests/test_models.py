"""Tests for scheduler request/response models."""

import pytest
from pydantic import ValidationError

from src.server.models import (
    FileStatus,
    JobState,
    ResumedJob,
    ResumeResponse,
    ScheduleRequest,
    ScheduleResponse,
)


class TestScheduleRequest:
    """Tests for ScheduleRequest model."""

    def test_valid_request(self) -> None:
        req = ScheduleRequest(bucket="raw-data", objects=["file1.parquet"])
        assert req.bucket == "raw-data"
        assert req.objects == ["file1.parquet"]

    def test_multiple_objects(self) -> None:
        req = ScheduleRequest(
            bucket="raw-data",
            objects=["file1.parquet", "file2.parquet"],
        )
        assert len(req.objects) == 2

    def test_empty_bucket_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ScheduleRequest(bucket="", objects=["file1.parquet"])

    def test_empty_objects_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ScheduleRequest(bucket="raw-data", objects=[])


class TestScheduleResponse:
    """Tests for ScheduleResponse model."""

    def test_response_with_files(self) -> None:
        resp = ScheduleResponse(
            files=[
                FileStatus(object_name="file1.parquet", status="started"),
                FileStatus(object_name="file2.parquet", status="already_in_progress"),
            ]
        )
        assert len(resp.files) == 2
        assert resp.files[0].status == "started"

    def test_response_is_frozen(self) -> None:
        resp = ScheduleResponse(files=[])
        with pytest.raises(ValidationError):
            resp.files = []


class TestResumeResponse:
    """Tests for ResumeResponse model."""

    def test_response_with_resumed_jobs(self) -> None:
        resp = ResumeResponse(
            resumed=[
                ResumedJob(object_name="file1.parquet", restart_step="data_cleaning"),
            ]
        )
        assert len(resp.resumed) == 1
        assert resp.resumed[0].restart_step == "data_cleaning"


class TestJobState:
    """Tests for JobState model."""

    def test_default_state(self) -> None:
        state = JobState()
        assert state.current_step is None
        assert state.status == "pending"
        assert state.completed_steps == []
        assert state.failed_step is None

    def test_in_progress_state(self) -> None:
        state = JobState(
            current_step="data_cleaning",
            status="in_progress",
            completed_steps=["descriptive_statistics"],
        )
        assert state.current_step == "data_cleaning"
        assert state.completed_steps == ["descriptive_statistics"]

    def test_failed_state(self) -> None:
        state = JobState(
            current_step="temporal_analysis",
            status="failed",
            completed_steps=["descriptive_statistics", "data_cleaning"],
            failed_step="temporal_analysis",
        )
        assert state.failed_step == "temporal_analysis"
