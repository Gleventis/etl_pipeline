"""Request and response models for the scheduler API."""

from pydantic import BaseModel, ConfigDict, Field


class StepDefinition(BaseModel):
    """A single step in a DAG pipeline definition.

    Attributes:
        name: Step identifier (e.g. 'data_cleaning').
        action: Analytical step type (e.g. 'DATA_CLEANING').
        checkpoint: Whether to persist state after this step.
        after: Names of steps this step depends on.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    action: str = Field(min_length=1)
    checkpoint: bool = True
    after: list[str] = Field(default_factory=list)


class ScheduleRequest(BaseModel):
    """Request to schedule analytical pipeline for a batch of files."""

    bucket: str = Field(min_length=1)
    objects: list[str] = Field(min_length=1)
    skip_checkpoints: list[str] = Field(default_factory=list)
    steps: list[StepDefinition] | None = None


class FileStatus(BaseModel):
    """Status of a single file in the schedule response."""

    model_config = ConfigDict(frozen=True)

    object_name: str
    status: str  # "started" or "already_in_progress"


class ScheduleResponse(BaseModel):
    """Response from the schedule endpoint."""

    model_config = ConfigDict(frozen=True)

    files: list[FileStatus]


class ResumedJob(BaseModel):
    """A single resumed job in the resume response."""

    model_config = ConfigDict(frozen=True)

    object_name: str
    restart_step: str


class ResumeResponse(BaseModel):
    """Response from the resume endpoint."""

    model_config = ConfigDict(frozen=True)

    resumed: list[ResumedJob]


class JobState(BaseModel):
    """Current state of a pipeline job for a single file."""

    current_step: str | None = None
    status: str = "pending"  # "pending", "in_progress", "completed", "failed"
    completed_steps: list[str] = Field(default_factory=list)
    failed_step: str | None = None


if __name__ == "__main__":
    req = ScheduleRequest(bucket="raw-data", objects=["file1.parquet"])
    print(f"ScheduleRequest: {req.model_dump()}")

    req_skip = ScheduleRequest(
        bucket="raw-data",
        objects=["file1.parquet"],
        skip_checkpoints=["temporal_analysis"],
    )
    print(f"ScheduleRequest with skip: {req_skip.model_dump()}")

    req_dag = ScheduleRequest(
        bucket="raw-data",
        objects=["file1.parquet"],
        steps=[
            StepDefinition(
                name="descriptive_statistics", action="DESCRIPTIVE_STATISTICS"
            ),
            StepDefinition(
                name="data_cleaning",
                action="DATA_CLEANING",
                after=["descriptive_statistics"],
            ),
        ],
    )
    print(f"ScheduleRequest with DAG: {req_dag.model_dump()}")

    state = JobState(current_step="data_cleaning", status="in_progress")
    print(f"JobState: {state.model_dump()}")
