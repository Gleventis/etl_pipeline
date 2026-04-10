"""Pydantic request and response models for the API server."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


# --- Files ---


class FileCreate(BaseModel):
    """Request to create or get a file record."""

    bucket: str = Field(min_length=1)
    object_name: str = Field(min_length=1)
    overall_status: str = Field(default="pending", min_length=1)


class FileUpdate(BaseModel):
    """Partial update for a file record."""

    overall_status: str | None = None
    total_computation_seconds: float | None = None
    total_elapsed_seconds: float | None = None
    retry_count: int | None = None


class FileResponse(BaseModel):
    """Single file response."""

    model_config = ConfigDict(frozen=True)

    file_id: int
    bucket: str
    object_name: str
    overall_status: str
    total_computation_seconds: float
    total_elapsed_seconds: float
    retry_count: int
    created_at: datetime
    updated_at: datetime


class FileListResponse(BaseModel):
    """Paginated list of files."""

    model_config = ConfigDict(frozen=True)

    files: list[FileResponse]
    total: int
    limit: int
    offset: int


# --- Job Executions ---


class JobExecutionCreate(BaseModel):
    """Request to create a single job execution."""

    file_id: int
    pipeline_run_id: str = Field(min_length=1)
    step_name: str = Field(min_length=1)
    status: str = Field(default="pending", min_length=1)
    retry_count: int = Field(default=0, ge=0)


class BatchExecutionItem(BaseModel):
    """Single execution item within a batch create request."""

    step_name: str = Field(min_length=1)
    status: str = Field(default="pending", min_length=1)
    retry_count: int = Field(default=0, ge=0)


class JobExecutionBatchCreate(BaseModel):
    """Request to create multiple job executions atomically."""

    file_id: int
    pipeline_run_id: str = Field(min_length=1)
    executions: list[BatchExecutionItem] = Field(min_length=1)


class JobExecutionUpdate(BaseModel):
    """Partial update for a job execution."""

    status: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    computation_time_seconds: float | None = None
    error_message: str | None = None


class JobExecutionResponse(BaseModel):
    """Single job execution response."""

    model_config = ConfigDict(frozen=True)

    job_execution_id: int
    file_id: int
    pipeline_run_id: str
    step_name: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    computation_time_seconds: float | None
    retry_count: int
    error_message: str | None
    created_at: datetime
    updated_at: datetime


class JobExecutionListResponse(BaseModel):
    """Paginated list of job executions."""

    model_config = ConfigDict(frozen=True)

    job_executions: list[JobExecutionResponse]
    total: int
    limit: int
    offset: int


class JobExecutionBatchResponse(BaseModel):
    """Response from batch job execution creation."""

    model_config = ConfigDict(frozen=True)

    job_execution_ids: list[int]
    created_count: int


# --- Analytical Results ---


class AnalyticalResultCreate(BaseModel):
    """Request to create an analytical result."""

    job_execution_id: int
    result_type: str = Field(min_length=1)
    summary_data: dict
    detail_s3_path: str | None = None
    computation_time_seconds: float = Field(ge=0)


class FileInfo(BaseModel):
    """Nested file info in analytical result responses."""

    model_config = ConfigDict(frozen=True)

    file_id: int
    bucket: str
    object_name: str


class AnalyticalResultResponse(BaseModel):
    """Single analytical result response."""

    model_config = ConfigDict(frozen=True)

    result_id: int
    job_execution_id: int
    result_type: str
    summary_data: dict
    detail_s3_path: str | None
    computation_time_seconds: float
    created_at: datetime
    file_info: FileInfo | None = None


class AnalyticalResultListResponse(BaseModel):
    """Paginated list of analytical results."""

    model_config = ConfigDict(frozen=True)

    results: list[AnalyticalResultResponse]
    total: int
    limit: int
    offset: int


# --- Metrics ---


class CheckpointSavingsFileResponse(BaseModel):
    """Checkpoint savings for a specific file."""

    model_config = ConfigDict(frozen=True)

    file_id: int
    object_name: str
    time_saved_seconds: float
    actual_computation_seconds: float
    percent_saved: float
    retry_count: int


class CheckpointSavingsAggregateResponse(BaseModel):
    """Aggregate checkpoint savings across all files."""

    model_config = ConfigDict(frozen=True)

    files_with_retries: int
    total_time_saved_seconds: float
    total_time_saved_hours: float
    avg_time_saved_per_file_seconds: float
    total_computation_seconds: float
    percent_saved: float


class StepFailureStatistic(BaseModel):
    """Failure statistics for a single step."""

    model_config = ConfigDict(frozen=True)

    step_name: str
    total_files_processed: int
    files_that_failed: int
    failure_rate_percent: float
    avg_retries_when_failed: float | None
    avg_computation_seconds: float | None


class FailureStatisticsResponse(BaseModel):
    """Failure statistics across all steps."""

    model_config = ConfigDict(frozen=True)

    statistics: list[StepFailureStatistic]


class PipelineSummaryResponse(BaseModel):
    """Comprehensive pipeline summary for thesis reporting."""

    model_config = ConfigDict(frozen=True)

    total_files: int
    files_with_retries: int
    retry_rate_percent: float
    avg_computation_minutes_per_file: float
    total_computation_hours: float
    total_hours_saved_by_checkpointing: float
    avg_minutes_saved_per_retry: float
    percent_time_saved: float


class StepPerformanceStatistic(BaseModel):
    """Performance statistics for a single analytical step."""

    model_config = ConfigDict(frozen=True)

    step_name: str
    executions: int
    avg_seconds: float
    min_seconds: float
    max_seconds: float
    stddev_seconds: float | None


class StepPerformanceResponse(BaseModel):
    """Step-level performance breakdown."""

    model_config = ConfigDict(frozen=True)

    statistics: list[StepPerformanceStatistic]


class PipelineEfficiencyStatistic(BaseModel):
    """Efficiency statistics for a single overall_status group."""

    model_config = ConfigDict(frozen=True)

    overall_status: str
    file_count: int
    avg_efficiency_ratio: float
    avg_computation_minutes: float
    avg_elapsed_minutes: float


class PipelineEfficiencyResponse(BaseModel):
    """Pipeline efficiency breakdown by status."""

    model_config = ConfigDict(frozen=True)

    statistics: list[PipelineEfficiencyStatistic]


class RecoveryTimeResponse(BaseModel):
    """Average recovery time improvement from checkpointing."""

    model_config = ConfigDict(frozen=True)

    avg_recovery_with_checkpoint_seconds: float
    avg_recovery_without_checkpoint_seconds: float
    avg_time_saved_seconds: float
    percent_improvement: float


# --- Step Dependencies ---


class StepDependencyEdge(BaseModel):
    """Single DAG edge in a step dependency graph."""

    step_name: str = Field(min_length=1)
    depends_on_step_name: str = Field(min_length=1)


class StepDependencyBatchCreate(BaseModel):
    """Request to batch-insert DAG edges for a pipeline run."""

    pipeline_run_id: str = Field(min_length=1)
    edges: list[StepDependencyEdge] = Field(min_length=1)


class StepDependencyBatchResponse(BaseModel):
    """Response from batch step dependency insertion."""

    model_config = ConfigDict(frozen=True)

    inserted: int


class StepDependencyResponse(BaseModel):
    """DAG edges for a pipeline run."""

    model_config = ConfigDict(frozen=True)

    pipeline_run_id: str
    edges: list[StepDependencyEdge]


if __name__ == "__main__":
    file_req = FileCreate(bucket="raw-data", object_name="test.parquet")
    print(f"FileCreate: {file_req.model_dump()}")

    batch = JobExecutionBatchCreate(
        file_id=1,
        pipeline_run_id="abc-123",
        executions=[
            BatchExecutionItem(step_name="descriptive_statistics"),
            BatchExecutionItem(step_name="data_cleaning"),
        ],
    )
    print(f"BatchCreate: {batch.model_dump()}")
