# Scheduler Checkpoint Configuration

## Overview
Extend the scheduler's API to accept an optional list of pipeline steps for which checkpointing should be disabled. When a step is in the skip list, the scheduler will still execute the step but will NOT persist state (via `save_job_state`) after that step completes. This means if the pipeline fails at a later step, it cannot resume from the skipped checkpoint — it must re-run from the last persisted checkpoint.

## Motivation
The thesis needs to measure pipeline recovery performance with and without checkpoints. Currently, checkpointing is hardcoded — every step always persists state. This feature enables selective checkpoint disabling for controlled experiments.

## API Change

### ScheduleRequest (modified)
Add an optional field:
- `skip_checkpoints: list[str] = []` — list of step names for which to skip checkpoint persistence. Valid values: `descriptive_statistics`, `data_cleaning`, `temporal_analysis`, `geospatial_analysis`, `fare_revenue_analysis`.

The field is optional with an empty list default, so existing callers are unaffected.

### ResumeRequest (unchanged)
Resume always uses persisted checkpoints. If a step was skipped, resume falls back to the last persisted checkpoint before the skipped step.

## Behavior

1. Caller sends `POST /scheduler/schedule` with `skip_checkpoints: ["temporal_analysis"]`
2. Scheduler processes files through all 5 steps as normal
3. After `temporal_analysis` completes successfully, the scheduler does NOT call `save_job_state`
4. All other steps checkpoint normally
5. If the pipeline fails at `geospatial_analysis`, resume will restart from `data_cleaning` (the last persisted checkpoint), not `temporal_analysis`

## Validation
- If `skip_checkpoints` contains an invalid step name, return 422 with the invalid step names listed
- Empty list (default) means all steps are checkpointed (current behavior)

## Affected Files
- `src/scheduler/src/server/models.py` — add `skip_checkpoints` field to `ScheduleRequest`
- `src/scheduler/src/server/routes.py` — pass `skip_checkpoints` through to service, validate step names
- `src/scheduler/src/services/scheduler.py` — thread `skip_checkpoints` to flow runs
- `src/scheduler/src/services/prefect_flows.py` — conditionally skip `save_job_state` calls
- `src/scheduler/src/services/pipeline.py` — add validation for step names
