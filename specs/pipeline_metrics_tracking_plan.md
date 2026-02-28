# Pipeline Metrics Tracking Implementation Plan

Wire the scheduler's `process_file_flow` to update job execution status/timing and file aggregates via the API Server PATCH endpoints, so thesis metrics return real data.

## Problem
The `process_file_flow` creates file records and job executions but never updates them after steps complete/fail. The `files` table fields (`overall_status`, `total_computation_seconds`, `total_elapsed_seconds`, `retry_count`) and `job_executions` fields (`status`, `started_at`, `completed_at`, `computation_time_seconds`) remain at their defaults. All thesis metrics queries depend on these fields.

## API Client Functions
- [x] Step 1: Add `update_job_execution` and `update_file` to `src/scheduler/src/services/api_server_client.py` with tests — **~25% context used**
  - 10 new tests (5 per function)
  - Verify: 121 scheduler tests pass ✅

## Wire into process_file_flow
- [x] Step 2: Update `process_file_flow` to call `update_job_execution` before/after each step — **~25% context used**
  - Before step: PATCH status="running", started_at=now
  - After success: PATCH status="completed", completed_at=now, computation_time_seconds
  - After failure: PATCH status="failed", completed_at=now, error_message
  - Verify: existing + new tests pass ✅

- [x] Step 3: Update `process_file_flow` to call `update_file` for file aggregates — **~40% context used**
  - On start: PATCH overall_status="in_progress"
  - After each step: PATCH total_computation_seconds (cumulative)
  - On all complete: PATCH overall_status="completed", total_elapsed_seconds
  - On failure: PATCH overall_status="failed", total_elapsed_seconds
  - On resume (start_step != None): PATCH retry_count (increment)
  - Verify: 121 scheduler tests pass ✅

- [x] Step 4: Update `tests/test_prefect_flows.py` for new API client calls — **~45% context used**
  - Mock `update_job_execution` and `update_file`
  - Verify correct calls at each lifecycle point
  - Verify: 121 scheduler tests pass ✅
