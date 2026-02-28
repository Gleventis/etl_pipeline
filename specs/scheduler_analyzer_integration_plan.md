# Scheduler-Analyzer Integration Implementation Plan

Refactor the scheduler to work with the analyzer's per-step endpoints, passing `taxi_type` and `job_execution_id`.

## Context
- The analyzer exposes 5 step-specific endpoints: `POST /analyze/{step-name}`
- Each endpoint requires `input_bucket`, `input_object`, `taxi_type`, `job_execution_id`
- The scheduler currently sends to a single `POST /analyze` with only `job`, `input_bucket`, `input_object`
- `taxi_type` can be extracted from the object path (e.g., `yellow/2022/01/...` → `yellow`)
- `job_execution_id` must be created via the API Server before dispatching to the analyzer

## 1. Analyzer Client Update
- [x] Update `src/scheduler/src/services/analyzer_client.py`
  - Add `taxi_type` and `job_execution_id` to `AnalyzerRequest`
  - Rename `job` → `step` in `send_job()`
  - Route to per-step endpoints (`/analyze/descriptive-statistics`, etc.)
  - Add `_step_to_endpoint` helper
- [x] Update `tests/test_analyzer_client.py` — new contract, endpoint routing, validation
- [x] Verify: 19/19 tests pass, 90/90 full suite — **~25% context used**

## 2. Taxi Type Extraction
- [x] Create `src/scheduler/src/services/taxi_type.py`
  - `extract_taxi_type(object_name: str) -> str` — extract taxi type from object path prefix
  - Supports: `yellow`, `green`, `fhv`, `fhvhv`
  - Raises `ValueError` for unrecognized paths
- [x] Create `tests/test_taxi_type.py` — all 4 types, nested paths, invalid paths
- [x] Verify: tests pass via docker compose (100/100 pass, 10 new) — **~15% context used**

## 3. API Server Client for Job Executions
- [x] Create `src/scheduler/src/services/api_server_client.py`
  - `create_file_record(api_server_url, bucket, object_name) -> int` — returns `file_id`
  - `create_job_execution(api_server_url, file_id, pipeline_run_id, step_name) -> int` — returns `job_execution_id`
  - Uses `httpx.Client` as context manager
  - Reference: `specs/api_server.md` § `POST /files`, `POST /job-executions`
- [x] Create `tests/test_api_server_client.py` — success, HTTP error, network error
- [x] Verify: tests pass via docker compose (108/108 pass, 8 new) — **~25% context used**

## 4. Update Prefect Flows
- [x] Update `src/scheduler/src/services/prefect_flows.py`
  - `execute_step` now accepts and passes `taxi_type` and `job_execution_id` to `send_job()`
  - `process_file_flow` extracts `taxi_type` from object path, creates file record once, creates job execution per step via API Server client
  - Added `pipeline_run_id` parameter to `process_file_flow`
- [x] Update `tests/test_prefect_flows.py` — new parameters, API Server mocking (12 tests: 3 execute_step + 9 process_file_flow)
- [x] Verify: tests pass via docker compose (111/111 pass, 3 new) — **~25% context used**

## 5. Update Settings (if needed)
- [x] Update `src/scheduler/src/services/config.py`
  - Add `API_SERVER_URL` setting (needed for job execution creation)
- [x] Update `tests/test_config.py` — new setting default and override
- [x] Verify: tests pass via docker compose (108/108 pass) — **~15% context used**

## 6. Verify Full Suite
- [x] Run full scheduler test suite
- [x] Verify: all tests pass via docker compose (111/111 pass, 32 warnings from Prefect logger) — **~25% context used**
