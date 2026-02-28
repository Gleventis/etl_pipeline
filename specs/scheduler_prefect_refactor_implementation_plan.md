# Scheduler Prefect Refactor — Implementation Plan

## 1. Add Prefect Dependency
- [x] Add `prefect` to `src/scheduler/pyproject.toml` dependencies (`specs/scheduler_prefect_refactor.md` § Tech Stack Changes) — **~15% context used**
- [x] Run `uv sync` to update `uv.lock` — **~15% context used**

## 2. Update Configuration
- [x] In `src/scheduler/src/services/config.py`: add `PREFECT_API_URL` field with default `http://localhost:4200/api` (`specs/scheduler_prefect_refactor.md` § Configuration) — **~15% context used**
- [x] In `src/scheduler/src/services/config.py`: remove `SCHEDULER_THREAD_POOL_SIZE` field (`specs/scheduler_prefect_refactor.md` § Architecture Changes) — **~15% context used**
- [x] Update `src/scheduler/tests/test_config.py`: replace `SCHEDULER_THREAD_POOL_SIZE` tests with `PREFECT_API_URL` tests — **~15% context used**
- [x] Hardcoded `max_workers=4` in `scheduler.py._process_concurrently()` to keep existing tests passing (code removed in Step 6) — **~15% context used**

## 3. Define Prefect Task — `execute_step`
- [x] Create `src/scheduler/src/services/prefect_flows.py` — **~25% context used**
- [x] Define `@task` function `execute_step(step, input_bucket, object_name, analyzer_url)` (`specs/scheduler_prefect_refactor.md` § Orchestration Model) — **~25% context used**
  - Calls `send_job()` from `src/scheduler/src/services/analyzer_client.py` (`specs/scheduler.md` § Analyzer Request)
  - Returns `AnalyzerResponse`
  - No automatic retries (`specs/scheduler_prefect_refactor.md` § Retry Policy)

## 4. Define Prefect Flow — `process_file_flow`
- [x] In `src/scheduler/src/services/prefect_flows.py`: define `@flow` function `process_file_flow(object_name, bucket, settings, db_url, start_step=None)` (`specs/scheduler_prefect_refactor.md` § Orchestration Model) — **~35% context used**
- Flow logic:
  - Open Postgres connection, resolve start step from `start_step` param or begin at first step (`specs/scheduler_prefect_refactor.md` § Resume Flow)
  - Determine completed steps (all steps before `start_step`) using `src/scheduler/src/services/pipeline.py` `STEPS` list
  - Persist initial job state to Postgres via `src/scheduler/src/services/database.py` `save_job_state()` (`specs/scheduler_prefect_refactor.md` § State Management)
  - Walk remaining steps sequentially, calling `execute_step` task for each
  - After each successful step: persist updated state to Postgres via `save_job_state()`
  - On failure: persist failed state to Postgres via `save_job_state()`, stop processing (`specs/scheduler_prefect_refactor.md` § Retry Policy)
  - On all steps complete: persist completed state to Postgres

## 5. Simplify StateManager
- [x] In `src/scheduler/src/services/state_manager.py`: remove `_jobs: dict[str, JobState]` in-memory hashmap (`specs/scheduler_prefect_refactor.md` § State Management) — **~40% context used**
- [x] Remove methods: `create_job()`, `update_step()`, `mark_completed()`, `mark_failed()`, `get_state()`, `_get()`, `_persist()` — **~40% context used**
- [x] Remove `bucket` parameter from constructor (no longer needed) — **~40% context used**
- [x] Keep `get_failed_jobs()` — delegates to `database.py` for resume flow (`specs/scheduler_prefect_refactor.md` § Resume Flow) — **~40% context used**
- [x] Update `src/scheduler/tests/test_state_manager.py`: remove in-memory state tests, keep `get_failed_jobs()` tests — **~40% context used**

## 6. Rewrite SchedulerService
- [x] In `src/scheduler/src/services/scheduler.py`: remove `ThreadPoolExecutor` and `_process_concurrently()` (`specs/scheduler_prefect_refactor.md` § Architecture Changes) — **~55% context used**
- [x] Rewrite `schedule_batch()`: check for in-progress flow runs via Postgres, trigger `process_file_flow` per file, wait for completion — **~55% context used**
- [x] Rewrite `resume_failed()`: read failed jobs from Postgres via `StateManager.get_failed_jobs()`, trigger `process_file_flow` per failed job with `start_step=failed_step` — **~55% context used**
- [x] Remove `process_file()` method — logic moved to `process_file_flow` in step 4 — **~55% context used**
- [x] Update constructor: remove `conn` parameter, accept `db_url` string instead (flows open their own connections) — **~55% context used**
- [x] Add `get_in_progress_jobs()` to `database.py` and `StateManager` for in-progress check — **~55% context used**
- [x] Update `src/scheduler/tests/test_scheduler.py`: mock `process_file_flow` instead of `send_job` + `ThreadPoolExecutor` — **~55% context used**
- [x] Update `src/scheduler/tests/test_state_manager.py`: add `get_in_progress_jobs()` tests — **~55% context used**

## 7. Update FastAPI Lifespan
- [x] In `src/scheduler/src/server/main.py`: update `lifespan()` to set `PREFECT_API_URL` env var from settings (`specs/scheduler_prefect_refactor.md` § Prefect Deployment) — **~60% context used**
- [x] Update `SchedulerService` construction: pass `db_url` string instead of `conn` object — **~60% context used**
- [x] Remove `get_connection()` from lifespan (flows manage their own connections) — **~60% context used**
- [x] Keep `init_schema()` call to ensure table exists on startup (uses temporary connection) — **~60% context used**

## 8. Add Prefect Server to Docker Compose
- [x] In `src/infrastructure/scheduler/docker-compose.yml`: add `prefect-server` service (`specs/scheduler_prefect_refactor.md` § Prefect Deployment) — **~65% context used**
  - Image: `prefecthq/prefect:3-latest`
  - Command: `prefect server start --host 0.0.0.0`
  - Port: `4200:4200`
  - Healthcheck via python urllib (curl not available in image)
- [x] Update `scheduler` service: — **~65% context used**
  - Add `PREFECT_API_URL=http://prefect-server:4200/api` env var
  - Remove `SCHEDULER_THREAD_POOL_SIZE` env var
  - Add `depends_on: prefect-server` with health condition

## 9. Update Tests
- [x] Update `src/scheduler/tests/test_scheduler.py`: mock `process_file_flow` instead of `send_job` + thread pool (`specs/scheduler_prefect_refactor.md` § Orchestration Model) — completed in Step 6
- [x] Update `src/scheduler/tests/test_state_manager.py`: remove in-memory hashmap tests, keep Postgres `get_failed_jobs()` integration tests — completed in Step 5
- [x] Update `src/scheduler/tests/test_config.py`: test `PREFECT_API_URL` instead of `SCHEDULER_THREAD_POOL_SIZE` — completed in Step 2
- [x] Add `src/scheduler/tests/test_prefect_flows.py`: unit tests for `process_file_flow` and `execute_step` with mocked analyzer client and database — completed in Steps 3–4

## 10. Create Service README
- [x] Create `src/scheduler/README.md` with service name, endpoints, configuration, how to run, and how to test — **~70% context used**

## 11. Fix Concurrent Flow Execution
- [x] `schedule_batch()` and `resume_failed()` were calling `process_file_flow` sequentially — spec requires concurrent execution (`specs/scheduler_prefect_refactor.md` § Concurrency) — **~75% context used**
- [x] Extracted `_run_flows_concurrently()` using `ThreadPoolExecutor` to submit all flow runs in parallel and `wait()` for completion — **~75% context used**
- [x] Updated `test_passes_correct_args_to_flow` to expect `start_step=None` kwarg — **~75% context used**
