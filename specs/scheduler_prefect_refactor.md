# Scheduler Prefect Refactor

## Overview
Refactor the scheduler service to use Prefect for pipeline orchestration, replacing the custom `ThreadPoolExecutor` + in-memory hashmap approach. A self-hosted Prefect server provides observability, monitoring, and flow visualization. The existing Postgres job state table is retained as an audit trail for querying, reporting, and debugging.

## Motivation
- Better observability and monitoring via Prefect's dashboard/UI
- Replace custom thread pool and state management with a battle-tested orchestration framework
- Gain built-in flow visualization, logging, and run history
- Simplify the codebase by offloading orchestration concerns to Prefect

## Architecture Changes

### What changes
- `src/scheduler/src/services/scheduler.py` — `SchedulerService` rewritten to create Prefect flow runs instead of using `ThreadPoolExecutor` (`specs/scheduler_implementation_plan.md` § 8)
- `src/scheduler/src/services/state_manager.py` — in-memory hashmap (`_jobs` dict) removed; Postgres persistence retained as write-only audit trail (`specs/scheduler_implementation_plan.md` § 6)
- `src/scheduler/src/services/config.py` — `SCHEDULER_THREAD_POOL_SIZE` replaced with `PREFECT_API_URL` (`specs/scheduler_implementation_plan.md` § 2)
- `src/scheduler/pyproject.toml` — add `prefect` dependency (`specs/scheduler_implementation_plan.md` § 1)
- `src/infrastructure/scheduler/docker-compose.yml` — add Prefect server container, replace `SCHEDULER_THREAD_POOL_SIZE` with `PREFECT_API_URL` (`specs/scheduler_implementation_plan.md` § 11)
- `src/scheduler/Dockerfile` — unchanged (uv handles new dependency)

### What stays the same
- `src/scheduler/src/server/routes.py` — `POST /schedule` and `POST /resume` endpoints unchanged (`specs/scheduler.md` § API)
- `src/scheduler/src/server/main.py` — FastAPI app and lifespan unchanged
- `src/scheduler/src/services/pipeline.py` — step definitions and bucket resolution unchanged (`specs/scheduler.md` § Pipeline Steps)
- `src/scheduler/src/services/analyzer_client.py` — HTTP client for analyzer unchanged (`specs/scheduler.md` § Analyzer Request)
- `src/scheduler/src/services/database.py` — Postgres persistence layer unchanged (`specs/scheduler_implementation_plan.md` § 4)
- `src/scheduler/src/server/models.py` — request/response models unchanged (`specs/scheduler.md` § API)
- `src/data_collector/` — entirely unchanged (`specs/data_collection.md`)

## Prefect Deployment

### Self-hosted Prefect server
- Runs as a container in docker-compose alongside the scheduler and Postgres
- Provides the Prefect UI dashboard on port 4200
- Scheduler connects via `PREFECT_API_URL` environment variable

## Orchestration Model

### Flow per file
- `POST /schedule` receives a batch of objects and triggers one Prefect flow run per file (`specs/scheduler.md` § Processing Flow)
- Each flow run walks a single file through the 5 pipeline steps sequentially as Prefect tasks
- The endpoint blocks until all flow runs complete, then returns per-file status

### Prefect flow definition
- One `@flow` function: `process_file_flow(object_name, bucket, settings, db_url, start_step=None)`
- One `@task` function per pipeline step dispatch: `execute_step(step, input_bucket, object_name, analyzer_url)`
- Tasks are called sequentially within the flow (no task-level concurrency within a file)

### Concurrency
- Multiple flow runs execute concurrently (Prefect handles this natively)
- `POST /schedule` triggers all flow runs, then waits for all to complete

## State Management

### Postgres audit trail (retained)
- Every state change (job created, step completed, step failed, job completed) is persisted to the `job_state` table (`specs/scheduler_implementation_plan.md` § 4)
- The `StateManager` class is simplified: removes the in-memory `_jobs` dict, becomes a thin wrapper around `database.py` for writes
- `get_failed_jobs()` still reads from Postgres for the resume flow

### In-memory hashmap (removed)
- The `_jobs: dict[str, JobState]` in `StateManager` is removed
- Prefect tracks active flow/task state natively

## Resume Flow
- `POST /resume` reads failed jobs from Postgres via `database.py` (`specs/scheduler.md` § API)
- For each failed job, triggers a new Prefect flow run with `start_step` set to the `failed_step`
- The flow skips already-completed steps and resumes from the failed step
- Blocks until all resumed flow runs complete

## Retry Policy
- No automatic task-level retries — a failed step immediately marks the job as failed in Postgres
- Recovery is manual via `POST /resume` (`specs/scheduler.md` § Error Handling)

## Configuration (Environment Variables)
| Variable | Description | Replaces |
|---|---|---|
| `PREFECT_API_URL` | Prefect server URL (e.g., `http://prefect-server:4200/api`) | `SCHEDULER_THREAD_POOL_SIZE` |
| `ANALYZER_URL` | Analyzer service URL | — (unchanged) |
| `DATABASE_URL` | Postgres connection string | — (unchanged) |
| `SERVER_HOST` | FastAPI server host | — (unchanged) |
| `SERVER_PORT` | FastAPI server port | — (unchanged) |
| `STEP_*_BUCKET` | Step-to-bucket mapping (5 vars) | — (unchanged) |

## Tech Stack Changes
- Add: `prefect` (orchestration framework)
- Remove: `concurrent.futures.ThreadPoolExecutor` usage (stdlib, no dependency change)
