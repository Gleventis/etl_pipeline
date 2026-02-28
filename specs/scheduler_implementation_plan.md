# Scheduler — Implementation Plan

## 1. Project Setup — ✅ DONE (2026-03-01) — ~15% context used
- [x] Scaffold `src/scheduler/` per repo structure (`specs/README.md` § Overall requirements #5)
  - `pyproject.toml` — dependencies: fastapi, uvicorn, pydantic, pydantic-settings, httpx, psycopg2-binary
  - `src/server/` — FastAPI app and routes
  - `src/services/` — business logic
  - `tests/` — pytest + testcontainers
- [x] Create `Dockerfile` and `docker-compose.yml` (`specs/README.md` § Overall requirements #3)

## 2. Configuration — ✅ DONE (2026-03-01) — ~15% context used
- [x] Create `src/scheduler/src/services/config.py` — Pydantic `BaseSettings` model loading env vars (`specs/scheduler.md` § Configuration)
  - `ANALYZER_URL`, `SCHEDULER_THREAD_POOL_SIZE`, `DATABASE_URL`, `SERVER_HOST`, `SERVER_PORT`
  - Step-to-bucket mapping: `STEP_DESCRIPTIVE_STATISTICS_BUCKET`, `STEP_DATA_CLEANING_BUCKET`, `STEP_TEMPORAL_ANALYSIS_BUCKET`, `STEP_GEOSPATIAL_ANALYSIS_BUCKET`, `STEP_FARE_REVENUE_ANALYSIS_BUCKET`

## 3. Request/Response Models — ✅ DONE (2026-03-01) — ~15% context used
- [x] Create `src/scheduler/src/server/models.py` — Pydantic models (`specs/scheduler.md` § API)
  - `ScheduleRequest`: bucket (str), objects (list[str])
  - `ScheduleResponse`: per-file status (started / already in progress)
  - `ResumeResponse`: list of resumed jobs with restart step
  - `JobState`: current_step, status, completed_steps, failed_step

## 4. Database Layer — ✅ DONE (2026-03-01) — ~25% context used
- [x] Create `src/scheduler/src/services/database.py` — Postgres job state persistence (`specs/scheduler.md` § Job State)
  - Table schema: `job_id`, `object_name`, `bucket`, `current_step`, `status`, `completed_steps` (JSONB), `failed_step`, `created_at`, `updated_at`
  - UNIQUE constraint on `(object_name, bucket)` for upsert semantics
  - Functions: `save_job_state()` (upsert), `get_failed_jobs()`, `get_job_history()`
  - `JobRecord` frozen Pydantic model for typed row representation
  - `get_connection()` context manager with configurable DSN
  - `init_schema()` for idempotent table creation
  - 10 integration tests against docker-compose Postgres
  - Initially uses direct Postgres connection; migrate to API Server when available (`specs/README.md` § API Server)

## 5. Pipeline Definition — ✅ DONE (2026-03-02) — ~20% context used
- [x] Create `src/scheduler/src/services/pipeline.py` — pipeline step definitions (`specs/scheduler.md` § Pipeline Steps)
  - Ordered list of step names: `descriptive_statistics`, `data_cleaning`, `temporal_analysis`, `geospatial_analysis`, `fare_revenue_analysis`
  - Function to resolve input bucket for a given step from config (`specs/scheduler.md` § Step-to-Bucket Mapping)
  - Function to determine next step given completed steps
  - 16 unit tests (50 total scheduler tests passing)

## 6. Job State Manager — ✅ DONE (2026-03-02) — ~30% context used
- [x] Create `src/scheduler/src/services/state_manager.py` — in-memory hashmap + Postgres sync (`specs/scheduler.md` § Job State)
  - In-memory dict tracking active jobs
  - Methods: `create_job()`, `update_step()`, `mark_completed()`, `mark_failed()`, `get_state()`, `get_failed_jobs()`
  - Each state change persists to Postgres via database layer
  - 18 integration tests (68 total scheduler tests passing)

## 7. Analyzer Client — ✅ DONE (2026-03-02) — ~25% context used
- [x] Create `src/scheduler/src/services/analyzer_client.py` — HTTP client for analyzer (`specs/scheduler.md` § Analyzer Request)
  - `AnalyzerRequest` / `AnalyzerResponse` frozen Pydantic models
  - `send_job()` sends synchronous POST to analyzer with `job`, `input_bucket`, `input_object`
  - Accepts `analyzer_url` as parameter (caller passes from config)
  - Handles `HTTPStatusError` and `HTTPError` gracefully, returns `AnalyzerResponse(success=False, error=...)`
  - 11 unit tests (79 total scheduler tests passing)

## 8. Scheduler Service (Core Logic) — ✅ DONE (2026-03-02) — ~35% context used
- [x] Create `src/scheduler/src/services/scheduler.py` — orchestration logic (`specs/scheduler.md` § Processing Flow)
  - `SchedulerService` facade class coordinating StateManager, analyzer_client, pipeline
  - `schedule_batch()`: receives list of objects + bucket, creates jobs, processes via thread pool
  - `process_file()`: walks a single file through all 5 steps sequentially
  - `resume_failed()`: reads failed jobs from Postgres, fast-forwards completed steps, restarts from failed step
  - Uses `ThreadPoolExecutor` with configurable size (`specs/scheduler.md` § Configuration)
- [x] Wire routes to `SchedulerService` via `app.state` + lifespan context manager
- [x] Update route tests to mock `SchedulerService`
- [x] 13 new integration tests, 93 total scheduler tests passing

## 9. FastAPI Server — ✅ DONE (2026-03-01) — ~15% context used
- [x] Create `src/scheduler/src/server/main.py` — FastAPI app (`specs/scheduler.md` § API)
- [x] Create `src/scheduler/src/server/routes.py` — endpoints
  - `POST /schedule` — validate request, call `schedule_batch()`
  - `POST /resume` — call `resume_failed()`

## 10. Entrypoint — ✅ DONE (2026-03-01) — ~15% context used
- [x] Create `src/scheduler/src/main.py` — uvicorn startup using config

## 11. Docker — ✅ DONE (2026-03-01) — ~15% context used
- [x] `src/scheduler/Dockerfile` — Python 3.12, uv install, run uvicorn (`specs/README.md` § Overall requirements #3)
- [x] `src/infrastructure/scheduler/docker-compose.yml` — scheduler + Postgres services (`specs/README.md` § Overall requirements #5)
  - Mounts utilities volume and sets PYTHONPATH for cross-service imports

## 12. Data Collector Integration — ✅ DONE (2026-03-02) — ~40% context used
- [x] Update `src/data_collector/src/services/config.py` — add `SCHEDULER_URL` env var (`specs/scheduler.md` § Processing Flow #1)
- [x] Create `src/data_collector/src/services/scheduler_client.py` — `notify_scheduler()` HTTP client
- [x] Update data collector to call `POST /schedule` after successful uploads to MinIO
  - Sends list of uploaded S3 keys + bucket to the scheduler
- [x] Update `src/infrastructure/data_collector/docker-compose.yml` — add `SCHEDULER_URL` env var
- [x] 5 new scheduler client unit tests, 81 total data collector tests passing

## 13. Tests
- Create `src/scheduler/tests/` — pytest + testcontainers (`specs/README.md` § Overall requirements #4)
  - Unit tests: pipeline step resolution, state manager, request/response models
  - Integration tests: full pipeline flow with mocked analyzer, real Postgres via testcontainers
  - Test resume flow: create failed job in Postgres, call `/resume`, verify restart from correct step
