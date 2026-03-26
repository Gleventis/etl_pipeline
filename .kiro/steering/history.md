# Change History

## 2026-02-28

**Summary**: Scaffolded data_collector service with project structure, Dockerfile, docker-compose.yml, and configuration module.

**Change Type**: `feat`

**Context / Motivation**: First step of the data collection implementation plan. Nothing existed yet — needed project scaffolding and a configuration foundation before any business logic could be built.

**Decisions Made**:
- Used hatchling as build backend with `packages = ["src"]` to match the nested source layout.
- Defaults for MinIO credentials set to `minioadmin` for local dev convenience.
- `THREAD_POOL_SIZE` constrained to `ge=1`, `SERVER_PORT` to valid port range.
- Tests clear docker-injected env vars with `monkeypatch.delenv` to test true defaults.

**Lessons**: Hatchling requires explicit `packages` config when source layout doesn't match project name. See lessons.md.

## 2026-02-28

**Summary**: Added request/response Pydantic models for `POST /collect` endpoint — `CollectRequest`, `CollectResponse`, supporting types.

**Change Type**: `feat`

**Context / Motivation**: Step 3 of the data collection implementation plan. These models define the API contract that URL generation, download service, and routes all depend on.

**Decisions Made**:
- `YearField`/`MonthField` use `model_validator(mode="before")` to accept both `int` and `{"from": N, "to": N}` JSON shapes, matching the spec's flexible input format.
- `IntRange` uses `alias="from"` since `from` is a Python keyword.
- All models are frozen (immutable DTOs).
- `TaxiType` is a `StrEnum` for direct JSON serialization compatibility.
- `expand()` methods on year/month fields produce the list of values for downstream iteration.

**Lessons**: None new — clean implementation.

## 2026-02-28

**Summary**: Added URL generator for TLC trip data downloads — `generate_urls()` function.

**Change Type**: `feat`

**Context / Motivation**: Step 5 of the data collection implementation plan. Pure function that produces download URLs from year/month/taxi_type combinations. Prerequisite for the download service (Step 6).

**Decisions Made**:
- Single function, no class — simplicity first for a stateless URL builder.
- `TaxiType.ALL` expands to the 4 concrete types internally.
- URL pattern matches TLC CDN: `https://d37ci6vzurychx.cloudfront.net/trip-data/<type>_tripdata_<year>-<month>.parquet`.
- No year/month validation beyond what the models already enforce — if TLC doesn't have the file, the download step will report it as a failure.

**Lessons**: None new — straightforward implementation.

## 2026-02-28

**Summary**: Added parquet schema validation per taxi type — `validate_parquet_schema()` and `EXPECTED_COLUMNS` definitions.

**Change Type**: `feat`

**Context / Motivation**: Step 4 of the data collection implementation plan. Schema validation is a prerequisite for the download service (Step 6) — downloaded files must be verified before upload to MinIO.

**Decisions Made**:
- Column-name-only validation (no type checking). Real TLC data shows type drift across years (e.g., `int32` vs `int64`, `Airport_fee` vs `airport_fee`). Strict type matching would false-reject valid files.
- Case-insensitive matching — all expected columns stored lowercase, actual columns lowercased before comparison.
- Subset check — extra columns in the file are allowed (TLC adds fields over time). Only missing expected columns are errors.
- Expected column sets derived from actual 2024-01 TLC parquet files, cross-checked against 2020 and 2023 for stability.

**Lessons**: None new — schema drift in real-world data sources means validation should be lenient on types and casing while strict on required column presence.

## 2026-02-28

**Summary**: Added download service — `download_one()`, `download_batch()`, and `parse_url_metadata()` for fetching and validating TLC parquet files.

**Change Type**: `feat`

**Context / Motivation**: Step 6 of the data collection implementation plan. The download service is the core business logic connecting URL generation → HTTP download → schema validation. It's the prerequisite for S3 upload (Step 7) and the FastAPI route (Step 8).

**Decisions Made**:
- Used `dataclass` (not Pydantic) for `DownloadResult` — it's an internal container, not a boundary model. Frozen + slots for immutability and memory efficiency.
- `download_one` creates a fresh `httpx.Client` per call — each thread in the pool gets its own client, avoiding shared state. Acceptable overhead for file downloads.
- S3 upload is NOT in the downloader — separation of concerns. The orchestrator (route handler) will take successful `DownloadResult.file_bytes` and upload them. This keeps the downloader testable without MinIO.
- `parse_url_metadata` extracted as a separate function for testability and reuse.
- Invalid URL patterns return a failed `DownloadResult` instead of raising — consistent with partial-success design.

**Lessons**: None new — clean implementation.

## 2026-02-28

**Summary**: Added shared S3 utility — `create_s3_client()`, `ensure_bucket()`, `upload_object()`, `download_object()`, `build_s3_key()`.

**Change Type**: `feat`

**Context / Motivation**: Step 7 of the data collection implementation plan. Shared utility in `src/utilities/s3.py` for all services to interact with MinIO/S3. The data collector needs this to upload validated parquet files after download.

**Decisions Made**:
- Placed in `src/utilities/` as a shared package, not inside data_collector — multiple services will need S3 access.
- Created `pyproject.toml`, `Dockerfile`, and `docker-compose.yml` for utilities to enable independent testing with MinIO.
- Functions take an explicit `client` parameter (dependency injection) rather than creating clients internally — testable and composable.
- `build_s3_key` is a pure function producing the `<taxi_type>/<year>/<month>/<filename>.parquet` key format from the spec.
- `ensure_bucket` is idempotent — uses `head_bucket` to check existence before creating.
- Integration tests run against real MinIO from docker-compose, with per-test bucket cleanup.

**Lessons**: None new — clean implementation.

## 2026-02-28

**Summary**: Added FastAPI server (`app.py`), `POST /collector/collect` route (`routes.py`), and uvicorn entrypoint (`main.py`). Updated docker-compose to mount utilities.

**Change Type**: `feat`

**Context / Motivation**: Steps 8-9 of the data collection implementation plan. This is the glue that wires all existing components together: URL generation → concurrent download → schema validation → S3 upload → response. Without the server, none of the previously built components were usable end-to-end.

**Decisions Made**:
- Route creates S3 client per request rather than at module level — avoids stale connections and makes testing straightforward with mocks.
- `ensure_bucket` called on every request — idempotent, negligible overhead, prevents "bucket not found" errors on first run.
- Upload failures are caught per-file and reported as failures in the response (not raised) — consistent with partial-success design from the spec.
- Utilities mounted via docker-compose volume (`/utilities`) with `PYTHONPATH=/app:/` — avoids needing utilities as a pip dependency during development.
- All 7 route tests use `unittest.mock.patch` to mock download_batch and S3 functions — fast, no external dependencies.
- Router uses `/collector` prefix to namespace the data collector endpoints.

**Lessons**: None new — clean implementation. All 70 tests pass.

## 2026-02-28

**Summary**: Added integration tests for `POST /collect` route with real MinIO — verifying the S3 upload path end-to-end.

**Change Type**: `test`

**Context / Motivation**: All 10 implementation plan steps were marked ✅, but the route tests (`test_routes.py`) mocked everything including S3. The spec requires "TestContainers and pytest for testing" — the data collector had no tests exercising real infrastructure. The most important gap was verifying that the upload path actually works against MinIO.

**Decisions Made**:
- Mock only `download_batch` (avoids hitting TLC CDN, which is slow/flaky). Use real MinIO for S3 operations.
- Patch `SETTINGS.MINIO_BUCKET` to an isolated test bucket (`test-integration`) to avoid polluting the default bucket.
- Per-test bucket cleanup via fixture teardown — delete all objects then the bucket.
- 3 focused tests: single upload roundtrip, partial failure (only successes uploaded), multiple files uploaded.
- Import `ClientError` inline in the test that needs it (matches existing test style in `test_s3.py`).

**Lessons**: None new — straightforward integration test pattern.

## 2026-02-28

**Summary**: Added Swagger UI accessibility tests — verifying `/docs` and `/openapi.json` endpoints are reachable.

**Change Type**: `test`

**Context / Motivation**: User requested verification that the data_collector service has a Swagger page accessible through the browser. FastAPI serves Swagger UI at `/docs` by default, but no test existed to assert this.

**Decisions Made**:
- 3 tests: `/docs` returns 200, `/docs` HTML contains `swagger-ui`, `/openapi.json` returns 200 with correct title/version.
- No mocking needed — these are framework-level endpoints with no external dependencies.
- Validates the OpenAPI schema metadata matches `app.py` configuration (`title="Data Collector"`, `version="0.1.0"`).

**Lessons**: None new — straightforward test.

## 2026-02-28

**Summary**: Added steering rule enforcing `main.py` over `app.py` as the FastAPI entrypoint for services under `/src`.

**Change Type**: `docs`

**Context / Motivation**: User wants a consistent convention where every FastAPI service uses `main.py` (not `app.py`) as its entrypoint. This standardizes the project structure across services.

**Decisions Made**:
- Rule stored at `.kiro/steering/rules/fastapi_main_entrypoint.md` per MDC Rule Management directive.
- Glob targets `**/src/**/main.py` and `**/src/**/app.py` so it triggers when either file is touched.
- `alwaysApply: true` since this is a structural convention that should always be enforced.
- Rule scoped to FastAPI services only — non-FastAPI modules are unaffected.

**Lessons**: None new.

## 2026-02-28

**Summary**: Added steering rule requiring context window usage estimation on completed implementation plan steps.

**Change Type**: `docs`

**Context / Motivation**: User wants visibility into how much context is consumed per plan step, helping decide when to reset context mid-plan.

**Decisions Made**:
- Format: `— **~XX% context used**` appended to the completed step line.
- `alwaysApply: true` since this applies to any plan execution regardless of service.
- Glob targets `**/*.md` since implementation plans are markdown files.

**Lessons**: None new.

## 2026-03-01

**Summary**: Added steering rule requiring a `README.md` for every service once its implementation plan is complete.

**Change Type**: `docs`

**Context / Motivation**: User wants each service to have a README with key info and run instructions, created as the final step of any service implementation plan.

**Decisions Made**:
- Rule stored at `.kiro/steering/rules/service_readme.md`.
- `alwaysApply: true` — applies to all service implementations.
- README scope kept minimal: purpose, endpoints, config, how to run, how to test.

**Lessons**: None new.

## 2026-03-01

**Summary**: Scaffolded scheduler service — project setup, config, request/response models, FastAPI app with stub routes, Dockerfile, docker-compose with Postgres.

**Change Type**: `feat`

**Context / Motivation**: Steps 1-3, 9-11 of the scheduler implementation plan. The scheduler service didn't exist yet — needed the full foundation (project structure, config, models, server, docker) before any business logic (pipeline, state manager, analyzer client) could be built.

**Decisions Made**:
- Matched data_collector patterns exactly: same Dockerfile structure, same pyproject.toml layout, same test organization.
- Used `main.py` (not `app.py`) per `fastapi_main_entrypoint` rule.
- Routes return empty stubs (TODO comments) — wiring to business logic deferred to steps 5-8.
- `JobState` is mutable (not frozen) since it accumulates state during pipeline execution.
- All response models are frozen (immutable DTOs).
- `ScheduleRequest` validates non-empty bucket and non-empty objects list.
- Docker-compose includes Postgres with healthcheck for the database layer (step 4).
- 24 tests: config defaults/overrides/validation, model construction/validation/freezing, route status codes/validation, swagger accessibility.

**Lessons**: None new — clean scaffolding following established patterns.

## 2026-03-01

**Summary**: Added steering rule enforcing single-task execution per session when following implementation plans.

**Change Type**: `docs`

**Context / Motivation**: User wants to prevent multi-step plan execution in a single session — each session should complete exactly one task, then stop.

**Decisions Made**:
- Rule stored at `.kiro/steering/rules/single_task_execution.md`.
- `alwaysApply: true` — applies to all plan-driven work.
- Glob targets `**/*.md` since plans are markdown files.

**Lessons**: None new.

## 2026-03-01

**Summary**: Added database layer for scheduler job state persistence — `database.py` with Postgres table, CRUD functions, and `JobRecord` model.

**Change Type**: `feat`

**Context / Motivation**: Step 4 of the scheduler implementation plan. The database layer is the foundation for all subsequent steps (state manager, scheduler service, resume flow). Without it, no job state can be persisted or recovered.

**Decisions Made**:
- Used raw psycopg2 with parameterized queries (not SQLAlchemy) — matches the spec's "direct Postgres connection" approach and keeps dependencies minimal. Will migrate to API Server later per spec.
- UPSERT via `ON CONFLICT (object_name, bucket)` — a file in a bucket is the natural unique key for job state.
- `completed_steps` stored as JSONB — native Postgres JSON support, no manual serialization on read.
- `JobRecord` is a frozen Pydantic model — read-only DTO from database rows.
- `get_connection()` accepts `database_url` parameter for testability (tests pass compose Postgres URL).
- Tests use docker-compose Postgres (not testcontainers) — Docker socket not available inside containers.

**Lessons**: Testcontainers don't work inside docker-compose containers. See lessons.md.

---

## 2026-03-02: Scheduler Pipeline Definition

**Summary**: Added `pipeline.py` with ordered step list, bucket resolution, and next-step logic.

**Change Type**: `feat`

**Context / Motivation**: Step 5 of the scheduler implementation plan. Foundation for steps 6-8 (state manager, analyzer client, scheduler service) which all depend on pipeline step definitions.

**Decisions Made**:
- Module-level constant `STEPS` list rather than an enum — simpler, and the step names are already strings everywhere (database, config, API models).
- `_STEP_TO_BUCKET_ATTR` maps step names to Settings attribute names — avoids a long if/elif chain and keeps the mapping declarative.
- `get_next_step()` uses a set for O(1) lookup on completed steps — handles out-of-order completion lists correctly.
- Both functions are pure (take explicit args, no global state) — easy to test and compose.

**Lessons**: None new — straightforward implementation.

---

## 2026-03-02: Scheduler Step 6 — Job State Manager

**Date**: 2026-03-02T20:31+01:00

**Summary**: Implemented `StateManager` class — in-memory job state tracking with Postgres persistence for the scheduler service.

**Change Type**: `feat`

**Context / Motivation**: Step 6 of the scheduler implementation plan. The state manager is the bridge between in-memory active job tracking and durable Postgres storage. Steps 7 (analyzer client) and 8 (scheduler service) depend on this to track job progress through the pipeline.

**Decisions Made**:
- `StateManager` takes a Postgres connection and bucket via constructor injection — keeps it testable and avoids global state.
- Reuses `JobState` model from `server/models.py` for in-memory state — no duplicate model, single source of truth.
- `update_step()` uses `STEPS.index()` to determine next step rather than calling `get_next_step()` — avoids converting to a set and back, and the step being completed is always known.
- `mark_completed()` is separate from `update_step()` completing the last step — allows explicit completion marking for clarity in the scheduler service.
- Every mutation method persists immediately — no batching, consistent with the spec's "after each step completes, persist state to Postgres."

**Lessons**: None new — straightforward implementation.

---

## 2026-03-02: Scheduler Step 7 — Analyzer Client

**Date**: 2026-03-02T20:34+01:00

**Summary**: Implemented `analyzer_client.py` — HTTP client for dispatching analytical jobs to the analyzer service.

**Change Type**: `feat`

**Context / Motivation**: Step 7 of the scheduler implementation plan. The analyzer client is the HTTP interface between the scheduler and the analyzer service. Step 8 (scheduler service core logic) depends on this to send jobs through the pipeline.

**Decisions Made**:
- `send_job()` is a standalone function (not a class) — single responsibility, no state to manage. The caller passes `analyzer_url` explicitly rather than reading from config internally.
- `AnalyzerRequest` / `AnalyzerResponse` are frozen Pydantic models — immutable DTOs matching the spec's analyzer request format.
- Uses `httpx.Client` as context manager with `base_url` and `verify=False` — matches the project's HTTP client rule.
- Returns `AnalyzerResponse(success=False, error=...)` on HTTP/network errors instead of raising — lets the scheduler service decide how to handle failures without try/except at the call site.
- 11 unit tests using `unittest.mock.patch` on `httpx.Client` — no real HTTP calls needed.

**Lessons**: None new — clean implementation following established patterns.

---

## 2026-03-02: Scheduler Step 8 — Core Orchestration Service

**Date**: 2026-03-02T20:36+01:00

**Summary**: Implemented `SchedulerService` — core orchestration logic tying together StateManager, analyzer client, and pipeline definitions. Wired routes to service via `app.state`.

**Change Type**: `feat`

**Context / Motivation**: Step 8 of the scheduler implementation plan. This is the central piece that makes the scheduler functional — without it, routes were stubs and no files could be processed through the pipeline.

**Decisions Made**:
- `SchedulerService` is a Facade pattern — coordinates StateManager, analyzer_client, and pipeline behind `schedule_batch()`, `process_file()`, and `resume_failed()`. Appropriate because routes need a single entry point for orchestration.
- `schedule_batch()` checks for in-progress jobs before creating new ones — prevents duplicate processing.
- `process_file()` walks steps sequentially using `get_next_step()` — stops on first failure, marks job as failed at that step.
- `resume_failed()` fast-forwards completed steps in memory before reprocessing — avoids re-running already-completed steps.
- Routes access service via `request.app.state.scheduler_service` — standard FastAPI pattern for shared state.
- `main.py` uses `contextmanager` lifespan to initialize DB schema and create service instance.
- Route tests mock `SchedulerService` at module level (`app.state.scheduler_service = MagicMock()`) — avoids needing a real DB for HTTP contract tests.

**Lessons**: When routes switch from stubs to `app.state.service`, existing TestClient tests break because lifespan doesn't run. Fix: set mock on `app.state` before creating TestClient. See lessons.md.

---

## 2026-03-02 — Data Collector → Scheduler Integration

**Summary**: Wired data collector to call `POST /scheduler/schedule` after successful MinIO uploads.

**Change Type**: `feat`

**Context / Motivation**: Step 12 of the scheduler implementation plan. The data collector needs to notify the scheduler to start the analytical pipeline after files are uploaded to MinIO.

**Decisions Made**:
- Created `scheduler_client.py` as a standalone module with `notify_scheduler()` — keeps HTTP client logic separate from route logic (SRP).
- Scheduler notification is fire-and-forget from the route's perspective — failure to reach the scheduler does not fail the `/collect` response. The data was already uploaded successfully; scheduler can be retried via `/resume`.
- Only calls scheduler when there are successes (`if successes:`) — avoids sending empty object lists.
- Route tests mock `notify_scheduler` at the route module level, consistent with existing mock patterns for `download_batch`, `upload_object`, etc.

**Lessons**: None new — straightforward integration following established patterns.

---

## 2026-03-02: Data Collector Service README

**Date**: 2026-03-02T20:45

**Summary**: Added `README.md` to the data collector service root directory.

**Change Type**: `docs`

**Context / Motivation**: The data collection implementation plan was fully complete (all 11 steps ✅), but the Service README Rule requires a README when a service plan is done. This was the most important remaining gap.

**Decisions Made**:
- Included: service purpose, endpoint table, configuration table with defaults, docker compose commands for run and test.
- Kept it concise — dev reference, not a design doc.

**Lessons**: None new.

---

## 2026-03-02: Scheduler Prefect Refactor — Steps 1 & 2

**Date**: 2026-03-02T21:13+01:00

**Summary**: Added `prefect>=3.0.0` dependency and replaced `SCHEDULER_THREAD_POOL_SIZE` config field with `PREFECT_API_URL`.

**Change Type**: `feat`

**Context / Motivation**: Steps 1-2 of the scheduler Prefect refactor implementation plan. Foundation for all subsequent Prefect integration — the dependency must be available and the config must expose the Prefect server URL before flows/tasks can be defined.

**Decisions Made**:
- Combined Steps 1 and 2 since Step 1 (add dependency) has no code/tests of its own — it's purely a `pyproject.toml` + `uv sync` change.
- Hardcoded `max_workers=4` in `scheduler.py._process_concurrently()` to keep existing tests passing — this code is entirely removed in Step 6 of the plan.
- Removed the `test_thread_pool_size_minimum` test since the field no longer exists.
- Default `PREFECT_API_URL` is `http://localhost:4200/api` matching the spec's configuration table.

**Lessons**: When removing a config field that's still referenced by code scheduled for later rewrite, hardcode the value temporarily to avoid breaking existing tests. The alternative (leaving the field) would create confusion about what's "current" config.

---

## 2026-03-02: Scheduler Prefect Refactor — Step 3 (execute_step task)

**Date**: 2026-03-02T21:16+01:00

**Summary**: Created `prefect_flows.py` with `@task execute_step` that delegates to `send_job()` from analyzer_client.

**Change Type**: `feat`

**Context / Motivation**: Step 3 of the scheduler Prefect refactor implementation plan. The `execute_step` task is the atomic building block for the Prefect flow — it wraps the existing `send_job()` call as a Prefect task so it gets tracked, logged, and visualized in the Prefect UI.

**Decisions Made**:
- Thin wrapper — `execute_step` does nothing beyond calling `send_job()` and logging. No retries, no error handling beyond what `send_job()` already provides. Per spec: "No automatic task-level retries."
- Tests call `.fn()` to invoke the underlying function without Prefect runtime overhead — standard pattern for unit testing Prefect tasks.
- 3 tests: success path, failure path, argument delegation verification.

**Lessons**: None new — straightforward implementation.

---

## 2026-03-02: Scheduler Prefect Refactor — Step 4 (process_file_flow)

**Date**: 2026-03-02T21:19+01:00

**Summary**: Added `@flow process_file_flow` to `prefect_flows.py` — walks a single file through all pipeline steps sequentially, persisting state to Postgres at each transition.

**Change Type**: `feat`

**Context / Motivation**: Step 4 of the scheduler Prefect refactor implementation plan. This is the core orchestration logic that replaces the current `SchedulerService.process_file()` method. All subsequent steps (5-9) depend on this flow being defined.

**Decisions Made**:
- Flow opens its own Postgres connection via `get_connection(database_url=db_url)` — each flow run is self-contained, no shared connection state across concurrent runs.
- `start_step` parameter enables resume: completed steps are derived from `STEPS[:index]` rather than queried from DB — the caller (scheduler service) already knows the failed step from Postgres.
- `list(completed_steps)` copies passed to `save_job_state` at every call — prevents mutable list aliasing where mock captures would see the final state instead of the state at call time.
- 6 unit tests covering: all steps succeed, failure at first/second step, resume from middle/last step, correct bucket resolution per step.
- Tests mock `get_connection`, `save_job_state`, and `send_job` — no DB or HTTP needed.

**Lessons**: When passing a mutable list to a mocked function, the mock captures a reference, not a snapshot. Always pass `list(...)` copies if you need to assert on intermediate states in test call_args.

## 2026-03-02: Simplify StateManager to thin Postgres wrapper
- **Change Type**: `refactor`
- **Summary**: Removed in-memory `_jobs` hashmap and all methods except `get_failed_jobs()` from `StateManager`. Prefect flows now handle state persistence directly via `database.py`.
- **Context / Motivation**: Step 5 of the Prefect refactor plan. The in-memory state tracking was needed for the old `ThreadPoolExecutor` approach but is now redundant — Prefect tracks active flow/task state natively, and `process_file_flow` writes directly to Postgres.
- **Decisions Made**: Also removed the `bucket` parameter from the constructor since it was only used by the removed `_persist()` method. Tests rewritten to use direct `save_job_state()` calls instead of going through the old StateManager API.
- **Lessons**: None new — straightforward removal.

## 2026-03-02: Scheduler Prefect Refactor — Step 6 (Rewrite SchedulerService)

**Date**: 2026-03-02T21:26+01:00

**Summary**: Rewrote `SchedulerService` to use Prefect flow runs instead of `ThreadPoolExecutor`. Removed `process_file()` and `_process_concurrently()`. Constructor now accepts `db_url` string instead of `conn` object.

**Change Type**: `refactor`

**Context / Motivation**: Step 6 of the scheduler Prefect refactor implementation plan. This is the core refactor — the `SchedulerService` was the last piece still using the old `ThreadPoolExecutor` + in-memory state approach. With `process_file_flow` already defined (Step 4) and `StateManager` simplified (Step 5), the service could be rewritten to delegate orchestration entirely to Prefect.

**Decisions Made**:
- `schedule_batch()` checks for in-progress jobs via Postgres query (new `get_in_progress_jobs()` on `StateManager` and `database.py`) instead of the removed in-memory hashmap.
- Each file triggers `process_file_flow()` synchronously — Prefect handles the flow run lifecycle.
- `resume_failed()` reads failed jobs from Postgres, then triggers `process_file_flow()` with `start_step` per failed job.
- Constructor takes `db_url: str` instead of `conn: PgConnection` — flows open their own connections, so the service doesn't need to hold one.
- Tests mock `process_file_flow` at the module level instead of `send_job` — tests verify delegation to the flow, not internal step execution.
- All tests depend on `conn` fixture for schema init + cleanup, even tests that don't seed data — prevents "table does not exist" errors from the service opening its own connections.

**Lessons**: When a service opens its own DB connections internally (not injected), test fixtures must ensure schema exists before the service runs, even if the test itself doesn't directly use the connection.

---

**Date**: 2026-03-02T21:31+01:00
**Summary**: Updated FastAPI lifespan to pass `db_url` string to `SchedulerService` and set `PREFECT_API_URL` env var for Prefect client discovery.
**Change Type**: `refactor`
**Context / Motivation**: Step 7 of the Prefect refactor plan — the lifespan still held a `get_connection()` context manager and passed `conn` to `SchedulerService`, but the service constructor was already updated (Step 6) to accept `db_url`. The lifespan needed to match.
**Decisions Made**:
- Set `PREFECT_API_URL` via `os.environ` rather than `prefect.settings.temporary_settings` — simpler, Prefect reads the env var natively, and the lifespan runs once at startup so there's no scoping concern.
- `init_schema()` uses a temporary `get_connection()` that closes immediately after table creation — the long-lived connection is no longer needed since flows manage their own.
- No test changes required — existing route tests use a mock service on `app.state`, and the lifespan doesn't run during `TestClient` usage.
**Lessons**: None new — straightforward wiring change.

---

**Date**: 2026-03-02T21:34+01:00
**Summary**: Added Prefect server to scheduler docker-compose and marked Step 9 as complete (tests already done in prior steps).
**Change Type**: `feat`
**Context / Motivation**: Step 8 of the Prefect refactor plan — the docker-compose needed a `prefect-server` service for the self-hosted Prefect UI/API, and the scheduler service needed `PREFECT_API_URL` instead of `SCHEDULER_THREAD_POOL_SIZE`. Step 9 (test updates) was already completed during Steps 2–6.
**Decisions Made**:
- Used `python -c "import urllib.request; ..."` for the Prefect server healthcheck because the `prefecthq/prefect:3-latest` image doesn't include `curl` or `wget`.
- `start_period` omitted — Prefect server starts fast enough with `interval=10s` and `retries=5` (50s window).
- Marked Step 9 as complete since all test files were already updated during their respective implementation steps.
**Lessons**: Prefect Docker image only has Python — no curl/wget. Use `python -c "import urllib.request; ..."` for healthchecks.

---

**Date**: 2026-03-02T21:38+01:00
**Summary**: Created scheduler service README.md after verifying all implementation plan steps complete and 82/82 tests passing.
**Change Type**: `docs`
**Context / Motivation**: Service README Rule requires a README when the implementation plan is fully complete. All 9 steps of the Prefect refactor plan were already checked off. Added Step 10 to the plan for the README.
**Decisions Made**:
- Kept README minimal: service purpose, endpoints table, config table, run/test commands.
- Verified all 82 tests pass before creating the README to confirm the plan is truly complete.
**Lessons**: None new.

## 2026-03-02T21:49 — fix: concurrent Prefect flow execution
- **Change Type**: `fix`
- **Summary**: `schedule_batch()` and `resume_failed()` now run Prefect flow runs concurrently via `ThreadPoolExecutor` instead of sequentially.
- **Context / Motivation**: The spec requires "Multiple flow runs execute concurrently" but the implementation called `process_file_flow()` in a sequential `for` loop, blocking on each before starting the next.
- **Decisions Made**: Used `ThreadPoolExecutor` with `max_workers=len(args)` to match the number of files. Extracted `_run_flows_concurrently()` to avoid duplicating the pattern in both methods.
- **Lessons**: Prefect `@flow` calls block when invoked directly — see `lessons.md`.

## 2026-03-02T22:51 — feat: API server foundation (project setup, config, database models, docker)
- **Change Type**: `feat`
- **Summary**: Created the `api_server` service with project structure, Settings config, SQLAlchemy ORM models (Files, JobExecutions, AnalyticalResults), Dockerfile, docker-compose, and 15 passing tests.
- **Context / Motivation**: First task from the API server implementation plan. The API server is the database interface for all other services. Foundation must exist before CRUD, routes, or metrics can be built.
- **Decisions Made**:
  - Used SQLAlchemy ORM (mapped_column style) instead of raw SQL, per the python-database steering rule.
  - Module-level engine/session_factory singletons with `reset_globals()` for test isolation.
  - Postgres port mapped to 5433 on host to avoid conflicts with scheduler's Postgres on 5432.
  - Kept FastAPI app minimal (no routes yet) — just lifespan with `init_schema()`.
- **Lessons**: None new.

## 2026-03-02T22:56 — feat: API server Pydantic request/response models
- **Change Type**: `feat`
- **Summary**: Created `src/server/models.py` with all Pydantic request/response models for the API server — Files, Job Executions, Analytical Results, and Metrics endpoints.
- **Context / Motivation**: Next uncompleted task in the API server implementation plan. All subsequent CRUD, routes, and metrics tasks depend on these models being defined.
- **Decisions Made**:
  - Response models are frozen (`ConfigDict(frozen=True)`) — immutable DTOs matching the scheduler's pattern.
  - Request models are mutable — they're input containers, not value objects.
  - `BatchExecutionItem` separated from `JobExecutionBatchCreate` for clean nesting.
  - `FileInfo` is a nested model for analytical result responses (per spec's `file_info` field).
  - Two separate checkpoint savings response models (`FileResponse` vs `AggregateResponse`) since the spec returns different shapes for single-file vs all-files queries.
  - `summary_data` typed as `dict` (not `dict[str, Any]`) — matches the JSONB column's flexible schema.
- **Lessons**: None new.

---

### 2026-03-02: Files CRUD operations
- **Date**: 2026-03-02T22:58+01:00
- **Summary**: Created `src/services/crud.py` with Files CRUD functions and 18 tests in `tests/test_crud.py`.
- **Change Type**: `feat`
- **Context / Motivation**: First uncompleted task in the API server implementation plan. CRUD layer is the foundation for all route handlers.
- **Decisions Made**:
  - `create_or_get_file` does a SELECT-then-INSERT rather than INSERT ON CONFLICT — simpler with SQLAlchemy ORM and matches the spec's idempotent behavior.
  - `list_files` returns `tuple[list[Files], int]` (results + total count) to support pagination metadata in the response.
  - `update_file` skips `None` values in the updates dict — allows partial updates from Pydantic models where unset fields are None.
  - Separate count query in `list_files` rather than using `len()` on results — correct total even with limit/offset.
- **Lessons**: None new.

## 2026-03-02T23:01 — Job Executions CRUD Operations
- **Change Type**: `feat`
- **Summary**: Added CRUD functions for job_executions table: create (single + batch), get by ID, list with filtering/pagination, partial update.
- **Context / Motivation**: Next step in API server implementation plan. Job executions CRUD is a prerequisite for routes, metrics, and analytical results.
- **Decisions Made**: Followed same patterns as Files CRUD (ValueError for FK violations, None-exclusion in updates, count+list for pagination). Batch create uses a single commit for atomicity.
- **Lessons**: None new — patterns established in Files CRUD carried over cleanly.

## 2026-03-02T23:04 — feat: Analytical Results CRUD operations
- **Change Type**: `feat`
- **Summary**: Added CRUD functions for analytical_results table: create, get by ID, list with complex JOIN filtering. Added `extract_metadata_from_object_name` helper for parsing taxi_type/year/month from S3 object keys.
- **Context / Motivation**: Next step in API server implementation plan — analytical results CRUD is a prerequisite for routes and metrics endpoints.
- **Decisions Made**: `list_analytical_results` returns `tuple[list[tuple[AnalyticalResults, Files]], int]` to give callers both the result and the associated file info in one query. Filtering by taxi_type/year/month uses SQL LIKE patterns against `files.object_name` rather than storing extracted metadata separately — simpler, no schema changes needed.
- **Lessons**: None new.

## 2026-03-02T23:07 — feat: Metrics calculations for thesis checkpoint evaluation
- **Change Type**: `feat`
- **Summary**: Created `src/services/metrics.py` with three metrics functions: `calculate_checkpoint_savings`, `calculate_failure_statistics`, `calculate_pipeline_summary`. 12 tests in `tests/test_metrics.py`.
- **Context / Motivation**: Next step in API server implementation plan. Metrics are the thesis-critical calculations — checkpoint savings, failure rates, and pipeline summary. Required before metrics API routes can be built.
- **Decisions Made**:
  - `_time_saved_subquery` extracted as a helper — reused by both per-file and aggregate savings calculations.
  - Aggregate savings iterates over files with retries in Python rather than a single complex SQL query — simpler, and the number of files with retries is small (tens, not thousands).
  - `calculate_failure_statistics` uses `COUNT(DISTINCT CASE(...))` for counting files that failed per step — matches the thesis_metrics.md SQL pattern.
  - `calculate_pipeline_summary` reuses `calculate_checkpoint_savings(session)` for the savings portion — DRY, single source of truth for savings logic.
  - All functions return plain dicts matching the Pydantic response model shapes — routes will construct the models from these dicts.
- **Lessons**: None new.

## 2026-03-02T23:10 — feat: API Routes - Files (POST, GET, GET list, PATCH)
- **Change Type**: `feat`
- **Summary**: Created `src/server/routes.py` with Files API routes and `get_db` dependency. Wired router into `main.py`. 21 tests in `tests/test_routes.py`.
- **Context / Motivation**: First uncompleted task in the API server implementation plan. Routes are the HTTP layer that exposes CRUD operations to other services.
- **Decisions Made**:
  - `get_db` is a generator-based FastAPI dependency using `get_session_factory()` — standard pattern for session lifecycle management.
  - `_file_to_response` helper converts ORM objects to Pydantic models — keeps route handlers thin.
  - `status` query param aliased to `status_filter` to avoid shadowing the `status` import from FastAPI.
  - Tests use real docker-compose Postgres (not mocks) — validates the full stack from HTTP to DB.
  - Per-test table cleanup via `_clean_tables` autouse fixture.
- **Lessons**: None new.

## 2026-03-02T23:13 — API Routes - Job Executions
- **Change Type**: `feat`
- **Summary**: Added 5 Job Execution endpoints to `routes.py` with `_job_execution_to_response` helper. 20 new tests (41 total in `test_routes.py`).
- **Context / Motivation**: Next uncompleted task in the API server implementation plan. Job execution routes expose CRUD for step-level pipeline tracking.
- **Decisions Made**:
  - Followed exact same patterns as Files routes: helper converter, `Depends(get_db)`, `HTTPException` for 404s.
  - `ValueError` from CRUD layer mapped to 404 — consistent with Files pattern.
  - Batch endpoint passes `model_dump()` of each execution item to CRUD layer — keeps CRUD interface dict-based.
  - `status` query param aliased to `status_filter` — same pattern as Files list endpoint.
- **Lessons**: None new.

## 2026-03-02T23:16 — API Routes - Analytical Results
- **Change Type**: `feat`
- **Summary**: Added 3 Analytical Results endpoints to `routes.py` with `_analytical_result_to_response` helper. 15 new tests (56 total in `test_routes.py`).
- **Context / Motivation**: Next uncompleted task in the API server implementation plan. Analytical results routes expose create and query endpoints with complex filtering (JOIN through job_executions to files).
- **Decisions Made**:
  - `_analytical_result_to_response` takes optional `Files` param for nested `file_info` — GET by ID returns no file_info, GET list returns it (matches spec response shapes).
  - Complex filtering (taxi_type, year, month) delegated entirely to existing `list_analytical_results` CRUD function.
  - `datetime` query params (`created_at_from`, `created_at_to`) use FastAPI's built-in datetime parsing.
- **Lessons**: None new.

---

## 2026-03-02: API Routes - Metrics

- **Date**: 2026-03-02T23:19+01:00
- **Summary**: Added 3 metrics API route handlers and 8 route-level tests.
- **Change Type**: `feat`
- **Context / Motivation**: Metrics endpoints are the last API routes needed before integration/e2e tests and documentation. The underlying metrics calculations were already implemented and tested.
- **Decisions Made**:
  - `GET /metrics/checkpoint-savings` returns `CheckpointSavingsFileResponse` when `file_id` is provided, `CheckpointSavingsAggregateResponse` otherwise. Uses union return type.
  - Per-file endpoint returns 404 when file not found (empty dict from `calculate_checkpoint_savings`).
  - Route handlers are thin wrappers — all logic stays in `src/services/metrics.py`.
- **Lessons**: None new.

## 2026-03-02T23:22 — API Server End-to-End Integration Tests
- **Change Type**: `test`
- **Summary**: Created `tests/test_integration.py` with 6 end-to-end tests covering the full pipeline workflow, failure/retry with checkpoint savings verification, and complex filtering across the data model.
- **Context / Motivation**: The implementation plan had all CRUD, metrics, and route tests done but lacked a full workflow integration test validating the complete lifecycle (file → jobs → updates → results → metrics).
- **Decisions Made**: Used the same fixture pattern as existing test files (module-scoped schema init, per-test table cleanup). Verified checkpoint savings math explicitly (75s saved out of 300s = 25%). Also marked the previously-completed but unchecked test plan items as done.
- **Lessons**: None new — existing patterns worked cleanly.

## 2026-03-02T23:25 — API Server README
- **Change Type**: `docs`
- **Summary**: Created `src/api_server/README.md` with endpoints, configuration, and docker run/test commands.
- **Context / Motivation**: Final task in the API Server implementation plan — documentation.
- **Decisions Made**: Followed scheduler README format for consistency. Grouped endpoints by resource (Files, Job Executions, Analytical Results, Metrics).
- **Lessons**: None new.

---

## 2026-03-04

- **Change Type**: `docs`
- **Summary**: Created `specs/analyzer.md` (full specification) and `specs/analyzer_implementation_plan.md` (15-step plan). Updated `specs/README.md` features section with Analyzer and Scheduler endpoint routing refactor entries.
- **Context / Motivation**: User requested Analyzer service implementation. Conducted design interview to nail down: strategy pattern (abstract base per step × 4 taxi types), Polars over pandas, per-step endpoints, per-step output buckets, explicit taxi_type in request payload, job_execution_id passed by scheduler.
- **Decisions Made**: (1) One abstract base class per step with 4 concrete implementations = 20 classes, following SRP. (2) Polars for data manipulation. (3) Analyzer talks to MinIO directly for parquet I/O, API Server only for summary JSONB. (4) FHV/FHVHV skip impossible analyses gracefully. (5) Separated scheduler routing refactor as its own future feature.
- **Lessons**: Subagent file edits can be overwritten by subsequent edits to the same file — verify subagent output before making overlapping changes.

## 2026-03-04T21:52 — Analyzer Project Setup
- **Date**: 2026-03-04T21:52+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/analyzer/` directory structure, `pyproject.toml`, `Dockerfile`, `docker-compose.yml`, minimal `main.py` and `config.py`. All `__init__.py` files in place. Docker build verified.
- **Context / Motivation**: Step 1 of the analyzer implementation plan — foundational project scaffolding before any business logic.
- **Decisions Made**: (1) Followed existing service patterns (data_collector, api_server) for Dockerfile and docker-compose. (2) Port 8002 for analyzer (8000=data_collector, 8001=scheduler). (3) Included config.py early since `src/main.py` entrypoint references it. (4) Minimal `src/server/main.py` with just `FastAPI()` — routes added in later steps.
- **Lessons**: None new.

## 2026-03-04T21:56 — Analyzer Configuration Tests
- **Date**: 2026-03-04T21:56+01:00
- **Change Type**: `test`
- **Summary**: Created `tests/test_config.py` for analyzer Settings — defaults, env override, port validation. All 3 tests pass.
- **Context / Motivation**: Step 2 of the analyzer implementation plan. Config.py was already created in Step 1; this step adds test coverage.
- **Decisions Made**: (1) Used `monkeypatch.delenv` pattern from data_collector to clear docker-compose injected env vars. (2) Extracted `_clear_env` helper with full list of env var names for reuse. (3) Tested port boundary validation (0 and 70000).
- **Lessons**: None new.

## 2026-03-04T21:58 — Analyzer Request/Response Models
- **Date**: 2026-03-04T21:58+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/analyzer/src/server/models.py` with `TaxiType`, `AnalyzeRequest`, `AnalyzeResponse`, `StepResult`. 18 tests pass.
- **Context / Motivation**: Step 3 of the analyzer implementation plan. These models are the foundation for all subsequent steps (API client, ABCs, registry, routes).
- **Decisions Made**: (1) Defined `TaxiType` as a local `StrEnum` (4 members, no `ALL`) rather than importing from data_collector — analyzer only processes individual types. (2) `AnalyzeRequest`/`AnalyzeResponse` are frozen Pydantic models matching the spec's JSON payloads. (3) `AnalyzeResponse` is compatible with scheduler's `AnalyzerResponse` (same `success`/`error` fields). (4) `StepResult` is mutable since it's built up during computation. (5) Added `min_length=1` on bucket/object and `ge=1` on job_execution_id for boundary validation.
- **Lessons**: None new.

## 2026-03-04T22:00 — Analyzer API Server Client
- **Date**: 2026-03-04T22:00+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/analyzer/src/services/api_server_client.py` with `post_analytical_result()`. 6 tests pass.
- **Context / Motivation**: Step 4 of the analyzer implementation plan. The analyzer needs to POST summary results to the API Server's `POST /analytical-results` endpoint after each analytical step completes.
- **Decisions Made**: (1) Single function `post_analytical_result()` returning `bool` — matches the simplicity of the scheduler's `send_job()` pattern. (2) Used `unittest.mock.patch` for httpx mocking to match the existing project convention (scheduler tests), rather than introducing `pytest-httpx` as a new dependency. (3) Returns `False` on any HTTP or network error instead of raising — the caller (step_executor) decides how to handle failures. (4) Timeout set to 25.0s matching the HTTP client rule defaults.
- **Lessons**: None new.

## 2026-03-04T22:03 — Analyzer Abstract Base Classes
- **Date**: 2026-03-04T22:03+01:00
- **Change Type**: `feat`
- **Summary**: Created 5 ABCs in `src/analyzer/src/services/base/` — one per analytical step. 10 tests pass.
- **Context / Motivation**: Step 5 of the analyzer implementation plan. The strategy pattern requires abstract interfaces that concrete taxi-type implementations will extend.
- **Decisions Made**: (1) Each ABC defines a single `analyze(df: polars.DataFrame) -> StepResult` method — minimal interface, same signature across all steps. (2) Used `abc.ABC` + `@abstractmethod` rather than `Protocol` since these are meant to be subclassed, not structurally matched. (3) No shared helper methods in the ABCs — concrete implementations will add their own as needed.
- **Lessons**: None new.

## 2026-03-04T22:05 — Analyzer Registry (Step 6)
- **Change Type**: `feat`
- **Summary**: Added `StepName` enum, 20 concrete analyzer stubs, registry factory, and 23 tests.
- **Context / Motivation**: Step 6 of the analyzer implementation plan — the registry is the central dispatch mechanism mapping `(step_name, taxi_type)` to concrete analyzer classes.
- **Decisions Made**: Created minimal concrete stubs (raise `NotImplementedError`) so the registry is fully testable now; real implementations come in Steps 7-11. Used a flat dict mapping `(StepName, TaxiType)` tuples to class types — simple, explicit, no magic.
- **Lessons**: None new.

## 2026-03-04T22:09 — Descriptive Statistics Implementation (Step 7)
- **Change Type**: `feat`
- **Summary**: Implemented descriptive statistics `analyze()` for all 4 taxi types (Yellow, Green, FHV, FHVHV) with 21 tests.
- **Context / Motivation**: Step 7 of the analyzer implementation plan — first concrete analytical step. Computes percentiles, histograms, correlation matrix, skewness/kurtosis on numeric columns.
- **Decisions Made**: (1) Used template method pattern — `analyze()` is concrete in the base class, delegates column selection to abstract `_numeric_columns()`. This avoids duplicating ~80 lines of numpy/scipy logic across 4 files. (2) Each concrete class is just a column list filter. (3) Updated `test_base_classes.py` to accept non-`analyze` abstract methods since the ABC contract changed. (4) FHV gets 3 numeric columns (sr_flag + location IDs), FHVHV gets 12, Yellow 14, Green 15.
- **Lessons**: When refactoring an ABC from abstract `analyze()` to template method with a different abstract method, existing tests that assert `"analyze" in __abstractmethods__` will break — update them.

## 2026-03-05T10:24 — Analyzer Feature Spec

- **Change Type**: `docs`
- **Summary**: Created `specs/analyzer.md` — standalone feature specification for the analyzer service. Updated `specs/README.md` features section with link.
- **Context / Motivation**: The analyzer had an implementation plan but no feature spec. All other services (data_collection, scheduler, api_server) had one. Needed for consistency and as a reference document.
- **Decisions Made**: Followed the format of existing specs. Included taxi-type variation matrix, full data flow, scheduler contract gap, all configuration, and internal model documentation. Derived entirely from README.md, the implementation plan, and existing source code — no speculative content.
- **Lessons**: None new.

---

## 2026-03-05

- **Change Type**: `docs`
- **Summary**: Broke down sections 8–15 of `specs/analyzer_implementation_plan.md` into atomic subtasks per `atomic_plan_tasks.md` rule.
- **Context / Motivation**: Sections 8–15 had bundled tasks (e.g., all 4 taxi types + tests in one bullet). Needed granular tasks for single-task-per-session execution and context management.
- **Decisions Made**: Each concrete implementation per taxi type is its own subtask. Each test file is its own subtask. Verification steps are explicit. Removed the `Create src/analyzer/src/main.py` task from Section 13 since it already exists. Noted that `src/server/main.py` already exists and only needs a router wiring update.
- **Lessons**: None new.

---

### 2026-03-05T10:43 — Yellow Data Cleaning Implementation
- **Change Type**: `feat`
- **Summary**: Implemented `YellowDataCleaning.analyze()` with IQR, Z-score, Isolation Forest outlier detection, quality rules, and removal vs capping strategy comparison.
- **Context / Motivation**: Task 8.1 in the analyzer implementation plan — first concrete data cleaning implementation.
- **Decisions Made**: Used `polars.to_arrow()` instead of `to_pandas()` for parquet serialization (pandas not available in the Docker image). Outlier detection runs on 5 fare/distance columns. Quality rules check negative fares, zero distances, impossible durations, and invalid passenger counts. Detail output uses the removal strategy (IQR-based) as the cleaned dataframe.
- **Lessons**: `pyarrow.Table.from_pandas()` requires pandas; use `polars_df.to_arrow()` directly when pandas is not a dependency.

## 2026-03-05T10:47 — Green Data Cleaning + Shared Cleaning Utilities
- **Change Type**: `feat`
- **Summary**: Implemented `GreenDataCleaning` with Green-specific columns. Extracted shared outlier detection/cleaning logic to `base/cleaning_utils.py` and refactored `YellowDataCleaning` to use it.
- **Context / Motivation**: Task 8.2 in the analyzer implementation plan. Green uses identical outlier detection logic as Yellow — extracting shared utilities avoids 4x code duplication across taxi types.
- **Decisions Made**: Created `cleaning_utils.py` with `run_outlier_detection`, `apply_removal_strategy`, `apply_capping_strategy`, `build_step_result` as shared functions. Green adds `negative_ehail_fee` quality rule and uses `lpep_*` datetime columns for duration checks.
- **Lessons**: When multiple taxi types share identical algorithmic logic, extract to a shared module at the point of second use rather than duplicating.

## 2026-03-05T10:50 — FHV Data Cleaning Implementation
- **Date**: 2026-03-05
- **Change Type**: `feat`
- **Summary**: Implemented `FhvDataCleaning` with duration-only quality rules. No fare/distance outlier detection since FHV has no fare columns.
- **Context / Motivation**: Task 8.3 in the analyzer implementation plan. FHV schema only has `dispatching_base_num`, `pickup_datetime`, `dropoff_datetime`, `pulocationid`, `dolocationid`, `sr_flag`, `affiliated_base_number` — no numeric fare/distance columns for outlier detection.
- **Decisions Made**: Empty `_OUTLIER_COLUMNS` list — reuses shared `cleaning_utils` functions which gracefully handle empty column lists. Only quality rule is `impossible_durations` (dropoff <= pickup). Same structural pattern as Yellow/Green for consistency.
- **Lessons**: The shared `cleaning_utils` functions handle empty column lists cleanly, so the FHV implementation is minimal — just quality rules and the shared pipeline.

## 2026-03-05T10:53+01:00
- **Change Type**: `feat`
- **Summary**: Implemented `FhvhvDataCleaning` with outlier detection on `trip_miles`, `trip_time`, `base_passenger_fare`, `tips`, `driver_pay` and quality rules for negative fares, zero distances, impossible durations, negative trip time.
- **Context / Motivation**: Task 8.4 in the analyzer implementation plan. FHVHV has partial fare data — no `fare_amount`/`total_amount` but has `base_passenger_fare`, `tips`, `driver_pay` plus `trip_miles` and `trip_time`.
- **Decisions Made**: Used same structural pattern as Yellow/Green/FHV. Five outlier columns (all available numeric columns). Added `negative_trip_time` quality rule specific to FHVHV since `trip_time` is an explicit integer column (unlike other taxi types that derive duration from datetime diff).
- **Lessons**: None new — pattern is well-established from prior implementations.

## 2026-03-05: Data cleaning tests for all taxi types
- **Change Type**: `test`
- **Summary**: Created `tests/test_data_cleaning.py` with 23 tests covering Yellow, Green, FHV, and FHVHV data cleaning implementations.
- **Context / Motivation**: Steps 8.1–8.4 implemented all 4 data cleaning classes but had no tests. This was the most important next task to validate correctness before moving to temporal analysis.
- **Decisions Made**: Followed the same test structure as `test_descriptive_statistics.py` (per-type test classes + edge cases class). Used small deterministic DataFrames with known violations for quality rule tests. Kept helper functions minimal — only the columns needed by each implementation.
- **Lessons**: None new — existing patterns worked well.

## 2026-03-05: Yellow Temporal Analysis Implementation
- **Date**: 2026-03-05T10:58+01:00
- **Change Type**: `feat`
- **Summary**: Implemented `YellowTemporalAnalysis` in `src/analyzer/src/services/yellow/temporal_analysis.py` — time-series decomposition, FFT, rolling stats, peak hour detection.
- **Context / Motivation**: Task 9.1 in the analyzer implementation plan. First temporal analysis concrete implementation, establishing the pattern for Green/FHV/FHVHV.
- **Decisions Made**: Used manual additive decomposition (moving average trend + seasonal averaging) instead of statsmodels — avoids adding a dependency. FFT via numpy.fft.rfft. Rolling stats via Polars rolling_mean/rolling_std. Peak hours defined as hour-of-day with above-average trip counts. Detail parquet stores JSON-serialized decomposition/rolling/frequencies (same pattern as descriptive statistics).
- **Lessons**: None new — followed existing patterns from descriptive statistics and data cleaning implementations.

## 2026-03-05: Green Temporal Analysis (Step 9.2)
- **Change Type**: `feat`
- **Summary**: Implemented `GreenTemporalAnalysis` in `src/analyzer/src/services/green/temporal_analysis.py` — same logic as Yellow using `lpep_pickup_datetime`.
- **Context / Motivation**: Task 9.2 in the analyzer implementation plan. Green taxi uses identical temporal analysis logic, only the pickup datetime column name differs.
- **Decisions Made**: Duplicated helper functions from Yellow rather than extracting shared module — matches existing pattern (each taxi type is self-contained). Surgical change: only replaced the stub, no refactoring of Yellow.
- **Lessons**: None new — straightforward port of Yellow pattern.

## 2026-03-05T11:04 — FHV Temporal Analysis
- **Change Type**: `feat`
- **Summary**: Implemented `FhvTemporalAnalysis` in `src/analyzer/src/services/fhv/temporal_analysis.py` — uses `pickup_datetime`, no fare aggregations.
- **Context / Motivation**: Task 9.3 in the analyzer implementation plan. FHV schema has no fare columns, so `_build_hourly_series` only aggregates trip counts.
- **Decisions Made**: Same helper function pattern as Yellow/Green. Removed `avg_fare` from hourly series since FHV has no fare data. No shared module extraction — matches existing per-taxi-type self-contained pattern.
- **Lessons**: None new — straightforward adaptation of existing pattern.

## 2026-03-05
- **Change Type**: `feat`
- **Summary**: Implemented `FhvhvTemporalAnalysis` in `src/analyzer/src/services/fhvhv/temporal_analysis.py` — uses `pickup_datetime` + `base_passenger_fare`, adds wait time analysis (request → on_scene → pickup).
- **Context / Motivation**: Task 9.4 in the analyzer implementation plan. FHVHV has unique datetime columns (`request_datetime`, `on_scene_datetime`) enabling wait time analysis not available in other taxi types.
- **Decisions Made**: Followed Yellow pattern (with fare aggregation) plus `_compute_wait_times()` for FHVHV-specific wait time metrics. Wait times stored in both summary and detail parquet. Negative durations filtered out as invalid.
- **Lessons**: None new — straightforward adaptation with one additional feature.

## 2026-03-05: Temporal Analysis Tests (Task 9.5)
- **Date**: 2026-03-05T11:09+01:00
- **Change Type**: `test`
- **Summary**: Created `tests/test_temporal_analysis.py` with 26 tests covering all four taxi types (yellow, green, fhv, fhvhv) plus edge cases.
- **Context / Motivation**: Task 9.5 in the analyzer implementation plan. Tests validate decomposition, Fourier output, rolling stats, peak hours, FHV no-fare behavior, FHVHV wait time analysis, parquet output, and edge cases (empty, single row, single hour, missing column).
- **Decisions Made**: Followed `test_data_cleaning.py` pattern — per-type test classes + shared edge case class. Used 500-row sample DataFrames spanning multiple days/hours to exercise decomposition and rolling windows meaningfully.
- **Lessons**: None new.

## 2026-03-05T11:11 — Yellow Geospatial Analysis
- **Change Type**: `feat`
- **Summary**: Implemented `YellowGeospatialAnalysis` concrete class replacing the `NotImplementedError` stub.
- **Context / Motivation**: Step 10.1 of the analyzer implementation plan — first geospatial analysis implementation, unblocking the remaining taxi types and tests.
- **Decisions Made**: Used module-level helper functions (`_zone_trip_counts`, `_route_counts`, `_dbscan_clusters`, `_kmeans_clusters`, `_distance_by_zone`) to keep the `analyze()` method concise. DBSCAN eps=5.0/min_samples=10 chosen as reasonable defaults for zone ID space (1–265). K-means n_clusters=5 with fallback when fewer zones exist.
- **Lessons**: None new — followed established patterns from temporal analysis implementations.

## 2026-03-05T11:14 — Green Geospatial Analysis Implementation
- **Change Type**: `feat`
- **Summary**: Implemented `GreenGeospatialAnalysis` in `src/analyzer/src/services/green/geospatial_analysis.py`, replacing the `NotImplementedError` stub.
- **Context / Motivation**: Plan task 10.2 — Green taxi data has identical zone ID columns and `trip_distance` as Yellow, so the geospatial logic is the same.
- **Decisions Made**: Duplicated Yellow helper functions rather than extracting shared module — matches existing pattern across other Green implementations (temporal, data_cleaning) and avoids refactoring Yellow (surgical changes principle).
- **Lessons**: None new — straightforward duplication of existing pattern.

## 2026-03-05T11:17 — FHV Geospatial Analysis
- **Change Type**: `feat`
- **Summary**: Implemented `FhvGeospatialAnalysis` in `src/analyzer/src/services/fhv/geospatial_analysis.py`, replacing the `NotImplementedError` stub.
- **Context / Motivation**: Plan task 10.3 — FHV data has `pulocationid`/`dolocationid` for zone clustering and route detection, but lacks `trip_distance` so distance distribution is skipped.
- **Decisions Made**: Reused same helper functions as Yellow/Green (DBSCAN, K-means, route counts, zone heatmap). Omitted `_distance_by_zone` and `distance_by_zone` detail field entirely. Added `distance_distribution_skipped: True` flag to summary for downstream consumers.
- **Lessons**: None new — straightforward adaptation of existing pattern with column omission.

## 2026-03-05T11:19 — FHVHV Geospatial Analysis
- **Change Type**: `feat`
- **Summary**: Implemented `FhvhvGeospatialAnalysis` in `src/analyzer/src/services/fhvhv/geospatial_analysis.py`.
- **Context / Motivation**: Plan task 10.4 — FHVHV data has `pulocationid`/`dolocationid` for zone clustering and `trip_miles` for distance distribution (unlike Yellow/Green which use `trip_distance`, and FHV which has no distance column).
- **Decisions Made**: Followed Yellow/Green pattern exactly, only changing `_DIST_COL` from `trip_distance` to `trip_miles`. Includes distance distribution (unlike FHV which skips it).
- **Lessons**: None new — straightforward column name adaptation of existing pattern.

## 2026-03-05T11:21 — Geospatial Analysis Tests
- **Change Type**: `test`
- **Summary**: Created `tests/test_geospatial_analysis.py` with 27 tests covering all 4 taxi types + edge cases.
- **Context / Motivation**: Plan task 10.5 — implementations 10.1–10.4 were done but untested. Tests needed before proceeding to fare revenue analysis.
- **Decisions Made**: Followed `test_temporal_analysis.py` pattern. Verified DBSCAN/K-means clusters, top routes, zone heatmaps, distance distributions, FHV skip behavior, FHVHV `trip_miles` usage, and edge cases (empty, single row, single zone, missing columns).
- **Lessons**: None new — straightforward test creation following established patterns.

## 2026-03-05: Implement YellowFareRevenueAnalysis
- **Change Type**: `feat`
- **Summary**: Implemented `YellowFareRevenueAnalysis.analyze()` with revenue forecasting, fare anomaly detection, tip prediction, fare distribution, and surcharge breakdown.
- **Context / Motivation**: Step 11.1 of the analyzer implementation plan — first concrete fare revenue analysis implementation needed before Green/FHV/FHVHV variants and tests.
- **Decisions Made**: Used same patterns as geospatial/temporal (module-level helper functions, json-serialized detail parquet, guard clause for empty/missing data). Linear regression for forecasting and tip prediction (simplest viable model per spec). Z-score threshold of 3 for anomaly detection (standard). Distance bucketed into 7 ranges for fare distribution.
- **Lessons**: None new — existing patterns worked cleanly.

## 2026-03-05: Implement GreenFareRevenueAnalysis
- **Change Type**: `feat`
- **Summary**: Implemented `GreenFareRevenueAnalysis` concrete class with Green-specific columns (`lpep_*` datetimes, `ehail_fee` surcharge, no `airport_fee`).
- **Context / Motivation**: Step 11.2 of the analyzer implementation plan — completing fare revenue analysis for all taxi types.
- **Decisions Made**: Reused identical analytical logic from Yellow (revenue forecast, anomaly detection, tip prediction, fare distribution, surcharge breakdown) with only column constants changed. Green has same fare structure as Yellow minus `airport_fee` plus `ehail_fee`.
- **Lessons**: None new — straightforward adaptation of existing pattern.

## 2026-03-05T11:30 — FHV Fare Revenue Analysis (Skip)
- **Change Type**: `feat`
- **Summary**: Implemented `FhvFareRevenueAnalysis` — returns skip result since FHV has no fare columns.
- **Context / Motivation**: Step 11.3 of the analyzer implementation plan. FHV data lacks fare/tip/surcharge columns entirely.
- **Decisions Made**: Returns `StepResult` with `skipped: True` summary, empty `detail_bytes`, and standard s3 key. Includes `num_rows` in summary for audit trail. No computation needed.
- **Lessons**: None new — straightforward skip pattern.

---

### 2026-03-05T11:32+01:00
- **Change Type**: `feat`
- **Summary**: Implemented `FhvhvFareRevenueAnalysis` — full fare revenue analysis using FHVHV-specific columns.
- **Context / Motivation**: Step 11.4 of the analyzer implementation plan. FHVHV has partial fare data (`base_passenger_fare`, `tips`, `driver_pay`) but no `fare_amount`/`total_amount`.
- **Decisions Made**: Used `base_passenger_fare` as primary fare column, `driver_pay` alongside it for anomaly detection. `trip_miles` for distance buckets instead of `trip_distance`. Surcharges: `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`. Followed exact same helper function pattern as Yellow/Green for consistency.
- **Lessons**: None new — straightforward column remapping of established pattern.

## 2026-03-05: Fare Revenue Analysis Tests (Step 11.5)
- **Date**: 2026-03-05T11:35+01:00
- **Change Type**: `test`
- **Summary**: Created `tests/test_fare_revenue_analysis.py` — 30 tests covering all 4 taxi types.
- **Context / Motivation**: Step 11.5 of the analyzer implementation plan. All 4 fare revenue implementations (11.1–11.4) were complete but untested. Tests needed before building step executor and routes on top.
- **Decisions Made**: Followed existing test pattern from `test_geospatial_analysis.py`. Tested summary structure, detail parquet validity, taxi-type-specific behavior (FHV skip, FHVHV `base_passenger_fare`/`driver_pay`), and edge cases (empty df, single row, all-zero fares, missing fare column).
- **Lessons**: None new — straightforward test creation following established patterns.

## 2026-03-05: Step executor orchestration logic
- **Date**: 2026-03-05T11:38+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/analyzer/src/services/step_executor.py` and `tests/test_step_executor.py` — shared orchestration function wiring download → analyze → upload → post → respond.
- **Context / Motivation**: Steps 12.1 and 12.2 of the analyzer implementation plan. All 20 concrete analyzers and the registry were complete. The step executor is the glue that connects them to the route handlers, making it the critical next piece.
- **Decisions Made**: Used a `_STEP_TO_BUCKET_ATTR` dict mapping `StepName` → Settings attribute name via `getattr()` to resolve output buckets. API post failure is logged as warning but does NOT fail the step (per spec). Detail S3 key is namespaced as `{taxi_type}/{job_execution_id}/{detail_s3_key}`. All S3 and API interactions are mockable at the module boundary.
- **Lessons**: None new — straightforward orchestration following the spec's data flow.

## 2026-03-05T11:41 — Analyzer Routes Module

- **Date**: 2026-03-05T11:41+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/analyzer/src/server/routes.py` with 5 POST endpoints under `/analyze`, each delegating to `execute_step` with the appropriate `StepName`.
- **Context / Motivation**: Step 13.1 of the analyzer implementation plan. The step executor (12.1) was complete — routes are the HTTP entry points that expose it.
- **Decisions Made**: Each endpoint is a thin wrapper calling `execute_step` with a hardcoded `StepName`. Used module-level `SETTINGS` import (matches existing pattern in `config.py`). No async — matches project convention of sync handlers.
- **Lessons**: None new — straightforward wiring.

## 2026-03-05T11:43 — Wire analyzer routes router into FastAPI app
- **Change Type**: `feat`
- **Summary**: Added `app.include_router(router)` to `src/analyzer/src/server/main.py`, connecting the 5 analyzer endpoints to the FastAPI app.
- **Context / Motivation**: Step 13.2 of the analyzer implementation plan. Routes module (13.1) existed but wasn't wired into the app — endpoints were unreachable.
- **Decisions Made**: Followed the same pattern as `src/api_server/src/server/main.py`. No lifespan needed for the analyzer (no DB init on startup).
- **Lessons**: None new — single-line wiring.

## 2026-03-05T11:45 — Analyzer Route Handler Tests

- **Change Type**: `test`
- **Summary**: Created `tests/test_routes.py` with 20 tests covering all 5 analyzer endpoints — delegation, response shape, and request validation.
- **Context / Motivation**: Step 13.3 of the analyzer implementation plan. Routes (13.1) and wiring (13.2) were done but had no test coverage.
- **Decisions Made**: Mocked `execute_step` at the `routes` module level (not deeper). Used `pytest.mark.parametrize` over the 5 endpoints to avoid repetition. Validation tests cover missing fields, invalid taxi_type, zero job_execution_id, and empty input_bucket.
- **Lessons**: None new — straightforward TestClient + mock pattern.

## 2026-03-05: Add Swagger UI accessibility tests for analyzer service
- **Change Type**: `test`
- **Summary**: Created `tests/test_swagger.py` with 3 tests verifying `/docs`, swagger-ui HTML content, and `/openapi.json` schema correctness.
- **Context / Motivation**: Step 13.4 of the analyzer implementation plan. Ensures the Swagger UI is accessible and the OpenAPI schema matches the expected title ("Analyzer") and version ("0.1.0").
- **Decisions Made**: Followed the `data_collector/tests/test_swagger.py` pattern exactly, adapting only the import path (`src.server.main` per fastapi_main_entrypoint rule) and expected title/version.
- **Lessons**: None new.

## 2026-03-05: Integration test — Descriptive Statistics with real MinIO
- **Date**: 2026-03-05T11:48+01:00
- **Change Type**: `test`
- **Summary**: Created `tests/test_integration.py` with end-to-end test for descriptive statistics: upload sample Yellow taxi parquet to MinIO, call endpoint, verify output parquet in output bucket.
- **Context / Motivation**: Steps 13.5 + 14.1 of the analyzer implementation plan. Validates the full orchestration flow (download → analyze → upload) with real MinIO, mocking only the API server POST.
- **Decisions Made**: Followed `data_collector/tests/test_integration.py` pattern. Used `polars.to_arrow()` for parquet creation (lesson from 2026-03-05). Mocked `post_analytical_result` at the `step_executor` module level to avoid network calls to a non-existent API server.
- **Lessons**: None new.

## 2026-03-05T11:52
- **Date**: 2026-03-05T11:52+01:00
- **Change Type**: `test`
- **Summary**: Added parametrized integration tests for green, fhv, and fhvhv taxi types against the descriptive-statistics endpoint with real MinIO.
- **Context / Motivation**: Step 14.2 of the analyzer implementation plan. The existing integration test only covered yellow; this validates all four taxi types end-to-end.
- **Decisions Made**: Used `pytest.mark.parametrize` with factory functions per taxi type to avoid test duplication. Each factory uses a distinct RNG seed. Tested against descriptive-statistics as the representative step (simplest to validate, exercises the full orchestration path).
- **Lessons**: None new.

## 2026-03-05: Integration test — FHV fare revenue skip
- **Change Type**: `test`
- **Summary**: Added integration test verifying FHV fare revenue analysis returns `success: true` with `skipped: true` summary since FHV has no fare columns.
- **Context / Motivation**: Plan step 14.3 — needed to confirm the FHV skip path works end-to-end through the real endpoint with MinIO.
- **Decisions Made**: Verified the API server mock receives the skip metadata (`skipped`, `reason`). Did not assert on S3 output object since the executor uploads empty bytes — the important contract is the summary payload.
- **Lessons**: None new.

## 2026-03-05: Integration test — API Server contract and failure handling
- **Date**: 2026-03-05T11:57+01:00
- **Change Type**: `test`
- **Summary**: Added integration tests verifying POST payload shape matches `AnalyticalResultCreate` contract and that API Server failure does not fail the analytical step.
- **Context / Motivation**: Plan step 14.4 — needed to confirm the analyzer's `post_analytical_result` call sends all required fields with correct types, and that a failed API post still results in `success: true`.
- **Decisions Made**: Created separate test file `test_integration_api_server.py` to keep API contract tests isolated. Validated field set, types, and value constraints (e.g. `result_type` min_length=1, `computation_time_seconds` >= 0) matching the API server's `AnalyticalResultCreate` model.
- **Lessons**: None new.

## 2026-03-05: Verify all analyzer integration tests pass
- **Change Type**: `chore`
- **Summary**: Ran full analyzer test suite (230/230 pass, including 5 integration tests) to complete step 14.5.
- **Context / Motivation**: Gate check before documentation step — confirms all integration tests (MinIO, API Server mock, FHV skip, all taxi types) are green.
- **Decisions Made**: No code changes needed; pure verification step.
- **Lessons**: None — clean pass.

## 2026-03-05T12:01 — Analyzer README

- **Change Type**: `docs`
- **Summary**: Created `src/analyzer/README.md` — service purpose, endpoints table, configuration table, run/test commands.
- **Context / Motivation**: Final task (15.1) in the analyzer implementation plan. All 230 tests pass; plan is now fully complete.
- **Decisions Made**: Followed `src/api_server/README.md` format. Included sample request JSON for clarity since all 5 endpoints share the same payload shape.
- **Lessons**: None.

## 2026-03-05

**Summary**: Updated scheduler's `AnalyzerRequest` with `taxi_type` and `job_execution_id` fields; refactored `send_job()` to route to per-step analyzer endpoints.
**Change Type**: `feat`
**Context / Motivation**: The analyzer service exposes 5 step-specific endpoints (`/analyze/descriptive-statistics`, etc.) requiring `taxi_type` and `job_execution_id`. The scheduler's client was still sending to a single `POST /analyze` with only `job`, `input_bucket`, `input_object`.
**Decisions Made**: Renamed `job` parameter to `step` for clarity. Used simple `str.replace('_', '-')` for step→endpoint mapping rather than a lookup dict — the convention is consistent. Kept `taxi_type` as `str` (not enum) in the scheduler since it's a pass-through value.
**Lessons**: None new — straightforward contract alignment.

## 2026-03-05T12:10
**Summary**: Added `extract_taxi_type()` to extract taxi type from MinIO object path prefixes in the scheduler.
**Change Type**: `feat`
**Context / Motivation**: The scheduler needs to extract taxi type from object paths (e.g., `yellow/2022/01/file.parquet` → `yellow`) before dispatching to the analyzer. Part of scheduler-analyzer integration plan, step 2.
**Decisions Made**: Simple string split on `/` and lookup in a set. Case-insensitive. Raises `ValueError` for unrecognized prefixes rather than returning `None` — fail-fast is safer here since an invalid taxi type would cause downstream failures anyway.
**Lessons**: None new.

---

## 2026-03-05: Scheduler API Server client
**Date**: 2026-03-05T12:10+01:00
**Summary**: Created `api_server_client.py` with `create_file_record` and `create_job_execution` functions for the scheduler to interact with the API Server.
**Change Type**: `feat`
**Context / Motivation**: The scheduler needs to create file records and job execution records in the API Server before dispatching analytical steps to the analyzer. Part of scheduler-analyzer integration plan, step 3.
**Decisions Made**: Functions raise on failure (httpx exceptions propagate) rather than returning success/failure booleans — the caller (Prefect flows) owns retry logic. Followed same httpx.Client pattern as `analyzer_client.py`.
**Lessons**: None new.

## 2026-03-05T12:13
**Summary**: Added `API_SERVER_URL` setting to scheduler `Settings` class, updated docker-compose env vars and config tests.
**Change Type**: `feat`
**Context / Motivation**: The Prefect flows (step 4) need `API_SERVER_URL` from config to create job execution records via the API Server before dispatching to the analyzer. This is a prerequisite for step 4 of the scheduler-analyzer integration plan.
**Decisions Made**: Default `http://localhost:8000` matches the API Server's default port. Added to docker-compose as `http://api-server:8000` for container networking.
**Lessons**: None new.

---

**Date**: 2026-03-05T12:15
**Summary**: Updated Prefect flows to pass `taxi_type` and `job_execution_id` to the analyzer, with API Server integration for file records and job executions.
**Change Type**: `feat`
**Context / Motivation**: Step 4 of the scheduler-analyzer integration plan. The analyzer's per-step endpoints require `taxi_type` and `job_execution_id` fields. The scheduler must extract taxi type from object paths and create job execution records via the API Server before dispatching each step.
**Decisions Made**: `taxi_type` extracted once at flow start (not per step). File record created once per flow. Job execution created per step. `pipeline_run_id` added as a required parameter to `process_file_flow` (callers must provide it). Fixed stale `job=` kwarg to `step=` in `send_job` call.
**Lessons**: None new.

---

**Date**: 2026-03-05T12:19+01:00
**Summary**: Verified full scheduler test suite passes (111/111) after scheduler-analyzer integration. Completed integration plan section 6.
**Change Type**: `chore`
**Context / Motivation**: Final verification step of the scheduler-analyzer integration plan. All prior steps (1-5) were complete; needed to confirm no regressions across the full test suite.
**Decisions Made**: No code changes needed — all 111 tests passed on first run. 32 Prefect logger warnings are benign (missing flow run context in test environment).
**Lessons**: None new.

## 2026-03-09

**Summary**: Created aggregator specification and implementation plan.
**Change Type**: `docs`
**Context / Motivation**: Last feature in the pipeline — aggregation service. User interview established: stateless, sync, JSON responses, fixed endpoints, Postgres summaries only via API Server. Five aggregation types: descriptive stats, taxi comparison, temporal patterns, data quality, pipeline performance.
**Decisions Made**:
- Postgres-only data source (no S3 parquet reads) — analyzer summaries are sufficient
- Fixed endpoints over generic query endpoint — simpler for future dashboard
- Sync request-response — aggregations are lightweight (JSONB queries, in-memory reshaping)
- 40-step atomic implementation plan covering setup, models, client, services, routes, tests, docker, docs
**Lessons**: API Server already supports all required filtering (result_type, taxi_type, year, month) — no new API Server endpoints needed.

## 2026-03-09

**Summary**: Created aggregator service directory structure (`src/server/`, `src/services/`, `tests/`) with `__init__.py` files.
**Change Type**: feat
**Context / Motivation**: First step of the aggregator implementation plan — foundational directory layout matching existing service conventions (analyzer).
**Decisions Made**: Matched analyzer's pattern of empty `__init__.py` files. No `pyproject.toml` yet (that's Step 2).
**Lessons**: None new.

## 2026-03-09: Aggregator pyproject.toml
**Summary**: Created `src/aggregator/pyproject.toml` with minimal dependencies (fastapi, uvicorn, pydantic, pydantic-settings, httpx) and dev deps (pytest, ruff).
**Change Type**: feat
**Context / Motivation**: Step 2 of aggregator implementation plan — unblocks all subsequent coding, linting, and testing steps.
**Decisions Made**: Kept dependencies minimal compared to analyzer (no polars, numpy, scipy, etc.) since aggregator only does HTTP calls and in-memory aggregation. Used hatchling build backend with `packages = ["src"]` per lessons learned.
**Lessons**: None new.

## 2026-03-09: Aggregator config, Dockerfile, docker-compose
- **Change Type**: `feat`
- **Summary**: Created `Settings` class for aggregator service with 5 env vars (API_SERVER_URL, SERVER_HOST, SERVER_PORT, LOG_LEVEL, REQUEST_TIMEOUT). Also created Dockerfile and docker-compose.yml as prerequisites for verification.
- **Context / Motivation**: Step 3 (+ Steps 23/24 as prerequisites) of the aggregator implementation plan. Config is the foundation for all downstream aggregator components.
- **Decisions Made**: Matched analyzer's config pattern (pydantic-settings BaseSettings + module-level SETTINGS singleton). Created Dockerfile/docker-compose early since they're needed for ruff verification of every subsequent step.
- **Lessons**: None new — existing patterns applied cleanly.

## 2026-03-09: Aggregator — FiltersApplied and DescriptiveStatsResponse models
- **Change Type**: `feat`
- **Summary**: Created `models.py` with `FiltersApplied`, `ColumnStats`, and `DescriptiveStatsResponse` pydantic models for the aggregator service.
- **Context / Motivation**: Step 4 of the aggregator implementation plan. These are the first response models — `FiltersApplied` is shared across all endpoints, `DescriptiveStatsResponse` is the response for `GET /aggregations/descriptive-stats`.
- **Decisions Made**: Added `ColumnStats` as a nested model for per-column stats (mean/min/max/percentiles) since the spec shows `aggregated_stats` as a dict of column name → stats object. All models use `ConfigDict(frozen=True)` since they're read-only DTOs.
- **Lessons**: None new.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Added `TaxiMetrics` and `TaxiComparisonResponse` pydantic models to `src/aggregator/src/server/models.py`.
- **Context / Motivation**: Step 5 of the aggregator implementation plan. These models define the response shape for `GET /aggregations/taxi-comparison`, which compares key metrics across taxi types.
- **Decisions Made**: `avg_fare`, `avg_trip_distance`, `avg_tip_percentage` are `float | None` because FHV has no fare data per the analyzer spec. `comparison` is `dict[str, TaxiMetrics]` keyed by taxi type string to match the spec's JSON shape.
- **Lessons**: None new.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Added `TemporalPatternsResponse` pydantic model to `src/aggregator/src/server/models.py`.
- **Context / Motivation**: Step 6 of the aggregator implementation plan. Defines the response shape for `GET /aggregations/temporal-patterns` — hourly/daily trip volume patterns and peak hours.
- **Decisions Made**: `hourly_avg_trips` uses `dict[str, float]` with string keys ("0"-"23") matching the spec JSON. `peak_hours` is `list[int]` for the top hours. `daily_avg_trips` uses `dict[str, float]` with day-name keys. All fields are required (no defaults) since the aggregation service always computes them.
- **Lessons**: None new.

## 2026-03-09T14:15+01:00
- **Change Type**: `feat`
- **Summary**: Added `OutlierMethodSummary` and `DataQualityResponse` pydantic models to `src/aggregator/src/server/models.py`.
- **Context / Motivation**: Step 7 of the aggregator implementation plan. Defines the response shape for `GET /aggregations/data-quality` — outlier detection summaries per method, quality violations, and overall removal rate.
- **Decisions Made**: `outlier_summary` uses `dict[str, OutlierMethodSummary]` with method name keys ("iqr", "zscore", "isolation_forest") matching the spec. `quality_violations` uses `dict[str, int]` for flexible violation type keys. Both models are frozen (immutable DTOs).
- **Lessons**: None new.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Added `PipelinePerformanceResponse`, `StepPerformance`, `PipelineSavings`, and `PipelineFiltersApplied` pydantic models to `src/aggregator/src/server/models.py`.
- **Context / Motivation**: Step 8 of the aggregator implementation plan. Defines the response shape for `GET /aggregations/pipeline-performance` — per-step computation time stats, checkpoint savings, and the extra `analytical_step` filter.
- **Decisions Made**: Created `PipelineFiltersApplied` extending `FiltersApplied` with `analytical_step` field rather than modifying the shared base model, keeping existing endpoints unaffected. All models frozen (immutable DTOs).
- **Lessons**: None new.

## 2026-03-09T14:17 — Step 9: API Server HTTP client
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/services/api_client.py` with `fetch_analytical_results()` (auto-pagination) and `fetch_pipeline_summary()`.
- **Context / Motivation**: Step 9 of the aggregator implementation plan. The API client is the foundation for all aggregation endpoints — every route depends on it to fetch data from the API Server.
- **Decisions Made**: Used module-level functions (not a class) since the client is stateless and reads config from the singleton `SETTINGS`. Auto-pagination loops with `PAGE_SIZE=1000` matching the API Server's max limit. Used `httpx.Client` as context manager per project conventions.
- **Lessons**: None new.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/services/descriptive_stats.py` with `aggregate_descriptive_stats()` function.
- **Context / Motivation**: Step 10 of the aggregator implementation plan. First aggregation service — computes cross-file aggregates from descriptive statistics analytical results.
- **Decisions Made**: Used the analyzer's actual `summary_data` shape (percentiles dict with p1-p99, distribution dict with mean/std/skewness/kurtosis) to drive aggregation. Min/max derived from p1/p99 percentiles across files. Mean of means for central tendency. All percentile keys averaged across files. Returns zero-state response for empty inputs rather than erroring.
- **Lessons**: None new.

## 2026-03-09: Add taxi comparison aggregation service
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/services/taxi_comparison.py` with `aggregate_taxi_comparison()` function (Step 11).
- **Context / Motivation**: Aggregator needs to compare key metrics across taxi types (yellow, green, fhv, fhvhv) from descriptive_statistics results.
- **Decisions Made**: Extract fare/distance/tip means from `summary_data.distribution`. Compute tip percentage as `tip_amount_mean / fare_amount_mean * 100`. Handle FHVHV column name variants (`trip_miles`, `tips`). FHV returns null for all fare-related fields since it has no fare data.
- **Lessons**: None new — followed existing `descriptive_stats.py` pattern.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/services/temporal_patterns.py` with `aggregate_temporal_patterns()` function (Step 12).
- **Context / Motivation**: Aggregator needs to merge temporal analysis results across files, primarily peak hour detection.
- **Decisions Made**: Analyzer's temporal `summary_data` only stores `peak_hours` (list of ints), not per-hour or per-day volume breakdowns. Therefore `hourly_avg_trips` and `daily_avg_trips` are returned as empty dicts. Peak hours are aggregated by frequency across files — hours appearing in >50% of files are kept; fallback to most common if threshold filters everything out.
- **Lessons**: Always check the actual analyzer output shape before implementing aggregation — the spec's response example may assume data that isn't in the summary.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/services/data_quality.py` with `aggregate_data_quality()` function (Step 13).
- **Context / Motivation**: Aggregator needs to summarize data cleaning metrics across files — outlier counts per detection method, quality violations, and overall removal rate.
- **Decisions Made**: Outlier counts are summed across all columns and files per method (iqr, zscore, isolation_forest). `avg_rate_percent` is computed as `total_outliers / total_rows * 100`. Quality violations are summed by key across files. Overall removal rate uses `rows_removed / rows_before` from `strategy_comparison.removal`. All values rounded to 2 decimal places.
- **Lessons**: The analyzer's `summary_data` for data cleaning has a well-structured shape (`outlier_counts` keyed by column then method, `quality_violations` as flat dict, `strategy_comparison` with removal/capping sub-dicts). Following the same aggregation pattern as prior services (extract summaries → accumulate → compute rates) keeps the code consistent.

## 2026-03-09: Add pipeline_performance aggregation service
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/services/pipeline_performance.py` with `aggregate_pipeline_performance()` function and 9 tests.
- **Context / Motivation**: Step 14 of the aggregator implementation plan. This is the thesis-critical aggregation — it groups analytical results by step, computes per-step computation time stats, and includes checkpoint savings from the API Server.
- **Decisions Made**: Used `file_info.file_id` for distinct file counting with fallback to `len(results)` when file_info is absent. Rounds all float outputs to 2 decimal places for consistency with other aggregation services.
- **Lessons**: None new — followed established patterns from `data_quality.py`.

## 2026-03-09: Aggregator routes.py with GET /health
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/server/routes.py` with a `health_router` (`GET /health`) and an empty `router` with prefix `/aggregations` for future aggregation endpoints.
- **Context / Motivation**: Step 15 of the aggregator implementation plan. The health endpoint is needed before wiring the FastAPI app and is a prerequisite for all subsequent route steps.
- **Decisions Made**: Used two separate routers — `health_router` for `/health` (no prefix) and `router` for `/aggregations` (prefixed) — to match the spec's URL structure where health is at root level.
- **Lessons**: None new.

## 2026-03-09T14:32 — Step 16: GET /aggregations/descriptive-stats route
- **Change Type**: `feat`
- **Summary**: Added `GET /aggregations/descriptive-stats` endpoint to `routes.py`, wiring `api_client.fetch_analytical_results` → `descriptive_stats.aggregate_descriptive_stats` → `DescriptiveStatsResponse`.
- **Context / Motivation**: Step 16 of the aggregator implementation plan. First real aggregation endpoint — unblocks all subsequent route steps and route tests.
- **Decisions Made**: Catch `httpx.HTTPStatusError` and `httpx.ConnectError` separately, returning 502 Bad Gateway with structured error JSON per the spec. Query params match the spec's common filter set (`taxi_type`, `start_year`, `start_month`, `end_year`, `end_month`).
- **Lessons**: None new.

## 2026-03-09

- **Change Type**: `feat`
- **Summary**: Added `GET /aggregations/taxi-comparison` route to `routes.py`, wiring `fetch_analytical_results` (×4 taxi types) → `aggregate_taxi_comparison` → `TaxiComparisonResponse`.
- **Context / Motivation**: Step 17 of the aggregator implementation plan. Compares key metrics across all 4 taxi types (yellow, green, fhv, fhvhv).
- **Decisions Made**: `taxi_type` query param omitted from this endpoint per spec (it always compares all types). Year/month filters still apply. Error handling follows the same pattern as the descriptive-stats route.
- **Lessons**: None new.

## 2026-03-09T14:36

- **Change Type**: `feat`
- **Summary**: Added `GET /aggregations/temporal-patterns` route to `routes.py`, wiring `fetch_analytical_results(result_type="temporal_analysis")` → `aggregate_temporal_patterns` → `TemporalPatternsResponse`.
- **Context / Motivation**: Step 18 of the aggregator implementation plan. Aggregates hourly/daily trip volume patterns and peak hours across files.
- **Decisions Made**: Follows identical error handling pattern (502 on HTTPStatusError/ConnectError) as existing routes. Accepts all common filters (taxi_type, start_year, start_month, end_year, end_month).
- **Lessons**: None new.

## 2026-03-09: Add GET /aggregations/data-quality route
- **Change Type**: `feat`
- **Summary**: Added data-quality aggregation endpoint to aggregator routes, wiring api_client → data_quality service → DataQualityResponse.
- **Context / Motivation**: Step 19 of aggregator implementation plan. Follows identical pattern to existing descriptive-stats, taxi-comparison, and temporal-patterns routes.
- **Decisions Made**: Reused exact same error handling pattern (HTTPStatusError → 502, ConnectError → 502) as sibling routes. Used `result_type="data_cleaning"` per spec.
- **Lessons**: None new — straightforward wiring task.

## 2026-03-09: Add GET /aggregations/pipeline-performance route
- **Change Type**: `feat`
- **Summary**: Added the pipeline-performance endpoint to the aggregator routes, wiring fetch_analytical_results + fetch_pipeline_summary → aggregate_pipeline_performance → PipelinePerformanceResponse.
- **Context / Motivation**: Step 20 of the aggregator implementation plan. This is the thesis-critical endpoint for demonstrating checkpointing value. Last route needed before the FastAPI app can be assembled.
- **Decisions Made**: Followed the exact same error-handling pattern (HTTPStatusError, ConnectError → 502) as the other four routes. Used PipelineFiltersApplied (with analytical_step) instead of FiltersApplied. Passed analytical_step as result_type to fetch_analytical_results (None means fetch all types).
- **Lessons**: None new — straightforward pattern replication.

## 2026-03-09: Create aggregator FastAPI main.py entrypoint
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/server/main.py` — FastAPI app with lifespan handler, health + aggregation routers included, uvicorn entrypoint.
- **Context / Motivation**: Step 21 of aggregator implementation plan. Blocker for all downstream work (tests, docker run, integration).
- **Decisions Made**: Stateless lifespan (logging config only, no DB init). Matched api_server pattern but stripped DB-specific logic. Used `contextmanager` for lifespan per existing project convention.
- **Lessons**: None new — straightforward step.

## 2026-03-09: Aggregator uvicorn entrypoint
- **Date**: 2026-03-09T14:42+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/src/main.py` — thin uvicorn entrypoint importing app and settings.
- **Context / Motivation**: Step 22 of aggregator implementation plan. Required for running the service directly via `python -m src.main` or `uv run python src/main.py`.
- **Decisions Made**: Matched exact pattern from `src/analyzer/src/main.py`. No additional logic needed — the server main.py already has the lifespan and router wiring.
- **Lessons**: None new — trivial step.

---

### 2026-03-09T14:43 — Step 25: Create `tests/conftest.py` with shared fixtures
- **Change Type**: `feat`
- **Summary**: Created `src/aggregator/tests/conftest.py` with shared test fixtures: `client` (TestClient), `mock_fetch_results`, `mock_fetch_pipeline_summary`, and factory helpers `make_filters`/`make_pipeline_filters`.
- **Context / Motivation**: Step 25 of aggregator implementation plan. Shared fixtures needed for upcoming route tests (Steps 32-37) and to reduce duplication across service test files.
- **Decisions Made**: Exposed `make_filters`/`make_pipeline_filters` as plain functions (not fixtures) since they're stateless factories — tests import them directly. Mock fixtures patch at the routes module level where the functions are imported.
- **Lessons**: None new.

## 2026-03-09: Add API client tests for aggregator service
- **Change Type**: `test`
- **Summary**: Created `tests/test_api_client.py` with 8 tests covering `fetch_analytical_results` (single page, pagination, empty results, filter forwarding, None filter omission, error propagation) and `fetch_pipeline_summary` (success, error).
- **Context / Motivation**: Step 26 of the aggregator implementation plan. API client is the foundation for all aggregation routes — testing it first ensures downstream route tests build on verified code.
- **Decisions Made**: Mocked `httpx.Client` at the module level rather than using `respx` or similar — keeps dependencies minimal (only unittest.mock + pytest needed).
- **Lessons**: None new.

## 2026-03-09T14:46 — Step 27: Tests for aggregate_descriptive_stats
- **Date**: 2026-03-09
- **Change Type**: `test`
- **Summary**: Created `tests/test_descriptive_stats.py` with 7 tests covering `aggregate_descriptive_stats` — empty results, single file, multiple files (mean averaging, min/max across files, different columns), missing summary_data, and filter passthrough.
- **Context / Motivation**: Step 27 of the aggregator implementation plan. Validates the core aggregation logic for descriptive statistics before route-level tests.
- **Decisions Made**: Followed existing test pattern from `test_data_quality.py` — helper `_make_result` factory, direct function calls (no mocking needed for pure aggregation logic).
- **Lessons**: None new.

## 2026-03-09: Add taxi comparison aggregation tests (Step 28)
- **Change Type**: `test`
- **Summary**: Created `tests/test_taxi_comparison.py` with 9 tests covering `aggregate_taxi_comparison`.
- **Context / Motivation**: Step 28 of the aggregator implementation plan — test coverage for taxi type comparison aggregation.
- **Decisions Made**: Tests cover all types present, missing types, FHV null fare fields, multiple files per type, tip percentage calculation, FHVHV alternate column names (trip_miles/tips), empty results list, and filter passthrough.
- **Lessons**: None new — followed existing test patterns from `test_descriptive_stats.py`.

## 2026-03-09T14:49 — Step 29: Tests for aggregate_temporal_patterns
- **Change Type**: `test`
- **Summary**: Created `tests/test_temporal_patterns.py` with 8 tests covering `aggregate_temporal_patterns`.
- **Context / Motivation**: Step 29 of the aggregator implementation plan — test coverage for temporal pattern aggregation.
- **Decisions Made**: Tests cover empty results, single file, multiple files with peak hour consensus (threshold > half), fallback when threshold filters all hours, missing summary_data, empty peak hours, filter passthrough, and sorted output verification.
- **Lessons**: None new — followed existing test patterns.

## 2026-03-09
- **Change Type**: `test`
- **Summary**: Created `tests/test_data_quality.py` with 8 tests covering `aggregate_data_quality`.
- **Context / Motivation**: Step 30 of the aggregator implementation plan — test coverage for data quality aggregation.
- **Decisions Made**: Tests cover empty results, single/multi-file outlier summation across columns, outlier rate calculation, quality violation summation across files, overall removal rate from strategy_comparison, missing summary_data handling, and filter passthrough.
- **Lessons**: None new — followed existing test patterns.

## 2026-03-09
- **Change Type**: `test`
- **Summary**: Created `tests/test_pipeline_performance.py` with 9 tests covering `aggregate_pipeline_performance`.
- **Context / Motivation**: Step 31 of the aggregator implementation plan — test coverage for pipeline performance aggregation.
- **Decisions Made**: Tests cover: empty results with summary passthrough, single step/file, multi-step grouping with min/max/avg/total verification, avg computation per file, pipeline summary inclusion, fallback file count when file_info is missing, skipping results without computation_time, filter passthrough, and empty pipeline summary defaults.
- **Lessons**: None new — followed existing test patterns.

## 2026-03-09: Step 32 — Health route test
- **Change Type**: `test`
- **Summary**: Created `tests/test_routes.py` with test for `GET /health` endpoint.
- **Context / Motivation**: Step 32 of the aggregator implementation plan — first route-level test, foundation for Steps 33-37.
- **Decisions Made**: Minimal test — single assertion on status code and JSON body. Uses existing `client` fixture from conftest.
- **Lessons**: None new.

## 2026-03-09: Add route tests for GET /aggregations/descriptive-stats
- **Change Type**: `test`
- **Summary**: Added 5 route-level tests for the descriptive-stats aggregation endpoint covering success, filter passthrough, empty results, HTTP error → 502, and unreachable → 502.
- **Context / Motivation**: Step 33 of the aggregator implementation plan — first route test class beyond the health check.
- **Decisions Made**: Used existing `mock_fetch_results` fixture from conftest. Tested both `httpx.HTTPStatusError` and `httpx.ConnectError` paths for 502 responses. Verified filter values propagate to both the API client call args and the response body.
- **Lessons**: None new.

## 2026-03-09: Add route tests for GET /aggregations/taxi-comparison
- **Change Type**: `test`
- **Summary**: Added 6 route-level tests for the taxi-comparison endpoint covering success with results, filters passthrough, empty results, FHV null fare fields, HTTP error → 502, and unreachable → 502.
- **Context / Motivation**: Step 34 of the aggregator implementation plan. Follows the same pattern established in Step 33 for descriptive-stats route tests.
- **Decisions Made**: Used side_effect callable to return different data per taxi_type, matching the route's loop over TAXI_TYPES. Verified FHV null fare fields as a distinct case since FHV data lacks fare columns.
- **Lessons**: None new — pattern was well-established from prior route tests.

## 2026-03-09
- **Change Type**: `test`
- **Summary**: Added 5 route-level tests for `GET /aggregations/temporal-patterns` covering success with results, filters passthrough, empty results, HTTP error → 502, and unreachable → 502.
- **Context / Motivation**: Step 35 of the aggregator implementation plan. Continues the route test pattern from Steps 33–34.
- **Decisions Made**: Verified peak hour aggregation logic (hours appearing in >50% of files) through the route layer. Used two files with overlapping peak hours to validate the threshold behavior.
- **Lessons**: None new — pattern consistent with prior route tests.

## 2026-03-09: Add route tests for GET /aggregations/data-quality
- **Change Type**: `test`
- **Summary**: Added 5 route-level tests for the data-quality aggregation endpoint covering success, filters, empty results, HTTP error → 502, and unreachable → 502.
- **Context / Motivation**: Step 36 of the aggregator implementation plan. Follows the established pattern from descriptive-stats, taxi-comparison, and temporal-patterns route tests.
- **Decisions Made**: Matched existing test structure exactly — same 5 test cases per route class. Used `data_cleaning` as the `result_type` matching the route implementation.
- **Lessons**: None new — straightforward pattern replication.

## 2026-03-09: Add route tests for GET /aggregations/pipeline-performance
- **Change Type**: `test`
- **Summary**: Added 6 route-level tests for the pipeline-performance endpoint covering success with results, filters, empty results, HTTP errors on both results and summary fetches, and unreachable API Server.
- **Context / Motivation**: Step 37 of the aggregator implementation plan. This was the next unchecked task — all prior steps (1–36) were already complete.
- **Decisions Made**: Followed the exact pattern of existing route test classes. Pipeline-performance is unique in requiring two mocks (`mock_fetch_results` + `mock_fetch_pipeline_summary`) and using `PipelineFiltersApplied` with the extra `analytical_step` filter. Added a dedicated test for HTTP error on the summary fetch (separate failure path from results fetch).
- **Lessons**: None new — pattern was well-established by prior route tests.

## 2026-03-09 — Aggregator integration tests (Step 38)
- **Change Type**: `test`
- **Summary**: Created end-to-end integration test for the aggregator service that seeds API Server with known analytical results and verifies all 5 aggregation endpoints.
- **Context / Motivation**: Step 38 of the aggregator implementation plan — validates the full flow from API Server data through aggregation logic to HTTP responses.
- **Decisions Made**:
  - Updated aggregator docker-compose to include api_server + postgres services
  - Fixed api_server lifespan from sync `@contextmanager` to async `@asynccontextmanager` (required for uvicorn startup)
  - Used unique run IDs (`uuid.uuid4().hex[:8]`) and `>=` assertions to handle persistent DB state across test runs
  - Used `green` taxi type for seeded data to minimize collision with other test data
- **Lessons**: See `lessons.md` entries on FastAPI sync lifespan and persistent DB integration tests

## 2026-03-09: Create aggregator service README
- **Change Type**: `docs`
- **Summary**: Created `src/aggregator/README.md` with service description, endpoints table, configuration, and run/test commands.
- **Context / Motivation**: Step 39 of aggregator implementation plan — service_readme rule requires a README for every completed service.
- **Decisions Made**: Kept it concise — endpoints table, env vars table, docker compose commands. No prose beyond what's needed.
- **Lessons**: None — straightforward documentation task.

## 2026-03-09: Complete aggregator implementation plan — Step 40
- **Change Type**: `chore`
- **Summary**: Marked Step 40 as complete. The Spec and Plan links in `specs/README.md` were already present and pointing to the correct files.
- **Context / Motivation**: Final step of the aggregator implementation plan. Verified `[Spec](aggregator.md) | [Plan](aggregator_implementation_plan.md)` links exist at line 164 of README.md and both target files exist.
- **Decisions Made**: No file changes needed to README.md — only updated the plan to reflect completion.
- **Lessons**: None — verification-only task.

---

## 2026-03-09: End-to-End Compose spec and implementation plan
- **Date**: 2026-03-09T16:16+01:00
- **Change Type**: `docs`
- **Summary**: Created spec, implementation plan, and README feature entry for a unified docker-compose that wires all services on a shared network.
- **Context / Motivation**: No way to run the full pipeline end-to-end — each service has isolated docker-compose with separate infrastructure. Needed for thesis evaluation (single-file pipeline run, checkpoint savings measurement).
- **Decisions Made**: Offset host ports (8010–8014) to avoid conflicts with per-service compose files. Two separate Postgres instances (api_server + scheduler) to match existing isolation. No code changes to services — only env var overrides. 13-step plan: 5 infra services, 5 app services, validation, e2e run, cleanup.
- **Lessons**: None — spec/plan authoring only.

## 2026-03-09 — End-to-end compose: directory + minio service
- **Change Type**: `feat`
- **Summary**: Created `src/infrastructure/compose/` directory and `docker-compose.yml` with the shared MinIO service definition (Steps 1-2 of end-to-end plan).
- **Context / Motivation**: The end-to-end compose file is needed to wire all services on a shared Docker network for full pipeline runs and thesis evaluation.
- **Decisions Made**: Host ports 9010/9011 to avoid conflicts with per-service compose files. Reused exact minio config from data_collector compose. Noted that the plan's inter-service URLs reference host ports (e.g., `http://api_server:8013`) but should use container ports (e.g., `http://api_server:8000`) since Dockerfiles hardcode CMD ports — will correct in subsequent steps.
- **Lessons**: Plan Step 2 verify command had a typo (`end_to_end` vs `compose` directory) — fixed in the plan update.

## 2026-03-09T16:23
- **Change Type**: `feat`
- **Summary**: Added `postgres_api_server` service to the unified compose file (Step 3 of end-to-end plan).
- **Context / Motivation**: The API Server needs its own Postgres instance in the unified compose. Modeled after the existing `postgres` service in `src/infrastructure/api_server/docker-compose.yml`, renamed to `postgres_api_server` to avoid name collisions with the scheduler's Postgres.
- **Decisions Made**: Kept same credentials (`api_server`/`api_server`), same host port (`5433`), same healthcheck. Used `postgres:16-alpine` image consistent with the reference.
- **Lessons**: None new — straightforward infrastructure step.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Added `postgres_scheduler` service to the unified end-to-end compose file (Step 4).
- **Context / Motivation**: The scheduler service requires its own Postgres instance. Modeled after the `postgres` service in `src/infrastructure/scheduler/docker-compose.yml`, renamed to `postgres_scheduler` to avoid name collisions.
- **Decisions Made**: Credentials `scheduler`/`scheduler`, host port `5434`, `postgres:16-alpine`, `pg_isready` healthcheck — all matching the reference compose.
- **Lessons**: None new — straightforward infrastructure step.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Added `prefect_server` service to the unified end-to-end compose file (Step 5).
- **Context / Motivation**: The scheduler depends on a Prefect server for flow orchestration. Modeled after the `prefect-server` service in `src/infrastructure/scheduler/docker-compose.yml`, renamed to `prefect_server` for naming consistency.
- **Decisions Made**: Host port `4210` (avoids collision with scheduler's standalone `4200`), `prefecthq/prefect:3-latest` image, python urllib healthcheck per lessons learned (no curl/wget in Prefect image).
- **Lessons**: None new — applied existing lesson about Prefect Docker image lacking curl.

## 2026-03-09: Add api_server service to end-to-end compose
- **Change Type**: feat
- **Summary**: Added `api_server` service definition to `src/infrastructure/compose/docker-compose.yml` with port 8013, DATABASE_URL pointing to `postgres_api_server`, healthcheck, and `depends_on` for postgres.
- **Context / Motivation**: Step 6 of the end-to-end compose implementation plan — wiring the API server into the unified compose for full pipeline runs.
- **Decisions Made**: Overrode Dockerfile CMD with explicit `command` to use port 8013 (instead of default 8000) to avoid conflicts with per-service compose files. Used python urllib healthcheck consistent with prefect_server pattern.
- **Lessons**: Initial startup failed with DNS resolution error due to a race condition when postgres was recreated. On retry with stable postgres, api_server started healthy. No new lesson needed — `depends_on: service_healthy` handles this correctly.

## 2026-03-09T16:29
- **Change Type**: feat
- **Summary**: Added `analyzer` service definition to `src/infrastructure/compose/docker-compose.yml` with port 8012, MinIO and API Server connectivity, utilities volume mount.
- **Context / Motivation**: Step 7 of the end-to-end compose implementation plan — wiring the analyzer into the unified compose for full pipeline runs.
- **Decisions Made**: Overrode Dockerfile CMD with explicit `command` to use port 8012 (instead of default 8002). Set `PYTHONPATH=/app:/` so `from utilities.s3 import ...` resolves from the `/utilities` volume mount. No healthcheck added since the plan doesn't specify one and the analyzer has no lightweight health endpoint.
- **Lessons**: None — straightforward compose wiring following the established pattern from Step 6.

## 2026-03-09T16:30

- **Change Type**: feat
- **Summary**: Added `scheduler` service definition to `src/infrastructure/compose/docker-compose.yml` with port 8011, wired to postgres_scheduler, prefect_server, analyzer, and api_server.
- **Context / Motivation**: Step 8 of the end-to-end compose implementation plan — wiring the scheduler into the unified compose for full pipeline runs.
- **Decisions Made**: Overrode Dockerfile CMD with explicit `command` to use port 8011 (instead of default 8001). Updated all service URLs to match the compose service names (e.g., `http://analyzer:8012` instead of `http://analyzer:8002`). Used `service_started` for analyzer dependency (no healthcheck) and `service_healthy` for postgres_scheduler, prefect_server, and api_server. Preserved all `STEP_*_BUCKET` env vars from the scheduler's own compose.
- **Lessons**: None — straightforward compose wiring following the established pattern from Steps 6-7.

## 2026-03-09T16:31 — Add data_collector to unified compose
- **Change Type**: feat
- **Summary**: Added `data_collector` service definition to `src/infrastructure/compose/docker-compose.yml` with port 8010, wired to minio and scheduler.
- **Context / Motivation**: Step 9 of the end-to-end compose implementation plan — the data_collector is the pipeline entry point, required for end-to-end runs.
- **Decisions Made**: Overrode Dockerfile CMD with explicit `command` to use port 8010 and `src.server.app:app` (data_collector uses `app.py`, not `main.py`). Set `SCHEDULER_URL` to `http://scheduler:8011` matching the scheduler's compose port. Only depends on `minio` (healthy) — no direct dependency on scheduler since it's a runtime call, not a startup requirement.
- **Lessons**: None — straightforward compose wiring following the established pattern.

## 2026-03-09T16:33

- **Change Type**: feat
- **Summary**: Added `aggregator` service definition to `src/infrastructure/compose/docker-compose.yml` with port 8014, wired to api_server.
- **Context / Motivation**: Step 10 of the end-to-end compose implementation plan — the aggregator provides cross-file analytics endpoints needed for thesis reporting.
- **Decisions Made**: Adapted from standalone aggregator compose: changed `API_SERVER_URL` to `http://api_server:8013` (unified port), `SERVER_PORT` to `8014` (per plan), added healthcheck against `/health`. No volume mount needed (aggregator has no shared utilities dependency). Depends only on `api_server` (healthy).
- **Lessons**: None — straightforward compose wiring following the established pattern.

## 2026-03-09T16:34 — End-to-end compose validation (Step 11)
- **Change Type**: fix
- **Summary**: Fixed scheduler and aggregator lifespan functions (sync `@contextmanager` → `@asynccontextmanager`), validated all 9 services start healthy in unified compose.
- **Context / Motivation**: Step 11 of end-to-end compose plan — bring up all services and verify they start. Both scheduler and aggregator crashed on startup with `TypeError: '_GeneratorContextManager' object does not support the asynchronous context manager protocol`.
- **Decisions Made**: Minimal fix — only changed the decorator and function signature in both `main.py` files. No other code touched. This is a known pattern from `lessons.md` (2026-03-09 entry).
- **Lessons**: The existing lesson about async lifespans was validated again. TestClient masks this bug since it handles both sync/async lifespans internally — only real uvicorn startup reveals it.

## 2026-03-09T16:42 — fix: pass pipeline_run_id to process_file_flow
- **Change Type**: `fix`
- **Summary**: `SchedulerService._run_flows_concurrently` was not passing `pipeline_run_id` to `process_file_flow`, causing a silent `TypeError` in the thread pool. Flows never executed.
- **Context / Motivation**: Discovered while attempting Step 12 (end-to-end pipeline run). The data collector successfully uploaded files and notified the scheduler, but no Prefect flow runs were created and no records appeared in the API server.
- **Decisions Made**: Generate `uuid.uuid4().hex` per `schedule_batch`/`resume_failed` call. All files in the same batch share one `pipeline_run_id`.
- **Lessons**: See `lessons.md` § "ThreadPoolExecutor silently swallows missing keyword arguments"

## 2026-03-09T17:16 — End-to-end pipeline: Step 12 single-file run
- **Change Type**: fix
- **Summary**: Fixed bucket mismatch and timeout issues preventing end-to-end pipeline execution. Ran successful single-file pipeline (yellow taxi Jan 2024) through all 5 analytical steps.
- **Context / Motivation**: Step 12 of end-to-end compose plan required validating the full pipeline works. Three bugs blocked it: hardcoded bucket in SchedulerService, wrong STEP_*_BUCKET env vars in compose, and insufficient analyzer timeout.
- **Decisions Made**: (1) Removed `bucket` from `SchedulerService.__init__`, made it a parameter of `schedule_batch()` — cleaner than keeping a default that's always wrong. (2) Changed all STEP_*_BUCKET vars to `data-collector` since the analyzer always reads the original file, not cleaned output. (3) Added `ANALYZER_TIMEOUT=300s` config setting rather than just bumping the hardcoded default.
- **Lessons**: See lessons.md — hardcoded bucket mismatch, analyzer timeout too short.

## 2026-03-09: End-to-end compose teardown (Step 13)
- **Change Type**: chore
- **Summary**: Executed `docker compose down --remove-orphans --volumes` for the unified compose stack. Verified all containers and project volumes removed.
- **Context / Motivation**: Final step of the end-to-end compose implementation plan. All 13 steps now complete.
- **Decisions Made**: None — straightforward teardown.
- **Lessons**: None.

## 2026-03-09T17:37 — feat: Add GET /metrics/step-performance endpoint
- **Change Type**: `feat`
- **Summary**: Added step-level performance metric (metric #6 from thesis_metrics.md) to the API server — avg/min/max/stddev computation time per analytical step.
- **Context / Motivation**: The thesis_metrics spec defines 6 metrics; only 3 were implemented. Step-level performance is the most foundational — it characterizes the computational cost of each analytical step, which is the baseline data for the thesis.
- **Decisions Made**: Implemented as a single atomic task covering model + service function + route + tests. Used `func.stddev` (Postgres sample stddev) which returns NULL for single values — tested for this edge case. Ordered results by avg_seconds descending to surface the most expensive steps first.
- **Lessons**: None new — straightforward addition following existing patterns.

## 2026-03-09
- **Change Type**: `feat`
- **Summary**: Added `PipelineEfficiencyStatistic` and `PipelineEfficiencyResponse` Pydantic models for Metric #5 (Pipeline Efficiency).
- **Context / Motivation**: Thesis metrics plan Step 2 — models needed before the service function and route can be implemented.
- **Decisions Made**: Followed existing pattern (frozen ConfigDict, list wrapper response). Fields match the SQL spec: `overall_status`, `file_count`, `avg_efficiency_ratio`, `avg_computation_minutes`, `avg_elapsed_minutes`.
- **Lessons**: None new — straightforward model addition.

## 2026-03-09T17:43 — calculate_pipeline_efficiency function
- **Change Type**: `feat`
- **Summary**: Added `calculate_pipeline_efficiency` function to `src/api_server/src/services/metrics.py` implementing thesis metric #5 (Pipeline Efficiency).
- **Context / Motivation**: Thesis metrics plan Step 3 — service function needed before the route and tests can be implemented.
- **Decisions Made**: Used `func.nullif` to avoid division by zero in efficiency ratio. Grouped by `overall_status`, filtered `total_elapsed_seconds > 0`. Rounded ratio to 4 decimal places, minutes to 2. Followed existing pattern from `calculate_step_performance`.
- **Lessons**: None new — straightforward metric function following established patterns.

## 2026-03-09T17:45 — Add GET /metrics/pipeline-efficiency route
- **Change Type**: `feat`
- **Summary**: Wired `calculate_pipeline_efficiency` to `GET /metrics/pipeline-efficiency` endpoint in api_server routes.
- **Context / Motivation**: Thesis metrics plan Step 4 — the models and service function were already implemented in Steps 2-3; this step exposes them via HTTP.
- **Decisions Made**: Followed the exact pattern of the existing `get_step_performance` route — import models + function, construct response from list of dicts. No new logic needed.
- **Lessons**: None — straightforward wiring task.

## 2026-03-09: Add tests for calculate_pipeline_efficiency
- **Change Type**: `test`
- **Summary**: Added 5 pytest tests for `calculate_pipeline_efficiency` in `test_metrics.py` covering empty results, single file, multiple status groups, averaging, and zero-elapsed exclusion.
- **Context / Motivation**: Step 5 of thesis_metrics_implementation_plan — function was implemented in Step 3 but had no test coverage.
- **Decisions Made**: Followed existing test patterns (fixtures, helper functions, class grouping). Tested edge cases: zero elapsed time exclusion, multi-file averaging within same status group.
- **Lessons**: None new — straightforward test addition matching existing conventions.

## 2026-03-09T17:49 — Route tests for GET /metrics/pipeline-efficiency
- **Change Type**: `test`
- **Summary**: Added 4 route-level tests for the pipeline-efficiency metrics endpoint in `test_routes.py`.
- **Context / Motivation**: Step 6 of thesis_metrics_implementation_plan — completes the Pipeline Efficiency metric section (Steps 2-6).
- **Decisions Made**: Followed existing metrics route test pattern (TestGetCheckpointSavings, TestGetStepPerformance). Tests cover empty state, single file with correct ratio calculation, grouping by status, and exclusion of zero-elapsed files.
- **Lessons**: None new — straightforward pattern replication.

## 2026-03-09T17:51 — RecoveryTimeResponse model

- **Change Type**: `feat`
- **Summary**: Added `RecoveryTimeResponse` pydantic model to `api_server/src/server/models.py` for thesis metric #2 (average recovery time improvement).
- **Context / Motivation**: Step 7 of `thesis_metrics_implementation_plan.md` — the model is needed before the calculation function and route can be built.
- **Decisions Made**: Four float fields matching the SQL spec output (`avg_recovery_with_checkpoint_seconds`, `avg_recovery_without_checkpoint_seconds`, `avg_time_saved_seconds`, `percent_improvement`). Frozen config, consistent with all other metric response models.
- **Lessons**: None new — straightforward model addition.

## 2026-03-09: Add calculate_recovery_time_improvement function
- **Change Type**: `feat`
- **Summary**: Added `calculate_recovery_time_improvement` to `src/api_server/src/services/metrics.py` — Metric #2 from thesis_metrics spec.
- **Context / Motivation**: Thesis needs to demonstrate checkpoint value by comparing actual recovery time (retried steps only) vs hypothetical full restart.
- **Decisions Made**: Followed same pattern as existing metrics functions (query files with retries, iterate, aggregate). Returns dict matching `RecoveryTimeResponse` model fields. Returns zeros for all fields when no files have retries.
- **Lessons**: None new — straightforward addition following established patterns.

## 2026-03-09 — Add GET /metrics/recovery-time route
- **Change Type**: `feat`
- **Summary**: Wired `calculate_recovery_time_improvement` to `GET /metrics/recovery-time` endpoint returning `RecoveryTimeResponse`.
- **Context / Motivation**: Step 9 of thesis_metrics_implementation_plan — the function and model already existed from Steps 7-8, this step adds the HTTP route.
- **Decisions Made**: Followed the exact same pattern as `get_pipeline_efficiency` — no query params needed since the metric is always an aggregate.
- **Lessons**: None new — straightforward wiring task.

## 2026-03-09: Add tests for calculate_recovery_time_improvement
- **Change Type**: `test`
- **Summary**: Added 5 tests for `calculate_recovery_time_improvement` in `test_metrics.py` — empty result, excludes non-completed, single file, multi-file averages, edge case with no retry job executions.
- **Context / Motivation**: Step 10 of thesis_metrics_implementation_plan. The function was implemented but untested.
- **Decisions Made**: Followed existing test patterns (pipeline efficiency tests). Tested the edge case where a file has `retry_count > 0` but no job executions with `retry_count > 0` — this yields 100% improvement since checkpoint cost is 0.
- **Lessons**: None new — straightforward test addition.

## 2026-03-09 — Route tests for GET /metrics/recovery-time
- **Change Type**: `test`
- **Summary**: Added 4 route-level tests for the recovery-time metrics endpoint in test_routes.py.
- **Context / Motivation**: Final step in the thesis_metrics_implementation_plan — all other steps were complete.
- **Decisions Made**: Used PATCH /job-executions/{id} to set computation_time_seconds since POST /job-executions doesn't accept that field. Followed the same pattern as TestGetPipelineEfficiency tests.
- **Lessons**: POST /job-executions only accepts creation fields (file_id, pipeline_run_id, step_name, status, retry_count). Mutable fields like computation_time_seconds must be set via PATCH.

## 2026-03-09T18:06 — Add update_job_execution and update_file API client functions
- **Change Type**: `feat`
- **Summary**: Added `update_job_execution` and `update_file` HTTP client functions to `src/scheduler/src/services/api_server_client.py` for PATCH endpoints.
- **Context / Motivation**: The scheduler's `process_file_flow` creates file records and job executions but never updates them after steps complete/fail. All thesis metrics (checkpoint savings, recovery time, pipeline efficiency) depend on these fields being populated. These client functions are the foundation for wiring metrics tracking.
- **Decisions Made**: Followed existing client function patterns (httpx.Client context manager, verify=False, 25s timeout). Both functions build payload dynamically, excluding None fields. Neither returns a value — they raise on failure (consistent with the existing pattern where callers handle errors).
- **Lessons**: None new — straightforward addition following established patterns.

## 2026-03-09T20:33 — Wire update_file calls into process_file_flow
- **Change Type**: `feat`
- **Summary**: Wired `update_file` into `process_file_flow` lifecycle to populate file-level metrics (overall_status, total_computation_seconds, total_elapsed_seconds, retry_count) via API Server PATCH.
- **Context / Motivation**: Step 3 of pipeline_metrics_tracking_plan. The files table fields remained at defaults because the flow never called update_file. Thesis metrics queries depend on these fields.
- **Decisions Made**: Called update_file at 4 lifecycle points: flow start (in_progress), after each step success (cumulative computation), on failure (failed + elapsed), on completion (completed + elapsed + computation). For resume, set retry_count=1 (not incremented from current value — would require a GET first, which adds complexity for minimal benefit). Added update_file mock to all 9 existing TestProcessFileFlow tests to prevent real HTTP calls.
- **Lessons**: When adding a new function call to production code that existing tests don't mock, all tests break with real HTTP errors. Must add the mock to existing tests as part of the production code change, not defer to a separate "update tests" step.

## 2026-03-09
- **Change Type**: `test`
- **Summary**: Added `update_file` lifecycle assertions to 4 existing prefect flow tests, and marked Step 2 (already implemented) complete.
- **Context / Motivation**: Step 4 of pipeline_metrics_tracking_plan. Tests mocked `update_file` but never asserted call patterns. Without assertions, regressions in file status updates would go undetected. Step 2 was already implemented in code but not marked done.
- **Decisions Made**: Added assertions to `test_all_steps_succeed` (7 calls: 1 in_progress + 5 per-step + 1 completed), `test_fails_at_second_step` (3 calls), `test_resume_from_third_step` (5 calls with retry_count=1), `test_fails_at_first_step` (2 calls). Verified call counts, status values, and presence of timing fields.
- **Lessons**: None new — straightforward assertion additions.

---

### 2026-03-09T20:58 — Checkpoint lifecycle integration test
- **Change Type**: `test`
- **Summary**: Added `TestCheckpointLifecycle` class to `test_scheduler.py` with a test that verifies the full schedule → fail → resume checkpoint flow through real Postgres state.
- **Context / Motivation**: Existing tests verified each checkpoint piece in isolation (flow failure state, resume_failed reading DB, flow skipping steps). No test chained `schedule_batch` → flow failure writing state → `resume_failed` → flow dispatch with `start_step`. This gap meant a wiring bug between these components could go undetected.
- **Decisions Made**: Used a `side_effect` on the mocked flow to simulate the flow writing failure state to the real Postgres DB during `schedule_batch`. Then called `resume_failed` and asserted it dispatched a flow with `start_step=STEPS[2]`, proving the checkpoint was read correctly. This tests the integration seam without needing the full Prefect runtime.
- **Lessons**: None new.

---

## 2026-03-12

- **Change Type**: `feat`
- **Summary**: Added `validate_step_names()` function to `src/scheduler/src/services/pipeline.py` — validates step names against the `STEPS` list and returns invalid ones.
- **Context / Motivation**: Step 1 of the scheduler checkpoint configuration plan. The scheduler needs to validate `skip_checkpoints` step names from API requests before threading them through the pipeline. This function is the foundation for the 422 validation in the route layer.
- **Decisions Made**: Pure function using a set lookup for O(1) membership checks. Returns invalid names (not a bool) so the caller can include them in error messages. Keyword-only argument to match project convention.
- **Lessons**: None new.

## 2026-03-12
- **Change Type**: `feat`
- **Summary**: Added `skip_checkpoints: list[str] = Field(default_factory=list)` to `ScheduleRequest` model in `src/scheduler/src/server/models.py`.
- **Context / Motivation**: Step 2 of the scheduler checkpoint configuration plan. The API needs to accept an optional list of step names for which checkpoint persistence should be skipped, enabling thesis experiments comparing pipeline recovery with and without checkpoints.
- **Decisions Made**: Used `Field(default_factory=list)` for the default to maintain backward compatibility — existing callers that don't send the field get an empty list (all steps checkpointed). No validation at the model level; step name validation will be handled in the route layer (Step 3).
- **Lessons**: None new.

## 2026-03-12
- **Change Type**: `feat`
- **Summary**: Added `skip_checkpoints` validation in `routes.py` — returns 422 with invalid step names if `skip_checkpoints` contains unrecognized pipeline steps.
- **Context / Motivation**: Step 3 of the scheduler checkpoint configuration plan. The route layer must validate `skip_checkpoints` before passing the request to the service layer, providing clear error messages for invalid step names.
- **Decisions Made**: Validation runs before any service call to fail fast. Uses `HTTPException` with 422 status and includes the invalid names in the detail message for debuggability. Reuses `validate_step_names()` from Step 1.
- **Lessons**: None new.

## 2026-03-12T22:08

- **Change Type**: `feat`
- **Summary**: Threaded `skip_checkpoints` through `schedule_batch()` → `_run_flows_concurrently()` → `process_file_flow()` args.
- **Context / Motivation**: Step 4 of the scheduler checkpoint configuration plan. The validated `skip_checkpoints` list from the route needs to reach the Prefect flow so it can conditionally skip state persistence. This step wires the parameter through the scheduler service layer.
- **Decisions Made**: Used `list[str] | None = None` default on `schedule_batch` and `_run_flows_concurrently` to avoid breaking existing callers (e.g., `resume_failed` which has no skip list). Normalized to `[]` via `or []` before passing downstream. Added `skip_checkpoints` to `process_file_flow` signature as accepted-but-unused (logic deferred to Step 5).
- **Lessons**: None new.

## 2026-03-12T22:12 — Step 5: Conditional checkpoint skip in process_file_flow
- **Change Type**: `feat`
- **Summary**: Implemented conditional `save_job_state` skip in `process_file_flow` for steps listed in `skip_checkpoints`.
- **Context / Motivation**: Step 5 of the scheduler checkpoint configuration plan. This is the core logic that makes checkpoint skipping work — all plumbing (steps 1–4) was already in place, and this step actually uses the `skip_checkpoints` parameter to conditionally bypass state persistence.
- **Decisions Made**: Captured `just_completed = next_step` before reassigning `next_step` to the next step, then checked `just_completed not in (skip_checkpoints or [])`. The `or []` handles the `None` default. Failure-path `save_job_state` remains unconditional per spec — only the success-path call is gated.
- **Lessons**: None new.

---

## 2026-03-12

- **Change Type**: `test`
- **Summary**: Added tests for `skip_checkpoints` field on `ScheduleRequest` model. Verified Step 6 (`validate_step_names` tests) was already complete.
- **Context / Motivation**: Steps 6–7 of the scheduler checkpoint configuration plan. Step 6 tests were already present in `test_pipeline.py` from a prior session. Step 7 added 3 new tests to `test_models.py` covering default value, valid step names, and serialization.
- **Decisions Made**: Added tests inline in the existing `TestScheduleRequest` class rather than creating a new test class, since the tests are about the same model.
- **Lessons**: None new.

## 2026-03-12: Route test for invalid skip_checkpoints → 422
- **Date**: 2026-03-12T22:17+01:00
- **Change Type**: `test`
- **Summary**: Added route test verifying POST /scheduler/schedule with invalid `skip_checkpoints` returns 422 with the invalid step name in the detail message.
- **Context / Motivation**: Step 8 of the scheduler checkpoint configuration plan. Validates the route-level validation logic added in Step 3.
- **Decisions Made**: Used `HTTP_422_UNPROCESSABLE_ENTITY` to match the existing route code, despite FastAPI deprecation warning favoring `HTTP_422_UNPROCESSABLE_CONTENT` — that's a pre-existing issue.
- **Lessons**: None new.

## 2026-03-12

- **Change Type**: `test`
- **Summary**: Added route test verifying POST /scheduler/schedule with valid `skip_checkpoints` returns 202 and passes the list through to the service.
- **Context / Motivation**: Step 9 of the scheduler checkpoint configuration plan. Complements the invalid-case test from Step 8 by covering the happy path with checkpoint skipping.
- **Decisions Made**: Single test covers status code, response body, and service call assertion — all part of the same concept (valid skip_checkpoints accepted).
- **Lessons**: None new.

## 2026-03-12

- **Date**: 2026-03-12T22:21+01:00
- **Change Type**: `test`
- **Summary**: Added 3 tests for `process_file_flow` verifying `save_job_state` is skipped for steps in `skip_checkpoints`, failure saves always happen, and skipping all steps leaves only the initial save.
- **Context / Motivation**: Step 10 of the scheduler checkpoint configuration plan. This is the core behavioral test for the entire checkpoint skipping feature — validates that the thesis experiment (run pipeline without checkpoints) works correctly.
- **Decisions Made**: Three tests cover the key scenarios: partial skip, failure on skipped step, and full skip. Assertions verify exact `save_job_state` call counts and argument contents rather than just counts.
- **Lessons**: None new.

---

## 2026-03-12
- **Change Type**: `chore`
- **Summary**: Ran ruff check and ruff format on all 9 files modified during the scheduler checkpoint config feature. All passed clean (0 issues, 0 reformats). Verified all 136 scheduler tests pass.
- **Context / Motivation**: Step 11 of the scheduler checkpoint configuration plan. Code quality gate before considering the feature complete.
- **Decisions Made**: Ran ruff against only the files touched by this feature (surgical scope per behavioral guidelines).
- **Lessons**: None new.

## 2026-03-12T22:26 — Step 12: Update scheduler README with checkpoint configuration

- **Change Type**: `docs`
- **Summary**: Updated scheduler README to document the `skip_checkpoints` parameter on the schedule endpoint and added a new "Checkpoint Configuration" section with behavior explanation and example request.
- **Context / Motivation**: Final step (Step 12) of the scheduler checkpoint configuration implementation plan. README must reflect the new API capability.
- **Decisions Made**: Added inline description to the endpoint table row and a dedicated section rather than just a footnote, since the feature has non-obvious resume behavior worth documenting.
- **Lessons**: None new.

## 2026-03-12

**Summary**: Created `src/translator/pyproject.toml` — foundation for the translator service.
**Change Type**: feat
**Context / Motivation**: First step of the translator service implementation plan. The pyproject.toml defines dependencies (fastapi, uvicorn, httpx, pydantic, pydantic-settings, psycopg2-binary) and build config, enabling all subsequent translator steps.
**Decisions Made**: Followed existing service conventions (aggregator/scheduler) for structure. Used hatchling build backend with `packages = ["src"]` layout. No testcontainers in dev deps — not needed per plan (translator uses direct Postgres, not testcontainers).
**Lessons**: None new.

## 2026-03-12: Translator Settings config class

**Summary**: Created `src/translator/src/services/config.py` with `Settings` class and tests.
**Change Type**: feat
**Context / Motivation**: Step 2 of translator service implementation plan. Settings class provides env-var-driven configuration for all downstream service URLs, database connection, and server binding.
**Decisions Made**: Followed existing service pattern (aggregator, scheduler). Port 8005 chosen for translator (8000-8003 taken by other services). Created directory structure (`src/server/`, `src/services/`, `tests/`) with `__init__.py` files. Tests run locally with `uv run` since no Docker infrastructure exists yet (Step 16-17).
**Lessons**: None new.

## 2026-03-12: TranslateRequest model
**Summary**: Created `TranslateRequest` pydantic model in `src/translator/src/server/models.py`.
**Change Type**: feat
**Context / Motivation**: Step 3 of translator service implementation plan. Foundational request model needed by the `POST /translator/translate` route.
**Decisions Made**: Frozen (immutable DTO), `min_length=1` on `dsl` field to reject empty strings at the validation boundary. Matched existing project style (ConfigDict, Field).
**Lessons**: None new.

## 2026-03-12: RunResponse model
**Summary**: Created `RunResponse` pydantic model in `src/translator/src/server/models.py`.
**Change Type**: feat
**Context / Motivation**: Step 4 of translator service implementation plan. Response model for `POST /translator/translate` (202 Accepted) returning the `run_id` for async polling.
**Decisions Made**: Frozen (immutable DTO), single `run_id: str` field. No UUID type enforcement at the model level — the route/store layer is responsible for generating valid UUIDs.
**Lessons**: None new.

## 2026-03-12: RunStatusResponse model
**Summary**: Created `RunStatusResponse` pydantic model in `src/translator/src/server/models.py`.
**Change Type**: feat
**Context / Motivation**: Step 5 of translator service implementation plan. Response model for `GET /translator/runs/{run_id}` returning run status with phase and optional error.
**Decisions Made**: Frozen (immutable DTO), `error: str | None = None` defaults to None for non-failed runs. Phase is plain `str` rather than an enum — the spec lists valid phases but enforcement belongs in the store/executor layer, not the response DTO.
**Lessons**: None new.

---

## 2026-03-12: Translator DSL parser models + stub

**Summary**: Created `ParsedCommand` models (`CollectCommand`, `AnalyzeCommand`, `AggregateCommand`, `ParsedPipeline`) and stub `parse_dsl()` in `src/translator/src/services/dsl_parser.py`.
**Change Type**: feat
**Context / Motivation**: Step 6 of translator service implementation plan. These models define the contract between the DSL parser and the executor — every downstream component depends on them.
**Decisions Made**: Models aligned to downstream API contracts (data collector, scheduler, aggregator). All frozen (immutable DTOs). `parse_dsl()` validates empty input then raises `NotImplementedError` since grammar is TBD. Kept types simple (e.g., `year: int` not range types) — the executor will handle conversion to downstream API formats.
**Lessons**: None new.

## 2026-03-12: Translator RunStore persistence layer
**Summary**: Created `RunStore` class in `src/translator/src/services/run_store.py` with `init_schema`, `create_run`, `update_phase`, `get_run` methods.
**Change Type**: feat
**Context / Motivation**: Step 7 of translator service implementation plan. RunStore is the persistence layer that the executor, routes, and tests all depend on — it's the critical path for all subsequent steps.
**Decisions Made**: Class-based design (not module-level functions) for dependency injection into executor/routes. Follows scheduler's psycopg2 context manager pattern. Each method opens/closes its own connection for simplicity. Returns `dict | None` from `get_run` rather than a Pydantic model to keep it lightweight — the route layer already has `RunStatusResponse` for serialization.
**Lessons**: None new.

## 2026-03-12
**Summary**: Created `DataCollectorClient` in `src/translator/src/services/data_collector_client.py` with `collect` method and response models.
**Change Type**: feat
**Context / Motivation**: Step 8 of translator service implementation plan. HTTP client that translates `CollectCommand` from the DSL parser into a POST request to the data collector's `/collector/collect` endpoint.
**Decisions Made**: `base_url` injected via constructor (not per-call) for consistency with how Settings provides it. Local response models (`FileSuccess`, `FileFailure`, `CollectResult`) mirror the data collector's models — no cross-service imports. 300s timeout since data collection involves downloading and uploading parquet files. No retry logic per spec ("no retry" on downstream failure).
**Lessons**: None new.

## 2026-03-12: Add SchedulerClient for translator service
**Summary**: Created `SchedulerClient` in `src/translator/src/services/scheduler_client.py` with `schedule` method and response models.
**Change Type**: feat
**Context / Motivation**: Step 9 of translator service implementation plan. HTTP client that translates `AnalyzeCommand` from the DSL parser into a POST request to the scheduler's `/scheduler/schedule` endpoint with `skip_checkpoints` support.
**Decisions Made**: Follows `DataCollectorClient` pattern exactly. 600s timeout (doubled from data collector) since scheduling triggers the full analytical pipeline which can take minutes per file. Local `ScheduleResult`/`FileStatus` models mirror scheduler's response — no cross-service imports.
**Lessons**: None new.

## 2026-03-12: AggregatorClient + tests
**Summary**: Created `AggregatorClient` in `src/translator/src/services/aggregator_client.py` with `aggregate` method and `EmptyAggregationError`.
**Change Type**: feat
**Context / Motivation**: Step 10 of translator service implementation plan. HTTP client that translates `AggregateCommand` from the DSL parser into a GET request to the aggregator's `/aggregations/<endpoint>` with optional query params.
**Decisions Made**: Returns raw `dict` instead of typed response model since each aggregator endpoint has a different response shape — the executor will use the dict as-is. `EmptyAggregationError` raised when `file_count == 0` per spec's 412 requirement. 120s timeout (aggregator does cross-service calls to API server). Tests use `httpx.MockTransport` with monkeypatched `__init__` to inject transport without conflicting with production kwargs.
**Lessons**: Monkeypatching `httpx.Client` via a lambda that forwards `**kwargs` and also sets `transport` causes "multiple values for keyword argument" — must patch `__init__` directly and override `transport` in kwargs instead.

## 2026-03-12T22:51 — Step 11: Executor
**Summary**: Created `Executor` class in `src/translator/src/services/executor.py` that orchestrates collect → analyze → aggregate pipeline with phase tracking.
**Change Type**: feat
**Context / Motivation**: Step 11 of translator service implementation plan. Core orchestration logic that ties together all three downstream clients and the RunStore. Routes (Steps 12-13) depend on this.
**Decisions Made**: Used dependency injection for all three clients (DataCollectorClient, SchedulerClient, AggregatorClient) via `__init__` rather than accepting Settings and constructing clients internally — makes testing trivial with MagicMock and follows SOLID/DI principles. Removed `settings` from `execute()` signature since clients are pre-built. EmptyAggregationError caught separately for clarity but both exception paths produce the same "failed" phase. 10 tests covering full pipeline, partial sections (collect-only, analyze-only, aggregate-only, empty), and failure-stops-execution scenarios.
**Lessons**: None new — straightforward DI pattern.

## 2026-03-12: Add POST /translator/translate route
- **Change Type**: feat
- **Summary**: Created the `POST /translator/translate` endpoint in `src/translator/src/server/routes.py`. Parses DSL (400 on failure), creates a run record, spawns a daemon background thread for pipeline execution, and returns 202 with `run_id`.
- **Context / Motivation**: Step 12 of the translator service implementation plan. This is the core entry point that ties together the DSL parser, run store, and executor.
- **Decisions Made**: Used `request.app.state` pattern (consistent with scheduler and other services) for dependency access. Daemon thread so it doesn't block server shutdown. Catches both `ValueError` and `NotImplementedError` from `parse_dsl` as 400s.
- **Lessons**: None new — followed existing patterns from scheduler routes.

## 2026-03-12: Add GET /translator/runs/{run_id} endpoint
- **Change Type**: `feat`
- **Summary**: Added the GET endpoint for polling run status, completing the translator API surface.
- **Context / Motivation**: Step 13 of the translator service implementation plan. The POST /translate endpoint was already done; this completes the contract so operators can poll for run status.
- **Decisions Made**: Reused existing `_get_run_store()` helper and `RunStatusResponse` model. Kept it minimal — just a lookup + 404 guard.
- **Lessons**: None new — straightforward wiring of existing components.

## 2026-03-12T22:58 — Step 14: Create translator FastAPI main.py
- **Change Type**: `feat`
- **Summary**: Created `src/translator/src/server/main.py` with async lifespan wiring RunStore, downstream clients, and Executor into app.state.
- **Context / Motivation**: Step 14 of the translator service implementation plan. This is the critical wiring that makes the service runnable — unblocks Dockerfile, docker-compose, and route-level tests.
- **Decisions Made**: Used `@asynccontextmanager` (per lesson about sync lifespan crashing uvicorn). Followed aggregator/scheduler pattern for lifespan structure. RunStore schema init happens at startup. All clients constructed from SETTINGS URLs.
- **Lessons**: None new — applied existing lesson about async lifespan requirement.

## 2026-03-12: Add translator service uvicorn entrypoint
- **Change Type**: `feat`
- **Summary**: Created `src/translator/src/main.py` — thin uvicorn entrypoint matching the pattern used by all other services.
- **Context / Motivation**: Step 15 of the translator implementation plan. First blocker in the dependency chain for Dockerfile (Step 16) and docker-compose (Step 17).
- **Decisions Made**: Copied the exact pattern from `src/aggregator/src/main.py` — imports `app` from `src.server.main` and `SETTINGS` from `src.services.config`, runs uvicorn in `__main__` block.
- **Lessons**: None new — straightforward pattern replication.

## 2026-03-12T23:01 — Translator Dockerfile
- **Change Type**: `feat`
- **Summary**: Created `src/translator/Dockerfile` — Python 3.12-slim with uv, matching the existing service Dockerfile pattern.
- **Context / Motivation**: Step 16 of the translator implementation plan. Blocker for docker-compose (Step 17) and all test steps (18-24) which require container execution.
- **Decisions Made**: Copied exact pattern from `src/aggregator/Dockerfile`. Port 8005 matches `Settings.SERVER_PORT` default. Verified build succeeds.
- **Lessons**: None new — straightforward pattern replication.

## 2026-03-12: Translator docker-compose.yml
- **Change Type**: `feat`
- **Summary**: Created `src/infrastructure/translator/docker-compose.yml` with translator service + Postgres.
- **Context / Motivation**: Step 17 of translator implementation plan. Blocker for all test steps (18-24) since all execution happens via docker compose.
- **Decisions Made**: Used host port 5435 for Postgres to avoid conflict with existing `cbd_db` container on 5434. Matched patterns from aggregator/scheduler compose files. External services (data_collector, scheduler, aggregator) not included — only needed in end-to-end compose.
- **Lessons**: None new — straightforward infrastructure task.

## 2026-03-12: Step 18 — RunStore tests
- **Date**: 2026-03-12T23:04
- **Change Type**: `test`
- **Summary**: Added `tests/test_run_store.py` with 7 tests covering create_run, update_phase, and get_run (including not-found case).
- **Context / Motivation**: Step 18 of translator implementation plan. RunStore is the foundational data layer — testing it first ensures correctness before testing higher-level components (Executor, routes).
- **Decisions Made**: Used real Postgres from docker-compose (integration tests). Fixture initializes schema and cleans table between tests. Matched existing test style (class-based, descriptive method names).
- **Lessons**: None new — straightforward integration test against compose Postgres.

---

## 2026-03-12: DataCollectorClient tests
- **Date**: 2026-03-12T23:06
- **Change Type**: `test`
- **Summary**: Added `tests/test_data_collector_client.py` with 5 tests covering success, empty response, HTTP error propagation, request body correctness, and URL correctness.
- **Context / Motivation**: Step 19 of translator implementation plan. Client tests are prerequisites for Executor test confidence (Step 22).
- **Decisions Made**: Matched existing test pattern from `test_aggregator_client.py` — monkeypatched `httpx.Client.__init__` with `MockTransport`. Tested both response parsing and request construction.
- **Lessons**: None new — followed established pattern.

## 2026-03-12: SchedulerClient tests
- **Date**: 2026-03-12T23:08
- **Change Type**: `test`
- **Summary**: Added `tests/test_scheduler_client.py` with 6 tests covering success with files, empty files, HTTP error propagation, request body correctness, URL correctness, and skip_checkpoints default.
- **Context / Motivation**: Step 20 of translator implementation plan. Completes the client test coverage needed before Executor tests (Step 22).
- **Decisions Made**: Followed established test pattern from `test_data_collector_client.py` and `test_aggregator_client.py`. Added a 6th test (`test_skip_checkpoints_defaults_to_empty`) to verify the default empty list behavior since skip_checkpoints is a key feature of the scheduler integration.
- **Lessons**: None new — followed established pattern.

## 2026-03-12: Verify and mark executor tests as complete
- **Change Type**: `test`
- **Context / Motivation**: Step 22 of translator implementation plan — executor tests existed but step was unchecked. Verified all 10 tests pass (full pipeline, partial sections, failure stops execution), ruff clean.
- **Decisions Made**: No code changes needed; tests were already written and committed. Only the plan needed updating.
- **Lessons**: None — straightforward verification step.

## 2026-03-12: Write tests for POST /translator/translate route
- **Change Type**: `test`
- **Context / Motivation**: Step 23 of translator implementation plan — route-level tests for the translate endpoint covering success (202), parse errors (400), and pydantic validation (422).
- **Decisions Made**: Used `unittest.mock.patch` on `parse_dsl` to control parser behavior without needing a real DSL grammar. Set `app.state.run_store` and `app.state.executor` as MagicMocks at module level (no lifespan/DB needed). 7 tests total.
- **Lessons**: None — straightforward route testing with mocks.

## 2026-03-12: Write tests for GET /translator/runs/{run_id} route
- **Change Type**: `test`
- **Context / Motivation**: Step 24 of translator implementation plan — route-level tests for the run status endpoint covering found (200 with data and error fields) and not found (404).
- **Decisions Made**: Same mock pattern as step 23 — `app.state.run_store` as MagicMock, no DB needed. 4 tests: 200 with normal data, 200 with error field, store called with correct run_id, 404 on missing run.
- **Lessons**: None — straightforward route testing with mocks.

## 2026-03-12: Translator — ruff check and format (Step 25)
- **Change Type**: `chore`
- **Summary**: Ran ruff check and ruff format on all 24 translator Python files. All passed clean with no changes needed.
- **Context / Motivation**: Code quality gate before README and end-to-end integration steps.
- **Decisions Made**: None — all files were already compliant.
- **Lessons**: None — clean pass.

## 2026-03-12: Add translator to end-to-end docker-compose
- **Change Type**: `feat`
- **Summary**: Added `postgres_translator` and `translator` services to `src/infrastructure/compose/docker-compose.yml`, wiring the translator to all downstream services on the shared network.
- **Context / Motivation**: Step 27 of the translator implementation plan. Without this, the translator cannot participate in full pipeline runs.
- **Decisions Made**: Used `service_started` for data_collector and scheduler (no healthchecks defined), `service_healthy` for aggregator and postgres_translator. Port 5435 for translator Postgres, 8005 for the translator service.
- **Lessons**: None — straightforward wiring following existing patterns in the compose file.

## 2026-03-12
- **Change Type**: `docs`
- **Summary**: Created `src/translator/README.md` with service description, endpoints, configuration, and run/test commands.
- **Context / Motivation**: Step 26 of the translator implementation plan. Final step to complete the translator service documentation per `service_readme.md` rule.
- **Decisions Made**: Kept it minimal — service purpose, endpoint table, config table, docker compose commands. No DSL grammar docs since the parser is still a placeholder.
- **Lessons**: None.

---

## 2026-03-12

- **Change Type**: `feat`
- **Summary**: Implemented `parse_dsl` in `src/translator/src/services/dsl_parser.py`, replacing the `NotImplementedError` stub with a working key=value grammar parser. Updated tests from 3 stub tests to 29 comprehensive parsing tests.
- **Context / Motivation**: Step 28 of the translator implementation plan. The DSL parser was the last non-functional piece — without it the entire translator service couldn't process any input.
- **Decisions Made**: Simple key=value grammar (`SECTION key=val key=[a,b];`) — no parser combinator library needed. Pydantic handles field validation after parsing. Case-insensitive keywords. Duplicate/unknown sections raise `ValueError`.
- **Lessons**: None.

## 2026-03-13: Translator pyproject.toml setup
- **Change Type**: feat
- **Summary**: Updated `src/translator/pyproject.toml` with correct dependencies — `psycopg[binary]>=3.2.0` (replacing `psycopg2-binary`), added `pytest-asyncio` and `testcontainers` dev deps. Verified with `uv sync`.
- **Context / Motivation**: Step 1 of translator service implementation plan. Foundation for all subsequent translator work.
- **Decisions Made**: Used `psycopg[binary]` (psycopg3) over `psycopg2-binary` — modern async-capable driver. Followed aggregator's pyproject.toml pattern.
- **Lessons**: Previous implementation existed and was reset. Checked git diff before committing to avoid staging unrelated deletions.

## 2026-03-13: Add TranslateRequest model to translator service
- **Change Type**: `feat`
- **Summary**: Created `TranslateRequest` pydantic model with `dsl: str` field (min_length=1, frozen) and corresponding tests.
- **Context / Motivation**: Step 2 of translator service implementation plan. Foundation model needed before routes and executor can be built.
- **Decisions Made**: Used `ConfigDict(frozen=True)` matching existing service style. Added `min_length=1` to reject empty DSL strings at validation boundary.
- **Lessons**: None new — followed existing patterns from aggregator/data_collector models.

## 2026-03-13T22:08 — TranslateResponse model
- **Change Type**: `feat`
- **Summary**: Created `TranslateResponse` pydantic model with `run_id: UUID` field (frozen) and corresponding tests.
- **Context / Motivation**: Step 3 of translator service implementation plan. Response model for `POST /translator/translate` (202 Accepted).
- **Decisions Made**: Frozen model since it's a read-only DTO. Tests cover construction, serialization, and immutability.
- **Lessons**: None new — straightforward model following same pattern as TranslateRequest.

## 2026-03-13: Add RunStatusResponse model
- **Date**: 2026-03-13T22:09+01:00
- **Change Type**: `feat`
- **Summary**: Created `RunStatusResponse` pydantic model with `run_id`, `phase` (Literal-constrained), and `error` fields. Added `RunPhase` type alias.
- **Context / Motivation**: Step 4 of translator service implementation plan. Response model for `GET /translator/runs/{run_id}`.
- **Decisions Made**: Used `Literal` type alias instead of bare `str` for phase — catches invalid phase values at validation time. Included `pending` phase from execution flow spec even though the GET endpoint docs only list 5 phases.
- **Lessons**: None new — straightforward model addition.

## 2026-03-13T22:11 — Step 5: DSL command dataclasses

- **Change Type**: `feat`
- **Summary**: Created frozen dataclasses (`CollectCommand`, `AnalyzeCommand`, `AggregateCommand`, `ParsedDSL`) in `src/translator/src/services/parser.py`. 13 tests in `test_parser.py`.
- **Context / Motivation**: Step 5 of translator service implementation plan. These are the internal data structures that the DSL parser will produce and the HTTP client/executor will consume. Foundation for all subsequent translator steps.
- **Decisions Made**: Used `dataclass(frozen=True, slots=True)` instead of Pydantic — these are simple internal containers without validation needs, per the data models rule. `year`/`month` typed as `int | dict[str, int]` to support both single values and ranges matching the downstream `CollectRequest` contract.
- **Lessons**: None new — straightforward dataclass creation.

## 2026-03-13: Add parse_dsl() placeholder function
- **Change Type**: `feat`
- **Summary**: Added `parse_dsl()` to `src/translator/src/services/parser.py` — JSON-based placeholder parser that returns `ParsedDSL` or raises `ValueError`.
- **Context / Motivation**: Step 6 of translator service implementation plan. The DSL grammar is marked TODO in the spec, so a JSON placeholder enables downstream steps (executor, routes) to be built against a stable interface.
- **Decisions Made**: Used JSON as interim format because it maps 1:1 to the existing dataclasses and is trivially replaceable. Kept `import json` inside the function body since it's a placeholder — will move to top-level when the real parser replaces it.
- **Lessons**: None new.

## 2026-03-13: Translator database module
- **Change Type**: `feat`
- **Summary**: Created `src/translator/src/services/db.py` with `init_db`, `create_run`, `get_run`, `update_run` functions using psycopg3.
- **Context / Motivation**: Step 7 of translator service implementation plan. Foundation for run state persistence — executor, routes, and app all depend on this module.
- **Decisions Made**: Used psycopg3 (not psycopg2 like scheduler) since that's what's in pyproject.toml. Module-level functions with connection passed as parameter, matching scheduler's pattern. `get_run` returns `dict | None` using `dict_row` factory. No separate config module — `database_url` accepted as parameter with default.
- **Lessons**: None new.

## 2026-03-13: Translator HTTP client for downstream services
- **Change Type**: `feat`
- **Summary**: Created `src/translator/src/services/http_client.py` with `call_collector`, `call_scheduler`, `call_aggregator` functions. Also created `src/translator/src/services/config.py` with pydantic-settings.
- **Context / Motivation**: Step 8 of translator service implementation plan. The executor (Step 9) needs these functions to dispatch parsed DSL commands to downstream services.
- **Decisions Made**: Created a config module with pydantic-settings (matching aggregator pattern) rather than hardcoding URLs. Default ports match the end-to-end compose (collector:8010, scheduler:8011, aggregator:8014). 300s timeout default since downstream operations (data collection, analysis) are long-running. Each function opens/closes its own httpx.Client context manager per the project HTTP client rule.
- **Lessons**: None new.

## 2026-03-13: Translator executor module
- **Date**: 2026-03-13T22:22+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/translator/src/services/executor.py` with `execute_run()` that orchestrates COLLECT → ANALYZE → AGGREGATE pipeline. 8 unit tests covering full pipeline, partial DSL, and failure scenarios.
- **Context / Motivation**: Step 9 of translator service implementation plan. The executor is the core orchestration logic — routes (Steps 10-11) and the app (Step 12) depend on it.
- **Decisions Made**: Split into `execute_run` (top-level exception handler) and `_execute_steps` (sequential logic) to keep the try/except boundary clean. Each phase update opens its own DB connection since the executor runs in a background thread. Empty aggregator response (falsy `[]` or `{}`) triggers a `412 Precondition Failed` error per spec.
- **Lessons**: None new.

## 2026-03-13: Add POST /translator/translate route
- **Change Type**: `feat`
- **Summary**: Created `POST /translator/translate` endpoint in `src/translator/src/server/routes.py`. Parses DSL (400 on ValueError), creates DB run record, spawns background thread for execution, returns 202 with run_id.
- **Context / Motivation**: Step 10 of translator service implementation plan — first route, wiring parser + DB + executor together.
- **Decisions Made**: Used `threading.Thread(daemon=True)` for background execution (matches executor pattern from Step 9). Route manages its own DB connection via `get_connection` context manager rather than FastAPI `Depends()` since the connection is only needed for `create_run`, not the full request lifecycle.
- **Lessons**: None new — straightforward wiring step.

## 2026-03-13: Add GET /translator/runs/{run_id} route
- **Change Type**: `feat`
- **Summary**: Created `GET /translator/runs/{run_id}` endpoint in `src/translator/src/server/routes.py`. Fetches run by UUID, returns `RunStatusResponse` (200) or 404 if not found.
- **Context / Motivation**: Step 11 of translator service implementation plan — completes the API surface so callers can poll for async run status.
- **Decisions Made**: Same DB connection pattern as POST route (`get_connection` context manager, not `Depends()`). Maps raw dict from `get_run()` to `RunStatusResponse` model explicitly for type safety.
- **Lessons**: None new — straightforward endpoint addition.

## 2026-03-13: Translator FastAPI main.py entrypoint
- **Date**: 2026-03-13T22:29+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/translator/src/server/main.py` — FastAPI app entrypoint with async lifespan, DB init on startup, health endpoint, and router wiring.
- **Context / Motivation**: Step 12 of translator service implementation plan. This is the integration point that connects all previously built components (routes, DB, config) into a runnable service.
- **Decisions Made**: Used `main.py` (not `app.py`) per `fastapi_main_entrypoint.md` rule. Added `health_router` to `routes.py` following the aggregator service pattern. Used `@asynccontextmanager` for lifespan (lesson from 2026-03-09 about sync lifespan crashing uvicorn).
- **Lessons**: None new — straightforward wiring step.

## 2026-03-13

- **Date**: 2026-03-13T22:31+01:00
- **Change Type**: `feat`
- **Summary**: Created `src/translator/Dockerfile` — Docker image definition for the translator service.
- **Context / Motivation**: Step 13 of translator service implementation plan. Dockerfile is the critical path blocker for docker-compose (Step 14) and all test execution (Steps 15-20).
- **Decisions Made**: Followed aggregator Dockerfile pattern exactly. Port 8015 matches `config.py` SERVER_PORT default.
- **Lessons**: None new — straightforward pattern replication.

## 2026-03-13: Translator docker-compose.yml
- **Change Type**: `feat`
- **Summary**: Created `src/infrastructure/translator/docker-compose.yml` with translator service + Postgres 16-alpine.
- **Context / Motivation**: Step 14 of translator implementation plan. Blocker for all test steps (15-20) since docker-execution rule requires all code execution via docker compose.
- **Decisions Made**: Used host port 5436 to avoid conflicts with existing services (scheduler:5432, api_server:5433, compose:5434/5435). Followed aggregator compose pattern.
- **Lessons**: None new — straightforward infrastructure task.

## 2026-03-13: Add RunStatusResponse tests to test_models.py
- **Change Type**: `test`
- **Summary**: Completed Step 15 — added `TestRunStatusResponse` class with 10 new tests (16 total in file).
- **Context / Motivation**: Model tests are the foundation for the translator service test suite. The file already had tests for `TranslateRequest` and `TranslateResponse` but was missing `RunStatusResponse` coverage.
- **Decisions Made**: Used `pytest.mark.parametrize` for all 6 phase values to avoid repetitive test methods. Tested frozen immutability, error field, invalid phase rejection, and serialization.
- **Lessons**: None new — straightforward test addition.

## 2026-03-13T22:37+01:00
- **Change Type**: `test`
- **Summary**: Completed Step 16 — added 25 parser tests in `src/translator/tests/test_parser.py`.
- **Context / Motivation**: Parser is the core component of the translator service; all downstream execution depends on correct parsing. Tests validate the JSON-based placeholder parser against all valid/invalid input patterns.
- **Decisions Made**: Organized into three test classes (valid, invalid, dataclass properties). Covered edge cases: year/month ranges, whitespace trimming, missing required fields per section, non-object JSON types.
- **Lessons**: None new — straightforward test addition.

## 2026-03-13

**Summary**: Scaffolded translator service with pyproject.toml and package structure.
**Change Type**: feat
**Context / Motivation**: First step of the translator service implementation plan — project foundation needed before any code can be written.
**Decisions Made**: Matched aggregator's pyproject.toml pattern (hatchling build backend, src/ layout). Included psycopg[binary] for direct Postgres access (translator owns its own `translator_runs` table). Created empty `__init__.py` files for all packages to make the structure immediately usable.
**Lessons**: None new — applied existing lesson about hatchling + src/ layout from 2026-02-28.

## 2026-03-13: TranslateRequest model
- **Change Type**: `feat`
- **Summary**: Created `TranslateRequest` Pydantic model in `src/translator/src/server/models.py` with `dsl: str` field, `min_length=1` validation, and frozen config.
- **Context / Motivation**: Step 2 of translator service implementation plan. Foundational model needed by routes and executor.
- **Decisions Made**: Used `ConfigDict(frozen=True)` since request models are read-only DTOs. Added `min_length=1` to reject empty DSL at validation boundary.
- **Lessons**: None new.

## 2026-03-13: Add TranslateResponse model
- **Change Type**: `feat`
- **Summary**: Added `TranslateResponse` Pydantic model with `run_id: UUID` field to `src/translator/src/server/models.py`.
- **Context / Motivation**: Step 3 of translator service implementation plan. Required for the `POST /translator/translate` endpoint (Step 10).
- **Decisions Made**: Frozen ConfigDict for immutability, matching `TranslateRequest` pattern. UUID type accepts both native UUID and string coercion.
- **Lessons**: None — straightforward model addition.

## 2026-03-13: Add RunStatusResponse model to translator service
- **Change Type**: `feat`
- **Summary**: Added `RunStatusResponse` Pydantic model with `RunPhase` Literal type alias to `src/translator/src/server/models.py`.
- **Context / Motivation**: Step 4 of translator service implementation plan — model needed for `GET /translator/runs/{run_id}` endpoint.
- **Decisions Made**: Used `Literal` type alias instead of `Enum` for `RunPhase` — simpler, no serialization overhead, compile-time safety via type checkers. Frozen `ConfigDict` for immutability matching existing models.
- **Lessons**: None new — straightforward model addition.

## 2026-03-13: Translator DSL command dataclasses (Step 5)
- **Change Type**: `feat`
- **Summary**: Created `CollectCommand`, `AnalyzeCommand`, `AggregateCommand`, and `ParsedDSL` Pydantic models in `src/translator/src/services/parser.py`.
- **Context / Motivation**: Step 5 of translator service implementation plan — these are the structured command types that the DSL parser will produce and the executor/HTTP client will consume.
- **Decisions Made**: Used `int | dict[str, int]` for year/month to support both single values and `{from, to}` ranges matching data collector's `YearField`/`MonthField`. All models frozen since they're value objects. `skip_checkpoints` and `params` default to empty collections via `Field(default_factory=...)`.
- **Lessons**: None new — straightforward model creation following existing patterns.

## 2026-03-13: Add parse_dsl() JSON-based placeholder parser
- **Change Type**: `feat`
- **Summary**: Implemented `parse_dsl()` in `src/translator/src/services/parser.py` — JSON-based placeholder that converts DSL strings into `ParsedDSL` with optional collect/analyze/aggregate sections.
- **Context / Motivation**: Step 6 of translator service implementation plan. The parser is the core dependency for the executor (Step 9) and routes (Steps 10-11). Grammar is marked TODO in spec, so JSON placeholder is appropriate.
- **Decisions Made**: Used JSON as the placeholder format since it maps directly to the Pydantic models. Validation delegates to Pydantic for field-level checks; `parse_dsl` only handles JSON parsing and "at least one section" validation. Extra keys in the JSON are silently ignored (forward-compatible).
- **Lessons**: None new — straightforward implementation.

## 2026-03-13: Translator database module (Step 7)
- **Change Type**: `feat`
- **Summary**: Created `db.py` with `init_db`, `create_run`, `get_run`, `update_run` using psycopg3. Created minimal `config.py` with `DATABASE_URL` setting.
- **Context / Motivation**: Step 7 of translator service implementation plan — database module is the critical-path blocker for executor, routes, and main app.
- **Decisions Made**: Used psycopg3 (not psycopg2) since `psycopg[binary]` was already in deps. All functions accept `conn` as first arg (caller manages connection lifecycle). Created minimal config.py with only DB-related settings — Step 8 will extend with service URLs. Used `dict_row` for `get_run` to return dicts directly.
- **Lessons**: Previous versions of config.py and db.py already existed (created during incomplete Step 8 attempt). Subagent reported config.py as missing — always verify with `git status` before assuming files don't exist. See [lessons.md](lessons.md).

## 2026-03-13: Translator HTTP client for downstream services
- **Change Type**: `feat`
- **Summary**: Created `http_client.py` with `call_collector`, `call_scheduler`, `call_aggregator` functions and updated `config.py` with service URLs/timeout.
- **Context / Motivation**: Step 8 of translator implementation plan — HTTP client is the dependency for the executor (Step 9) which orchestrates the full pipeline.
- **Decisions Made**: Single module with three functions (not three separate client files). Used `httpx.Client` context manager with `verify=False` per project convention. Return raw `dict` from response JSON — let the executor decide what to do with it. 300s timeout default matches analyzer lesson.
- **Lessons**: Applied lesson 2026-03-12 (monkeypatch httpx.Client.__init__, pop verify, inject transport) for clean test mocking.

## 2026-03-13: Translator executor — background pipeline orchestration
- **Change Type**: `feat`
- **Summary**: Created `executor.py` with `execute_run()` that sequences COLLECT → ANALYZE → AGGREGATE downstream calls, updating run phase in Postgres at each step.
- **Context / Motivation**: Step 9 of translator service implementation plan. The executor is the core orchestration logic that routes, app entrypoint, and all downstream steps depend on.
- **Decisions Made**: Single function design (no class) — the executor has no state beyond what's in Postgres. Error handling opens a fresh DB connection in the except block to avoid using a potentially broken connection from the try block. Empty aggregator response (dict or list) treated as 412 per spec.
- **Lessons**: None new — existing patterns from http_client and db modules applied cleanly.

---

### 2026-03-13T23:15 — Step 10: POST /translator/translate route
- **Change Type**: `feat`
- **Summary**: Created `routes.py` with `POST /translator/translate` endpoint and `GET /health` health check, following the aggregator router pattern.
- **Context / Motivation**: Step 10 of translator service implementation plan. This is the entry point that ties together parser, DB, and executor — all downstream steps (GET route, main.py, route tests) depend on it.
- **Decisions Made**: Used daemon thread for background execution (matches executor design). Parse errors raise `HTTPException(400)` synchronously before run creation, so invalid DSL never creates a DB record. Health endpoint on separate `health_router` (no prefix) matching aggregator pattern.
- **Lessons**: None new — straightforward wiring of existing modules.

## 2026-03-13: Translator GET /runs/{run_id} endpoint
- **Date**: 2026-03-13T23:18+01:00
- **Change Type**: `feat`
- **Summary**: Added `GET /translator/runs/{run_id}` endpoint to `routes.py` — fetches run status from Postgres, returns `RunStatusResponse` or 404.
- **Context / Motivation**: Step 11 of translator service implementation plan. Without this endpoint, clients cannot poll for async run status after calling `POST /translate`. Unblocks Step 12 (app wiring).
- **Decisions Made**: Surgical addition — only added the new route function plus required imports (`UUID`, `RunStatusResponse`, `get_run`). No changes to existing code. Matched existing style (sync handler, `get_connection()` context manager, explicit keyword args).
- **Lessons**: None new — straightforward endpoint wiring.

## 2026-03-13: Translator FastAPI app entrypoint (Step 12)
- **Change Type**: fix
- **Summary**: Fixed and finalized `src/translator/src/server/main.py` — async lifespan with DB init, route wiring, bug fix for non-existent `SETTINGS.LOG_LEVEL`.
- **Context / Motivation**: Step 12 of translator implementation plan. The file existed from a prior session but had a bug referencing `SETTINGS.LOG_LEVEL` which doesn't exist in `config.py`.
- **Decisions Made**: Used `logging.INFO` directly instead of adding a `LOG_LEVEL` setting to config (prompt scope only — config changes weren't requested). Removed redundant `database_url=` kwarg since `get_connection()` defaults to `SETTINGS.DATABASE_URL`.
- **Lessons**: Subagent reported file as missing when it existed (reinforces 2026-03-13 lesson). Always verify with `fs_read` or `git status` before using `fs_write create`.

## 2026-03-13: Translator Dockerfile + docker-compose verification and fix
- **Change Type**: fix
- **Summary**: Verified existing Dockerfile builds, restored deleted docker-compose.yml with corrected env var names (HTTP_TIMEOUT instead of REQUEST_TIMEOUT, removed unused LOG_LEVEL).
- **Context / Motivation**: Steps 13-14 of translator implementation plan were already implemented in previous sessions but not marked complete. The docker-compose.yml was deleted from disk and had env var mismatches with the Settings class.
- **Decisions Made**: Fixed env var name to match `Settings.HTTP_TIMEOUT` rather than adding a new alias. Removed `LOG_LEVEL` since it's not in Settings (no-op env var is misleading).
- **Lessons**: Previous sessions committed files that were later deleted from the working tree without committing the deletion — always check `git status` before assuming a file needs creation.

## 2026-03-13: Translator model tests (Step 15)
- **Change Type**: test
- **Summary**: Added 21 unit tests for translator request/response models (TranslateRequest, TranslateResponse, RunStatusResponse).
- **Context / Motivation**: Step 15 of translator implementation plan — model tests are foundational and the first uncompleted test step.
- **Decisions Made**: Used parametrize for all 6 RunPhase values. Tested frozen enforcement, UUID coercion, serialization, and validation boundaries. 21 new tests, 61 total passing.
- **Lessons**: None new — straightforward Pydantic model testing.

## 2026-03-13: Translator DB integration tests
- **Change Type**: `test`
- **Summary**: Added 7 integration tests for translator DB operations (create_run, get_run, update_run) plus shared conftest fixture.
- **Context / Motivation**: Step 17 of translator implementation plan — DB module had zero test coverage despite being on the critical path for all features.
- **Decisions Made**: Used docker-compose Postgres instead of TestContainers (per lesson 2026-03-01). Pre/post-test cleanup via DELETE to handle persistent DB between runs.
- **Lessons**: None new — applied existing lessons correctly.

## 2026-03-13: Add POST /translator/translate route tests
- **Change Type**: `test`
- **Summary**: Created `src/translator/tests/test_routes.py` with 6 tests for the POST /translator/translate endpoint.
- **Context / Motivation**: Step 19 of translator service implementation plan — route-level tests validate the HTTP contract (status codes, error messages, background thread spawning).
- **Decisions Made**: Module-level patching of lifespan DB calls to avoid needing Postgres for route tests. Fixture-based patching for route-level mocks (get_connection, create_run, Thread). Tested both parse errors (400) and pydantic validation errors (422) separately.
- **Lessons**: None new — existing patterns from test_executor.py applied cleanly.

## 2026-03-13: Expand translator parser tests to 25
- **Change Type**: `test`
- **Summary**: Added 3 tests to `test_parser.py`: whitespace-around-dsl valid input, collect year wrong type invalid input, AnalyzeCommand frozen immutability.
- **Context / Motivation**: Step 16 of translator implementation plan required 25 tests; existing file had 22.
- **Decisions Made**: Added minimal tests covering the 3 missing categories (whitespace, wrong type, frozen for AnalyzeCommand) rather than restructuring existing tests.
- **Lessons**: None new — straightforward test expansion.

## 2026-03-13: Translator Step 18 — Executor tests enhanced
- **Date**: 2026-03-13T23:37+01:00
- **Change Type**: `test`
- **Summary**: Enhanced executor tests with phase transition assertions and added 3 new tests (11 total, 80 translator tests).
- **Context / Motivation**: Step 18 of translator plan required verifying phase transitions (collecting→analyzing→aggregating→completed), not just that HTTP clients were called.
- **Decisions Made**: Enhanced existing 4 tests to patch `update_run` and assert phase sequences. Added tests for: correct command passing, collect failure skipping later steps, empty aggregator never reaching completed. Kept existing failure/412 tests unchanged (already had proper assertions).
- **Lessons**: None new — straightforward test enhancement.

## 2026-03-13: Step 20 — GET /translator/runs/{run_id} route tests

- **Date**: 2026-03-13T23:40+01:00
- **Change Type**: `test`
- **Summary**: Added 2 tests for the GET run status route (existing run → 200, unknown run_id → 404). 82 translator tests total.
- **Context / Motivation**: Step 20 of translator plan — complete route test coverage before final cleanup steps.
- **Decisions Made**: Used per-test mock setup (same pattern as `test_spawns_background_thread`) rather than a shared fixture, since GET route needs different mocks than POST route.
- **Lessons**: None new — straightforward addition.
