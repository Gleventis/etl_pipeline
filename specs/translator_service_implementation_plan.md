# Translator Service — Implementation Plan

Spec: [translator_service.md](translator_service.md)

## Steps

- [x] Step 1: Create `src/translator/pyproject.toml` — **~10% context used**
  - Dependencies: fastapi, uvicorn, httpx, psycopg[binary], pydantic
  - Dev dependencies: pytest, pytest-asyncio, testcontainers, ruff
  - Python 3.12, uv-managed
  - Created package structure: `src/`, `src/server/`, `src/services/`, `tests/` with `__init__.py` files
  - Verified: `uv sync` resolves 33 packages, package imports successfully
  - Spec: [README.md § Overall requirements](README.md)

- [x] Step 2: Create `TranslateRequest` model in `src/translator/src/server/models.py` — **~8% context used**
  - Field: `dsl: str` with `min_length=1` validation
  - Frozen `ConfigDict` for immutability
  - Verified: import, instantiation, empty dsl rejection, frozen enforcement
  - Spec: [translator_service.md § POST /translator/translate](translator_service.md)

- [x] Step 3: Create `TranslateResponse` model in `src/translator/src/server/models.py` — **~5% context used**
  - Field: `run_id: UUID`
  - Frozen `ConfigDict` for immutability
  - Verified: instantiation, frozen enforcement, JSON serialization, string UUID coercion
  - Spec: [translator_service.md § POST /translator/translate](translator_service.md)

- [x] Step 4: Create `RunStatusResponse` model in `src/translator/src/server/models.py` — **~5% context used**
  - Fields: `run_id: UUID`, `phase: RunPhase (Literal)`, `error: str | None`
  - Phase values: `pending`, `collecting`, `analyzing`, `aggregating`, `completed`, `failed`
  - Used `Literal` type alias `RunPhase` instead of bare `str` for compile-time safety
  - Verified: instantiation all 6 phases, frozen enforcement, invalid phase rejection, JSON serialization, string UUID coercion
  - Spec: [translator_service.md § GET /translator/runs/{run_id}](translator_service.md)

- [x] Step 5: Create DSL command dataclasses in `src/translator/src/services/parser.py` — **~8% context used**
  - `CollectCommand`: `year: int | dict[str, int]`, `month: int | dict[str, int]`, `taxi_type: str` — maps to `CollectRequest`
  - `AnalyzeCommand`: `bucket: str`, `objects: list[str]`, `skip_checkpoints: list[str]` (default empty) — maps to `ScheduleRequest`
  - `AggregateCommand`: `endpoint: str`, `params: dict[str, str | int]` (default empty) — maps to aggregator routes
  - `ParsedDSL`: optional `collect`, `analyze`, `aggregate` — all frozen Pydantic models
  - Verified: import, instantiation (single values, ranges, defaults, partial DSL), frozen enforcement
  - Spec: [translator_service.md § DSL Grammar](translator_service.md)

- [x] Step 6: Create `parse_dsl()` placeholder function in `src/translator/src/services/parser.py` — **~12% context used**
  - Accepts `str`, returns `ParsedDSL`
  - Raises `ValueError` on parse failure (drives 400 response)
  - JSON-based placeholder until grammar is finalized by operator
  - Validates at least one section present, delegates to Pydantic for field validation
  - 22 tests in `tests/test_parser.py`: valid inputs (all sections, individual, combinations, ranges, defaults, extra keys), invalid inputs (empty, non-JSON, array, no sections, missing fields), frozen enforcement
  - Ruff clean, all tests passing
  - Spec: [translator_service.md § DSL Grammar](translator_service.md) (marked TODO)

- [x] Step 7: Create database module `src/translator/src/services/db.py` — **~15% context used**
  - `init_db(conn)`: creates `translator_runs` table if not exists (columns: `run_id UUID PK`, `dsl TEXT`, `phase VARCHAR`, `error TEXT NULL`, `created_at TIMESTAMPTZ`, `updated_at TIMESTAMPTZ`)
  - `create_run(conn, dsl: str) -> UUID`: inserts new run with phase `pending`
  - `get_run(conn, run_id: UUID) -> dict | None`: fetches run by ID using `dict_row`
  - `update_run(conn, run_id: UUID, phase: str, error: str | None)`: updates phase and error
  - `get_connection()` context manager with `SETTINGS.DATABASE_URL` default
  - Uses psycopg3 with parameterized queries, cursor context managers
  - Also created minimal `config.py` with `DATABASE_URL`, `SERVER_HOST`, `SERVER_PORT` (Step 8 will extend with service URLs)
  - Added `pydantic-settings` dependency to `pyproject.toml`
  - Verified: all 22 existing tests pass, ruff clean, imports verified
  - Spec: [translator_service.md § State Storage](translator_service.md)

- [x] Step 8: Create HTTP client `src/translator/src/services/http_client.py` — **~20% context used**
  - `call_collector(cmd: CollectCommand)`: `POST /collector/collect` with `year`, `month`, `taxi_type`
    - Downstream contract: `src/data_collector/src/server/models.py` (`CollectRequest`)
  - `call_scheduler(cmd: AnalyzeCommand)`: `POST /scheduler/schedule` with `bucket`, `objects`, `skip_checkpoints`
    - Downstream contract: `src/scheduler/src/server/models.py` (`ScheduleRequest`)
  - `call_aggregator(cmd: AggregateCommand)`: `GET /aggregations/<endpoint>` with query params
    - Downstream contract: `src/aggregator/src/server/routes.py` (5 endpoints)
  - All functions raise on non-2xx responses via `raise_for_status()`
  - Updated `config.py` with `COLLECTOR_URL` (8000), `SCHEDULER_URL` (8001), `AGGREGATOR_URL` (8003), `HTTP_TIMEOUT` (300s)
  - 10 tests in `tests/test_http_client.py`: request shape, params, year ranges, skip_checkpoints, error propagation
  - Monkeypatch approach follows lesson 2026-03-12 (pop `verify`, inject transport)
  - Ruff clean, all 32 tests passing
  - Spec: [translator_service.md § Downstream Service Contracts](translator_service.md)

- [x] Step 9: Create executor `src/translator/src/services/executor.py` — **~15% context used**
  - `execute_run(run_id: UUID, parsed: ParsedDSL)`: runs in background thread
  - Sequence: COLLECT → ANALYZE → AGGREGATE (each only if present in `ParsedDSL`)
  - Updates phase in DB before each downstream call
  - On success: phase → `completed`
  - On failure: phase → `failed`, stores error message, stops execution
  - Aggregator returning no data (empty dict or empty list) → stores `412 Precondition Failed` as error
  - 8 unit tests in `tests/test_executor.py`: full pipeline, partial DSL (collect-only, analyze-only, aggregate-only), collector failure, aggregator empty dict, aggregator empty list, mid-pipeline failure stops execution
  - Ruff clean, all 40 tests passing
  - Spec: [translator_service.md § Execution Flow](translator_service.md)
  - Spec: [translator_service.md § Error Handling](translator_service.md)

- [x] Step 10: Create `POST /translator/translate` route in `src/translator/src/server/routes.py` — **~25% context used**
  - Accepts `TranslateRequest`, calls `parse_dsl()` (400 on `ValueError` via `HTTPException`)
  - Creates run record via `create_run()` with `get_connection()` context manager
  - Spawns daemon background thread with `execute_run(run_id, parsed)`
  - Returns `202 Accepted` with `TranslateResponse(run_id=run_id)`
  - Added `health_router` with `GET /health` (matching aggregator pattern)
  - Added `if __name__ == "__main__"` block per main_block rule
  - Ruff clean, all 40 existing tests passing
  - Spec: [translator_service.md § API](translator_service.md)
  - Spec: [translator_service.md § Error Handling](translator_service.md)

- [x] Step 11: Create `GET /translator/runs/{run_id}` route in `src/translator/src/server/routes.py` — **~8% context used**
  - Fetches run via `get_run()` with `get_connection()` context manager, returns `RunStatusResponse`
  - Returns `404` with detail message if `run_id` not found
  - Added `UUID` import, `RunStatusResponse` and `get_run` imports
  - Verified: all 40 existing tests pass, ruff check + format clean
  - Spec: [translator_service.md § API](translator_service.md)

- [x] Step 12: Create FastAPI app in `src/translator/src/server/main.py` — **~15% context used**
  - Used `main.py` per `fastapi_main_entrypoint.md` rule (not `app.py`)
  - Async lifespan calls `init_db()` on startup to ensure `translator_runs` table exists
  - Fixed bug: previous version referenced non-existent `SETTINGS.LOG_LEVEL`, replaced with `logging.INFO`
  - Simplified lifespan docstring, removed redundant `database_url=` kwarg (defaults correctly)
  - Wired `health_router` (`/health`) and translator `router` (`/translator/*`)
  - Verified: all 40 existing tests pass, app imports correctly with 3 routes wired, ruff clean
  - Spec: [translator_service.md § State Storage](translator_service.md) (Postgres table must exist before serving)

- [x] Step 13: Create `src/translator/Dockerfile` — **~5% context used**
  - Python 3.12-slim base, uv for deps, port 8015
  - Follows aggregator Dockerfile pattern exactly
  - Verified: `docker build` succeeds
  - Spec: [README.md § Overall requirements](README.md)

- [x] Step 14: Create `src/infrastructure/translator/docker-compose.yml` — **~10% context used**
  - Translator service + Postgres 16-alpine
  - Host port 5436 (avoids conflicts with scheduler:5432, api_server:5433, compose:5434/5435)
  - Fixed env vars: renamed `REQUEST_TIMEOUT` → `HTTP_TIMEOUT`, removed unused `LOG_LEVEL`
  - Verified: `docker compose build` succeeds, all 40 existing tests pass via docker
  - Spec: [README.md § Overall requirements](README.md)

- [x] Step 15: Write tests for models in `src/translator/tests/test_models.py` — **~15% context used**
  - `TestTranslateRequest`: valid dsl, empty rejection, whitespace accepted, frozen (4 tests)
  - `TestTranslateResponse`: instantiation, string UUID coercion, serialization, frozen (4 tests)
  - `TestRunStatusResponse`: all 6 phases parametrized, invalid phase rejected, error default/field, serialization with/without error, frozen, UUID coercion (13 tests)
  - 21 tests total, all 61 translator tests passing, ruff clean
  - Spec: [translator_service.md § API](translator_service.md)

- [x] Step 16: Write tests for parser in `src/translator/tests/test_parser.py` — **~15% context used**
  - 25 tests: valid inputs (all sections, individual, combinations, defaults, ranges, whitespace), invalid inputs (empty, non-JSON, wrong type, no sections, missing fields), dataclass properties (frozen, defaults)
  - Added 3 new tests to existing 22: whitespace around DSL, collect year wrong type, AnalyzeCommand frozen
  - All passing, ruff clean

- [x] Step 17: Write tests for database operations in `src/translator/tests/test_db.py` — **~20% context used**
  - Uses docker-compose Postgres (not TestContainers — per lesson 2026-03-01)
  - Created `tests/conftest.py` with shared `conn` fixture (schema init + per-test cleanup)
  - 7 tests: `create_run` returns UUID, inserts row with pending phase; `get_run` returns None for missing, returns all columns; `update_run` changes phase, sets error, clears error
  - All 68 translator tests passing, ruff clean

- [x] Step 18: Write tests for executor in `src/translator/tests/test_executor.py` — **~20% context used**
  - Enhanced existing 8 tests from Step 9 with phase transition assertions
  - Added 3 new tests: correct command passing, collect failure skips later steps, empty aggregator never reaches completed
  - 11 tests total: full pipeline phases (collecting→analyzing→aggregating→completed), collect-only (collecting→completed), analyze-only (analyzing→completed), aggregate-only (aggregating→completed), collector failure sets failed, empty dict 412, empty list 412, mid-pipeline failure stops, correct command objects, collect failure skips analyze+aggregate, empty aggregator no completed
  - Ruff clean, all 80 translator tests passing

- [x] Step 19: Write tests for `POST /translator/translate` route in `src/translator/tests/test_routes.py` — **~25% context used**
  - 6 tests: valid DSL returns 202 with run_id, invalid JSON returns 400, no sections returns 400, empty DSL returns 422, missing field returns 422, background thread spawned as daemon
  - Mocks: lifespan DB calls (module-level patch), route-level get_connection/create_run/Thread
  - Ruff clean, all 74 translator tests passing

- [x] Step 20: Write tests for `GET /translator/runs/{run_id}` route in `src/translator/tests/test_routes.py` — **~10% context used**
  - 2 tests: existing run returns 200 with run_id/phase/error fields, unknown run_id returns 404 with detail
  - Mocks: `get_connection` + `get_run` patched per test (same pattern as POST tests)
  - Ruff clean, all 82 translator tests passing

- [x] Step 21: Run ruff check and format on all translator files — **~10% context used**
  - All 19 Python files pass `ruff check` with no errors
  - All 19 files already formatted (no changes needed by `ruff format`)
  - All 82 tests pass in 0.54s
  - Removed 15 orphaned old implementation files (replaced during steps 1-20)
  - Committed: `91a7b5b`

- [x] Step 22: Create translator service README at `src/translator/README.md` — **~5% context used**
  - Service description, endpoints table, configuration table, DSL example, how to run/test
  - Follows aggregator README pattern exactly
  - Committed: `48683e5`
  - Spec: [translator_service.md § API](translator_service.md)
