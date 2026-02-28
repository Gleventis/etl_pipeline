# API Server Implementation Plan

## ~~Project Setup~~ ✅ (2026-03-02) — ~15% context used
- [x] Created `src/api_server/` directory structure with `src/server/`, `src/services/`, `tests/`
- [x] Created `pyproject.toml` with dependencies

## ~~Configuration~~ ✅ (2026-03-02) — ~15% context used
- [x] Created `src/services/config.py` with `Settings` class (`DATABASE_URL`, `SERVER_HOST`, `SERVER_PORT`, `LOG_LEVEL`)

## ~~Database Models~~ ✅ (2026-03-02) — ~15% context used
- [x] Created `src/services/database.py` with SQLAlchemy ORM models: `Files`, `JobExecutions`, `AnalyticalResults`
- [x] All constraints, indexes, JSONB types, FKs implemented per spec
- [x] `get_engine()`, `get_session()`, `init_schema()` utilities created

## ~~Pydantic Request/Response Models~~ ✅ (2026-03-02) — ~20% context used
- [x] Created `src/server/models.py` with all Pydantic models
- [x] Request models: `FileCreate`, `FileUpdate`, `JobExecutionCreate`, `JobExecutionBatchCreate` (with `BatchExecutionItem`), `JobExecutionUpdate`, `AnalyticalResultCreate`
- [x] Response models: `FileResponse`, `FileListResponse`, `JobExecutionResponse`, `JobExecutionListResponse`, `JobExecutionBatchResponse`, `AnalyticalResultResponse`, `AnalyticalResultListResponse`, `FileInfo`
- [x] Metrics response models: `CheckpointSavingsFileResponse`, `CheckpointSavingsAggregateResponse`, `StepFailureStatistic`, `FailureStatisticsResponse`, `PipelineSummaryResponse`

## ~~CRUD Operations - Files~~ ✅ (2026-03-02) — ~25% context used
- [x] Created `src/services/crud.py` with Files CRUD functions
- [x] `create_or_get_file` — idempotent create, returns existing if `(bucket, object_name)` exists
- [x] `get_file_by_id` — returns File or None
- [x] `list_files` — filtering by status, bucket, object_name_pattern, retry_count_min with pagination
- [x] `update_file` — partial update, None values excluded
- [x] 18 tests in `tests/test_crud.py` covering idempotency, filtering, pagination, edge cases

## ~~CRUD Operations - Job Executions~~ ✅ (2026-03-02) — ~30% context used
- [x] Implemented `create_job_execution(session, file_id, pipeline_run_id, step_name, status, retry_count)` — raises ValueError if file_id invalid
- [x] Implemented `create_job_executions_batch(session, file_id, pipeline_run_id, executions)` — atomic transaction, returns list of IDs
- [x] Implemented `get_job_execution_by_id(session, job_execution_id)` — returns JobExecution or None
- [x] Implemented `list_job_executions(session, file_id, pipeline_run_id, step_name, status, retry_count_min, limit, offset)` — with filtering
- [x] Implemented `update_job_execution(session, job_execution_id, updates)` — partial update, returns updated or None
- [x] 19 tests in `tests/test_crud.py` covering creation, batch, FK validation, filtering, pagination, updates

## ~~CRUD Operations - Analytical Results~~ ✅ (2026-03-02) — ~35% context used
- [x] Implemented `extract_metadata_from_object_name(object_name)` — regex parser for taxi_type/year/month from S3 keys
- [x] Implemented `create_analytical_result(session, job_execution_id, result_type, summary_data, computation_time_seconds, detail_s3_path)` — FK validation, returns AnalyticalResults
- [x] Implemented `get_analytical_result_by_id(session, result_id)` — returns AnalyticalResults or None
- [x] Implemented `list_analytical_results(session, result_type, file_id, taxi_type, year, month, created_at_from, created_at_to, limit, offset)` — JOIN analytical_results → job_executions → files, returns list of (AnalyticalResults, Files) tuples
- [x] 18 new tests in `tests/test_crud.py` (55 total) covering creation, FK validation, complex filtering by taxi_type/year/month, pagination, empty results

## ~~Metrics Calculations~~ ✅ (2026-03-02) — ~40% context used
- [x] Created `src/services/metrics.py` with three metrics functions
- [x] `calculate_checkpoint_savings(session, file_id=None)` — per-file and aggregate time saved by checkpointing
- [x] `calculate_failure_statistics(session)` — failure rates grouped by step_name using COUNT(DISTINCT CASE)
- [x] `calculate_pipeline_summary(session)` — comprehensive pipeline metrics reusing checkpoint savings
- [x] 12 tests in `tests/test_metrics.py` covering per-file savings, aggregate savings, empty states, failure rates, grouping, and full summary

## ~~API Routes - Files~~ ✅ (2026-03-02) — ~45% context used
- [x] Created `src/server/routes.py` with `get_db` dependency, `_file_to_response` helper, and `APIRouter`
- [x] Implemented `POST /files` endpoint — calls `create_or_get_file`, returns 201 with FileResponse
- [x] Implemented `GET /files/{file_id}` endpoint — calls `get_file_by_id`, returns 200 or 404
- [x] Implemented `GET /files` endpoint — calls `list_files` with query params (status, bucket, object_name_pattern, retry_count_min, limit, offset), returns 200 with FileListResponse
- [x] Implemented `PATCH /files/{file_id}` endpoint — calls `update_file`, returns 200 or 404
- [x] Wired router into `src/server/main.py` via `app.include_router(router)`
- [x] 21 tests in `tests/test_routes.py` covering creation, idempotency, validation, 404s, filtering, pagination, partial updates

## ~~API Routes - Job Executions~~ ✅ (2026-03-02) — ~50% context used
- [x] Implemented `POST /job-executions` — calls `create_job_execution`, returns 201 or 404
- [x] Implemented `POST /job-executions/batch` — calls `create_job_executions_batch`, returns 201 or 404
- [x] Implemented `GET /job-executions/{job_execution_id}` — calls `get_job_execution_by_id`, returns 200 or 404
- [x] Implemented `GET /job-executions` — calls `list_job_executions` with query params (file_id, pipeline_run_id, step_name, status, retry_count_min, limit, offset), returns 200
- [x] Implemented `PATCH /job-executions/{job_execution_id}` — calls `update_job_execution`, returns 200 or 404
- [x] 20 new tests in `tests/test_routes.py` (41 total) covering creation, batch, FK validation, filtering, pagination, updates, error cases

## ~~API Routes - Analytical Results~~ ✅ (2026-03-02) — ~55% context used
- [x] Implemented `POST /analytical-results` endpoint — calls `create_analytical_result`, returns 201 or 404
- [x] Implemented `GET /analytical-results/{result_id}` endpoint — calls `get_analytical_result_by_id`, returns 200 or 404
- [x] Implemented `GET /analytical-results` endpoint — calls `list_analytical_results` with complex query params (result_type, file_id, taxi_type, year, month, created_at_from, created_at_to, limit, offset), returns 200 with file_info
- [x] 15 new tests in `tests/test_routes.py` (56 total) covering creation, FK validation, filtering by result_type/file_id/taxi_type/year, pagination, error cases

## ~~API Routes - Metrics~~ ✅ (2026-03-02) — ~60% context used
- [x] Implemented `GET /metrics/checkpoint-savings` endpoint — calls `calculate_checkpoint_savings`, returns 200 or 404 (per-file)
- [x] Implemented `GET /metrics/failure-statistics` endpoint — calls `calculate_failure_statistics`, returns 200
- [x] Implemented `GET /metrics/pipeline-summary` endpoint — calls `calculate_pipeline_summary`, returns 200
- [x] 8 new tests in `tests/test_routes.py` (64 total) covering empty state, per-file 404, per-file savings, aggregate savings, failure rates, pipeline summary

## ~~FastAPI Application~~ ✅ (2026-03-02) — ~15% context used
- [x] Created `src/server/main.py` with FastAPI app, lifespan handler calling `init_schema()`
- [x] Created `src/main.py` entrypoint

## ~~Docker~~ ✅ (2026-03-02) — ~15% context used
- [x] Created `Dockerfile` in `src/api_server/`
- [x] Created `src/infrastructure/api_server/docker-compose.yml` with api_server + postgres services

## ~~Unit Tests - Database~~ ✅ (2026-03-02) — ~15% context used
- [x] Created `tests/test_config.py` — 3 tests (defaults, env override, port range)
- [x] Created `tests/test_database.py` — 12 tests (schema creation, CRUD, constraints, JSONB, FKs)

## ~~Unit Tests - CRUD~~ ✅ (2026-03-02) — ~35% context used
- [x] Created `tests/test_crud.py` — 55 tests covering all CRUD functions
- [x] Tests for `create_or_get_file` idempotency, `list_files` filtering, `create_job_executions_batch` atomicity
- [x] Tests for `list_analytical_results` complex filtering and JOIN, `extract_metadata_from_object_name`

## ~~Unit Tests - Metrics~~ ✅ (2026-03-02) — ~40% context used
- [x] Created `tests/test_metrics.py` — 12 tests covering all metrics functions
- [x] Tests for `calculate_checkpoint_savings` (per-file and aggregate), `calculate_failure_statistics`, `calculate_pipeline_summary`

## ~~Integration Tests - Routes~~ ✅ (2026-03-02) — ~60% context used
- [x] Created `tests/test_routes.py` — 64 tests covering all endpoints
- [x] Files, Job Executions, Analytical Results, Metrics endpoints tested
- [x] Error cases (404, 400, validation), pagination, filtering all covered

## ~~Integration Tests - End-to-End~~ ✅ (2026-03-02) — ~65% context used
- [x] Created `tests/test_integration.py` — 6 tests covering full workflow
- [x] Full pipeline: file → batch jobs → updates → analytical results → metrics
- [x] Failure and retry scenario with checkpoint savings verification (time_saved=75s, percent=25%)
- [x] Complex filtering: taxi_type, year, pipeline_run_id, status+bucket

## ~~Documentation~~ ✅ (2026-03-02) — ~10% context used
- [x] Created `src/api_server/README.md` with service purpose, endpoints table, configuration, run and test commands
  - Reference: `src/scheduler/README.md`
