# Aggregator Implementation Plan

## Project Setup
- [x] Step 1: Create `src/aggregator/` directory structure with `src/server/`, `src/services/`, `tests/` — **~5% context used**
  - Verify: directories exist
- [x] Step 2: Create `src/aggregator/pyproject.toml` with dependencies (fastapi, pydantic, pydantic-settings, httpx, uvicorn) — **~8% context used**
  - Reference: `src/analyzer/pyproject.toml` for structure
  - Verify: `uv sync` succeeds

## Configuration
- [x] Step 3: Create `src/aggregator/src/services/config.py` with `Settings` class (`API_SERVER_URL`, `SERVER_HOST`, `SERVER_PORT`, `LOG_LEVEL`, `REQUEST_TIMEOUT`) — **~10% context used**
  - Reference: [aggregator.md](aggregator.md) § Configuration
  - Verify: ruff check passes

## Pydantic Response Models
- [x] Step 4: Create `src/aggregator/src/server/models.py` — `FiltersApplied` model and `DescriptiveStatsResponse` model — **~12% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/descriptive-stats` response
  - Verify: ruff check passes
- [x] Step 5: Add `TaxiComparisonResponse` and `TaxiMetrics` models to `models.py` — **~14% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/taxi-comparison` response
  - Verify: ruff check passes
- [x] Step 6: Add `TemporalPatternsResponse` model to `models.py` — **~15% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/temporal-patterns` response
  - Verify: ruff check passes
- [x] Step 7: Add `DataQualityResponse`, `OutlierMethodSummary` models to `models.py` — **~17% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/data-quality` response
  - Verify: ruff check passes
- [x] Step 8: Add `PipelinePerformanceResponse`, `StepPerformance`, `PipelineSavings` models to `models.py` — **~19% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/pipeline-performance` response
  - Verify: ruff check passes

## API Server Client
- [x] Step 9: Create `src/aggregator/src/services/api_client.py` — HTTP client wrapper for the API Server. Methods: `fetch_analytical_results(result_type, taxi_type, year, month)` with auto-pagination, `fetch_pipeline_summary()` — **~25% context used**
  - Reference: [api_server.md](api_server.md) § `GET /analytical-results` and `GET /metrics/pipeline-summary`
  - Reference: `src/api_server/src/server/models.py` § `AnalyticalResultResponse`, `AnalyticalResultListResponse` for response shapes
  - Verify: ruff check passes

## Aggregation Services
- [x] Step 10: Create `src/aggregator/src/services/descriptive_stats.py` — `aggregate_descriptive_stats(results, filters)` function. Takes list of analytical result dicts, returns aggregated stats — **~15% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/descriptive-stats` internal flow
  - Reference: [analyzer.md](analyzer.md) § Descriptive Statistics output summary (percentiles, distribution stats, row/column counts)
  - Verify: ruff check passes
- [x] Step 11: Create `src/aggregator/src/services/taxi_comparison.py` — `aggregate_taxi_comparison(results_by_type, filters)` function. Takes dict of taxi_type → list of results, returns comparison — **~15% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/taxi-comparison` internal flow
  - Reference: [analyzer.md](analyzer.md) § Taxi-Type Variations table (FHV has no fare data)
  - Verify: ruff check passes
- [x] Step 12: Create `src/aggregator/src/services/temporal_patterns.py` — `aggregate_temporal_patterns(results, filters)` function — **~15% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/temporal-patterns` internal flow
  - Reference: [analyzer.md](analyzer.md) § Temporal Analysis output summary (peak hours, hourly volumes)
  - Note: hourly_avg_trips and daily_avg_trips returned as empty dicts — analyzer summary_data only stores peak_hours, not per-hour/day volumes
  - Verify: ruff check passes
- [x] Step 13: Create `src/aggregator/src/services/data_quality.py` — `aggregate_data_quality(results, filters)` function — **~15% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/data-quality` internal flow
  - Reference: [analyzer.md](analyzer.md) § Data Cleaning output summary (outlier counts per method, removal stats, quality rule violations)
  - Verify: ruff check passes
- [x] Step 14: Create `src/aggregator/src/services/pipeline_performance.py` — `aggregate_pipeline_performance(results, pipeline_summary, filters)` function — **~15% context used**
  - Reference: [aggregator.md](aggregator.md) § `GET /aggregations/pipeline-performance` internal flow
  - Verify: ruff check passes

## API Routes
- [x] Step 15: Create `src/aggregator/src/server/routes.py` — `GET /health` endpoint — **~5% context used**
  - Verify: ruff check passes
- [x] Step 16: Add `GET /aggregations/descriptive-stats` route to `routes.py` — **~15% context used**
  - Wires: `api_client.fetch_analytical_results` → `descriptive_stats.aggregate_descriptive_stats` → `DescriptiveStatsResponse`
  - Verify: ruff check passes
- [x] Step 17: Add `GET /aggregations/taxi-comparison` route to `routes.py` — **~15% context used**
  - Wires: `api_client.fetch_analytical_results` (×4 taxi types) → `taxi_comparison.aggregate_taxi_comparison` → `TaxiComparisonResponse`
  - Verify: ruff check passes
- [x] Step 18: Add `GET /aggregations/temporal-patterns` route to `routes.py` — **~15% context used**
  - Wires: `api_client.fetch_analytical_results` → `temporal_patterns.aggregate_temporal_patterns` → `TemporalPatternsResponse`
  - Verify: ruff check passes
- [x] Step 19: Add `GET /aggregations/data-quality` route to `routes.py` — **~15% context used**
  - Wires: `api_client.fetch_analytical_results` → `data_quality.aggregate_data_quality` → `DataQualityResponse`
  - Verify: ruff check passes
- [x] Step 20: Add `GET /aggregations/pipeline-performance` route to `routes.py` — **~15% context used**
  - Wires: `api_client.fetch_analytical_results` + `api_client.fetch_pipeline_summary` → `pipeline_performance.aggregate_pipeline_performance` → `PipelinePerformanceResponse`
  - Verify: ruff check passes

## FastAPI Application
- [x] Step 21: Create `src/aggregator/src/server/main.py` — FastAPI app with router included, lifespan handler (no DB init needed) — **~5% context used**
  - Reference: `src/api_server/src/server/main.py` for structure
  - Verify: ruff check passes
- [x] Step 22: Create `src/aggregator/src/main.py` — uvicorn entrypoint — **~5% context used**
  - Verify: ruff check passes

## Docker
- [x] Step 23: Create `src/aggregator/Dockerfile` — **~10% context used**
  - Reference: `src/analyzer/Dockerfile` for structure
  - Verify: file exists
- [x] Step 24: Create `src/infrastructure/aggregator/docker-compose.yml` with aggregator service (depends on api_server + postgres) — **~10% context used**
  - Reference: `src/infrastructure/analyzer/docker-compose.yml` for structure
  - Verify: `docker compose -f src/infrastructure/aggregator/docker-compose.yml config` passes

## Tests — API Client
- [x] Step 25: Create `tests/conftest.py` with shared fixtures — **~25% context used**
  - Verify: ruff check passes
- [x] Step 26: Create `tests/test_api_client.py` — tests for `fetch_analytical_results` (pagination, filtering, empty results, API Server errors) — **~20% context used**
  - Mock: httpx responses
  - Verify: tests pass via docker compose

## Tests — Aggregation Services
- [x] Step 27: Create `tests/test_descriptive_stats.py` — tests for `aggregate_descriptive_stats` (single file, multiple files, empty input) — **~15% context used**
  - Verify: tests pass via docker compose
- [x] Step 28: Create `tests/test_taxi_comparison.py` — tests for `aggregate_taxi_comparison` (all types present, missing types, FHV null fare fields) — **~15% context used**
  - Verify: tests pass via docker compose
- [x] Step 29: Create `tests/test_temporal_patterns.py` — tests for `aggregate_temporal_patterns` (single file, multiple files, peak hour detection) — **~15% context used**
  - Verify: tests pass via docker compose
- [x] Step 30: Create `tests/test_data_quality.py` — tests for `aggregate_data_quality` (outlier summation, rate calculation, empty input) — **~15% context used**
  - Verify: tests pass via docker compose
- [x] Step 31: Create `tests/test_pipeline_performance.py` — tests for `aggregate_pipeline_performance` (per-step grouping, min/max/avg, pipeline summary inclusion) — **~15% context used**
  - Verify: tests pass via docker compose

## Tests — Routes
- [x] Step 32: Create `tests/test_routes.py` — tests for `GET /health` — **~5% context used**
  - Verify: tests pass via docker compose
- [x] Step 33: Add route tests for `GET /aggregations/descriptive-stats` (success, empty results, API Server error → 502) — **~25% context used**
  - Mock: `api_client` dependency
  - Verify: tests pass via docker compose
- [x] Step 34: Add route tests for `GET /aggregations/taxi-comparison` — **~15% context used**
  - Verify: tests pass via docker compose
- [x] Step 35: Add route tests for `GET /aggregations/temporal-patterns` — **~15% context used**
  - Verify: tests pass via docker compose
- [x] Step 36: Add route tests for `GET /aggregations/data-quality` — **~15% context used**
  - Verify: tests pass via docker compose
- [x] Step 37: Add route tests for `GET /aggregations/pipeline-performance` — **~15% context used**
  - Verify: tests pass via docker compose

## Integration Test
- [x] Step 38: Create `tests/test_integration.py` — end-to-end test: seed API Server with known analytical results, call each aggregation endpoint, verify correct aggregation — **~25% context used**
  - Requires: API Server + Postgres running via docker compose
  - Verify: tests pass via docker compose

## Documentation
- [x] Step 39: Create `src/aggregator/README.md` — **~15% context used**
  - Reference: [service_readme.md rule](../.kiro/steering/rules/service_readme.md)
  - Verify: file exists with all required sections

## Cleanup
- [x] Step 40: Update `specs/README.md` — add Spec and Plan links to the Aggregation feature entry — **~5% context used**
  - Verify: links point to correct files
  - Note: Links were already present in README.md — `[Spec](aggregator.md) | [Plan](aggregator_implementation_plan.md)` at line 164
