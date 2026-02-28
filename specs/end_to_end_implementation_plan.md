# End-to-End Compose Implementation Plan

Single unified docker-compose that wires all services on a shared network for full pipeline runs.

## Infrastructure Directory
- [x] Step 1: Create `src/infrastructure/compose/` directory — **~5% context used**
  - Verify: directory exists

## Compose File — Infrastructure Services
- [x] Step 2: Create `src/infrastructure/compose/docker-compose.yml` — define `minio` service — **~10% context used**
  - Reference: `src/infrastructure/data_collector/docker-compose.yml` § minio service definition
  - Host port: `9010` (API), `9011` (console)
  - Healthcheck: `mc ready local`
  - Verify: `docker compose -f src/infrastructure/compose/docker-compose.yml config` passes

- [x] Step 3: Add `postgres_api_server` service to compose file — **~15% context used**
  - Reference: `src/infrastructure/api_server/docker-compose.yml` § postgres service definition
  - Credentials: `api_server` / `api_server`, database: `api_server`
  - Host port: `5433`
  - Healthcheck: `pg_isready -U api_server`
  - Verify: `docker compose config` passes

- [x] Step 4: Add `postgres_scheduler` service to compose file — **~20% context used**
  - Reference: `src/infrastructure/scheduler/docker-compose.yml` § postgres service definition
  - Credentials: `scheduler` / `scheduler`, database: `scheduler`
  - Host port: `5434`
  - Healthcheck: `pg_isready -U scheduler`
  - Verify: `docker compose config` passes

- [x] Step 5: Add `prefect_server` service to compose file — **~25% context used**
  - Reference: `src/infrastructure/scheduler/docker-compose.yml` § prefect-server service definition
  - Host port: `4210`
  - Healthcheck: python urllib against `http://localhost:4200/api/health`
  - Verify: `docker compose config` passes

## Compose File — Application Services
- [x] Step 6: Add `api_server` service to compose file — **~30% context used**
  - Reference: `src/infrastructure/api_server/docker-compose.yml` § api_server service definition
  - Reference: `src/api_server/src/services/config.py` § Settings class for env var names
  - Build context: `../../api_server`
  - `DATABASE_URL` → `postgresql://api_server:api_server@postgres_api_server:5432/api_server`
  - `SERVER_PORT` → `8013`
  - Host port: `8013`
  - Depends on: `postgres_api_server` (healthy)
  - Healthcheck: python urllib against `http://localhost:8013/files?limit=1`
  - Verify: `docker compose config` passes

- [x] Step 7: Add `analyzer` service to compose file — **~35% context used**
  - Reference: `src/infrastructure/analyzer/docker-compose.yml` § analyzer service definition
  - Reference: `src/analyzer/src/services/config.py` § Settings class for env var names
  - Build context: `../../analyzer`
  - `MINIO_ENDPOINT` → `minio:9000`
  - `API_SERVER_URL` → `http://api_server:8013`
  - `SERVER_PORT` → `8012`
  - Host port: `8012`
  - Volume: `../../utilities:/utilities`
  - Depends on: `minio` (healthy), `api_server` (healthy)
  - Verify: `docker compose config` passes

- [x] Step 8: Add `scheduler` service to compose file — **~40% context used**
  - Reference: `src/infrastructure/scheduler/docker-compose.yml` § scheduler service definition
  - Reference: `src/scheduler/src/services/config.py` § Settings class for env var names
  - Build context: `../../scheduler`
  - `ANALYZER_URL` → `http://analyzer:8012`
  - `API_SERVER_URL` → `http://api_server:8013`
  - `PREFECT_API_URL` → `http://prefect_server:4200/api`
  - `DATABASE_URL` → `postgresql://scheduler:scheduler@postgres_scheduler:5432/scheduler`
  - `SERVER_PORT` → `8011`
  - All `STEP_*_BUCKET` env vars preserved from scheduler compose
  - Host port: `8011`
  - Volume: `../../utilities:/utilities`
  - Depends on: `postgres_scheduler` (healthy), `prefect_server` (healthy), `analyzer` (started), `api_server` (healthy)
  - Verify: `docker compose config` passes

- [x] Step 9: Add `data_collector` service to compose file — **~45% context used**
  - Reference: `src/infrastructure/data_collector/docker-compose.yml` § data_collector service definition
  - Reference: `src/data_collector/src/services/config.py` § Settings class for env var names
  - Build context: `../../data_collector`
  - `MINIO_ENDPOINT` → `minio:9000`
  - `SCHEDULER_URL` → `http://scheduler:8011`
  - `SERVER_PORT` → `8010`
  - Host port: `8010`
  - Volume: `../../utilities:/utilities`
  - Depends on: `minio` (healthy)
  - Verify: `docker compose config` passes

- [x] Step 10: Add `aggregator` service to compose file — **~10% context used**
  - Reference: `src/infrastructure/aggregator/docker-compose.yml` § aggregator service definition
  - Reference: `src/aggregator/src/services/config.py` § Settings class for env var names
  - Build context: `../../aggregator`
  - `API_SERVER_URL` → `http://api_server:8013`
  - `SERVER_PORT` → `8014`
  - Host port: `8014`
  - Depends on: `api_server` (healthy)
  - Verify: `docker compose config` passes

## Validation
- [x] Step 11: Run `docker compose -f src/infrastructure/compose/docker-compose.yml up --build -d` and verify all services start healthy — **~50% context used**
  - Fixed: scheduler and aggregator lifespans converted from sync `@contextmanager` to `@asynccontextmanager` (uvicorn requires async protocol)
  - Verify: `docker compose ps` shows all 9 services as `running` / `healthy` ✅
  - Verify: `curl http://localhost:8013/files?limit=1` returns 200 ✅
  - Verify: `curl http://localhost:8012/docs` returns 200 ✅
  - Verify: `curl http://localhost:8014/health` returns 200 ✅
  - Verify: 83 aggregator tests pass, 111 scheduler tests pass ✅

- [x] Step 11.1: Fix `SchedulerService` missing `pipeline_run_id` parameter — **~55% context used**
  - Bug: `_run_flows_concurrently` did not pass `pipeline_run_id` to `process_file_flow`, causing silent `TypeError` in thread pool — flows never ran
  - Fix: generate `uuid.uuid4().hex` per `schedule_batch`/`resume_failed` call and pass through
  - Verify: 111 scheduler tests pass ✅

- [x] Step 12: Run a single-file end-to-end pipeline — **~65% context used**
  - Fixed: Scheduler ignored request bucket, used hardcoded `"raw-data"` — refactored `SchedulerService` to accept `bucket` from route request and use `record.bucket` from DB for resume
  - Fixed: Compose `STEP_*_BUCKET` env vars pointed to non-existent `raw-data`/`cleaned-data` buckets — changed all to `data-collector` (where data_collector uploads)
  - Fixed: Analyzer client 60s timeout too short for data_cleaning (~100s) — added `ANALYZER_TIMEOUT` config (300s default) passed through `execute_step` task
  - Execute: `curl -X POST http://localhost:8010/collector/collect -H "Content-Type: application/json" -d '{"year": {"single": 2024}, "month": {"single": 1}, "taxi_type": "yellow"}'`
  - Verify: response shows 1 success ✅
  - Verify: `curl http://localhost:8013/files` shows file record with `bucket: "data-collector"` ✅
  - Verify: `curl http://localhost:8013/job-executions` shows 5 step executions ✅
  - Verify: `curl http://localhost:8013/analytical-results` shows 5 results (descriptive_statistics=3.84s, data_cleaning=99.53s, temporal_analysis=0.28s, geospatial_analysis=skipped, fare_revenue_analysis=1.36s) ✅
  - Verify: `curl http://localhost:8013/metrics/checkpoint-savings` returns savings data ✅
  - Verify: MinIO has 6 buckets (data-collector + 5 output buckets) ✅
  - Verify: 111 scheduler tests pass ✅

## Cleanup
- [x] Step 13: Tear down — **~5% context used**
  - Execute: `docker compose -f src/infrastructure/compose/docker-compose.yml down --remove-orphans --volumes`
  - Verify: all containers and volumes removed ✅
