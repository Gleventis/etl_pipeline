# End-to-End Compose

## Overview
A unified docker-compose file that brings up all services on a shared Docker network, enabling a full pipeline run from data collection through analysis to aggregation. This is required for thesis evaluation — running the pipeline end-to-end on real NYC TLC data and measuring checkpoint savings.

## Problem
Each service has its own `docker-compose.yml` with isolated infrastructure (separate Postgres instances, separate MinIO instances). Services reference each other by hostname (`http://analyzer:8002`, `http://api-server:8000`) but those hostnames only resolve when services share a Docker network — which they don't today.

## Solution
A single `docker-compose.yml` at `src/infrastructure/compose/docker-compose.yml` that defines:

### Shared Infrastructure
1. **MinIO** — single instance shared by data_collector and analyzer
   - Ports: `9000` (API), `9001` (console)
   - Credentials: `minioadmin` / `minioadmin`
2. **Postgres (API Server)** — used by api_server for files, job_executions, analytical_results
   - Port: `5433` (host) → `5432` (container)
   - Credentials: `api_server` / `api_server`
   - Database: `api_server`
3. **Postgres (Scheduler)** — used by scheduler for job_state
   - Port: `5434` (host) → `5432` (container)
   - Credentials: `scheduler` / `scheduler`
   - Database: `scheduler`
4. **Prefect Server** — used by scheduler for flow orchestration
   - Port: `4200`

### Services
All services on the same Docker network. Each service reuses its existing `Dockerfile` via `build.context`.

| Service | Port | Depends On |
|---------|------|------------|
| `data_collector` | `8010` | minio |
| `scheduler` | `8011` | postgres_scheduler, prefect_server, analyzer, api_server |
| `analyzer` | `8012` | minio, api_server |
| `api_server` | `8013` | postgres_api_server |
| `aggregator` | `8014` | api_server |

### Port Mapping
Host ports are offset to avoid conflicts with per-service compose files that may still be running:
- `8010–8014` for services
- `9010/9011` for MinIO
- `5433/5434` for Postgres instances
- `4210` for Prefect

### Environment Variable Overrides
Each service's `Settings` class reads from environment variables. The unified compose overrides the defaults so services point at each other by Docker hostname:

- `data_collector.SCHEDULER_URL` → `http://scheduler:8011`
- `data_collector.MINIO_ENDPOINT` → `minio:9000`
- `scheduler.ANALYZER_URL` → `http://analyzer:8012`
- `scheduler.API_SERVER_URL` → `http://api_server:8013`
- `scheduler.PREFECT_API_URL` → `http://prefect_server:4200/api`
- `analyzer.MINIO_ENDPOINT` → `minio:9000`
- `analyzer.API_SERVER_URL` → `http://api_server:8013`
- `aggregator.API_SERVER_URL` → `http://api_server:8013`

### Healthchecks
Every infrastructure service has a healthcheck. Application services use `depends_on` with `condition: service_healthy` to ensure correct startup order:

1. Postgres instances start first (healthcheck: `pg_isready`)
2. MinIO starts (healthcheck: `mc ready local`)
3. Prefect server starts (healthcheck: python urllib)
4. API Server starts after its Postgres is healthy (healthcheck: `GET /files?limit=1`)
5. Analyzer starts after MinIO is healthy
6. Scheduler starts after Postgres, Prefect, Analyzer, and API Server are healthy
7. Data Collector starts after MinIO is healthy
8. Aggregator starts after API Server is healthy

### How to Run

```bash
# Start all services
docker compose -f src/infrastructure/compose/docker-compose.yml up --build -d

# Trigger a single-file collection
curl -X POST http://localhost:8010/collector/collect \
  -H "Content-Type: application/json" \
  -d '{"year": "2024", "month": "01", "taxi_type": "yellow"}'

# Monitor scheduler (Prefect UI)
open http://localhost:4210

# Check pipeline results
curl http://localhost:8013/files
curl http://localhost:8013/job-executions
curl http://localhost:8013/analytical-results

# Check checkpoint savings
curl http://localhost:8013/metrics/checkpoint-savings
curl http://localhost:8013/metrics/pipeline-summary

# Aggregated results
curl http://localhost:8014/aggregations/pipeline-performance

# Tear down
docker compose -f src/infrastructure/compose/docker-compose.yml down --remove-orphans --volumes
```

## Constraints
- No code changes to any service — only a new `docker-compose.yml` and environment variable overrides
- Each service's `Dockerfile` is reused as-is via `build.context`
- The `utilities/` volume mount is preserved for services that need it (data_collector, scheduler, analyzer)
- The data_collector Dockerfile references `src.server.app:app` (not `main.py`) — this is a known inconsistency but is not addressed here
