# Project Overview

## Overview

This is a thesis project investigating checkpointing in distributed systems. The research vehicle is a microservice-based ETL system that processes NYC TLC (Taxi & Limousine Commission) taxi trip data across multiple coordinating services. The core thesis contribution is evaluating how checkpointing affects fault tolerance, recovery time, and resource efficiency in a distributed architecture — the system persists state after each processing step so that on failure, any service can resume from the last checkpoint rather than requiring a full restart of the distributed workflow.

The system is composed of six independently deployable services that communicate over HTTP, demonstrating real-world distributed system concerns: partial failure, cross-service state coordination, and recovery semantics. The analytical workload (five sequential compute-intensive steps per file) provides a measurable basis for comparing checkpoint-enabled vs. checkpoint-disabled recovery.

## Dependencies & Prerequisites

- Docker and Docker Compose for container orchestration
- Python 3.12 runtime
- Network access to the NYC TLC CDN for downloading source parquet files

## Data Sources

| Source | Type | Details |
|--------|------|---------|
| NYC TLC CDN | HTTPS | Public parquet files for yellow, green, FHV, and FHVHV taxi trip data |
| MinIO | S3-compatible | Raw parquet uploads (data-collector bucket) and analytical output parquet (per-step buckets) |
| Postgres (API Server) | PostgreSQL | `files`, `job_executions`, `analytical_results` tables |
| Postgres (Scheduler) | PostgreSQL | `job_state` table for checkpoint persistence |
| Postgres (Translator) | PostgreSQL | `translator_runs` table |

## API & Endpoints

### Data Collector (port 8010)

- `POST /collect` — Downloads TLC parquet files, validates schema, uploads to MinIO, notifies scheduler.

### Scheduler (port 8011)

- `POST /schedule` — Starts the analytical pipeline for a set of files. Supports `skip_checkpoints` parameter for thesis A/B experiments.
- `POST /resume` — Retries failed jobs from the last checkpoint.

### Analyzer (port 8012)

- `POST /analyze/descriptive-statistics`
- `POST /analyze/data-cleaning`
- `POST /analyze/temporal-analysis`
- `POST /analyze/geospatial-analysis`
- `POST /analyze/fare-revenue-analysis`

Each endpoint has 4 taxi-type-specific implementations (20 total via registry pattern). Dual output: summary JSONB to API Server, detail parquet to S3.

### API Server (port 8013)

- Full CRUD + filtering + batch inserts for `files`, `job_executions`, `analytical_results`.
- 6 metrics endpoints: checkpoint savings, failure statistics, pipeline summary, step performance, pipeline efficiency, recovery time.

### Aggregator (port 8014)

- `GET /aggregate/descriptive-stats`
- `GET /aggregate/taxi-type-comparison`
- `GET /aggregate/temporal-patterns`
- `GET /aggregate/data-quality-summary`
- `GET /aggregate/pipeline-performance`

### Translator (port 8015)

- `POST /translator/translate` — Accepts JSON DSL, returns `run_id` (202 Accepted), executes asynchronously.
- `GET /translator/runs/{run_id}` — Poll run status.

## Processing Flow

1. Data Collector downloads TLC parquet files → validates schema per taxi type → uploads to MinIO → calls `POST /schedule` on Scheduler.
2. Scheduler creates a file record via API Server → submits one Prefect flow per file → each flow runs 5 analytical steps sequentially.
3. For each step, Scheduler creates a `job_execution` record, then calls the Analyzer's step-specific endpoint.
4. Analyzer downloads parquet from MinIO → runs taxi-type-specific analysis → uploads detail parquet to S3 → posts summary JSONB to API Server → returns status.
5. Scheduler updates `job_execution` and file status via API Server PATCH endpoints (timing, status, computation seconds). Checkpoint state is persisted to Postgres after each successful step.
6. On failure, `POST /resume` reads failed jobs from Postgres and restarts each from its failed step.
7. Aggregator fetches analytical results from API Server on demand and computes cross-file aggregates.
8. Translator provides a DSL interface that orchestrates the above flow end-to-end.

### Analytical Steps (per file, sequential)

| Step | Name | Duration | Description |
|------|------|----------|-------------|
| 1 | Descriptive Statistics | ~30-60s | Percentiles (1st–99th), histograms (100 bins), correlation matrix, skewness/kurtosis |
| 2 | Data Cleaning | ~45-90s | IQR, Z-score, Isolation Forest outlier detection; data quality rules; cleaning strategy comparison. Produces cleaned parquet |
| 3 | Temporal Analysis | ~60-120s | Time-series decomposition, Fourier transforms, rolling window statistics, peak hour detection |
| 4 | Geospatial Analysis | ~90-180s | DBSCAN/K-means clustering on zones, route detection, zone heatmaps, distance distribution |
| 5 | Fare Revenue Analysis | ~60-120s | Revenue forecasting, fare anomaly detection, tip prediction, fare distribution. FHV skipped (no fare data) |

## Error Handling

| Error Condition | Behavior | User-facing message |
|-----------------|----------|---------------------|
| Analytical step failure | Scheduler marks job as failed, persists checkpoint state | Step-level error in `job_executions` record |
| File download failure | Data Collector returns error for that file, continues others | HTTP error response with file details |
| Service unreachable | httpx timeout/connection error propagated to caller | HTTP 502/503 with service identification |
| Schema validation failure | Data Collector rejects file before upload | HTTP 422 with schema mismatch details |

## Validation

| Input | Rule | On failure |
|-------|------|------------|
| Parquet schema | Must match expected columns for taxi type | 422 — file rejected before upload |
| DSL payload | Must contain valid COLLECT/ANALYZE/AGGREGATE sections | 422 — validation error |
| Taxi type | Must be one of: yellow, green, fhv, fhvhv | 422 — invalid taxi type |

## Security & Authorization

*N/A — this is a thesis project with no authentication or authorization requirements. All services communicate over a private Docker network.*

## Configuration

| Variable | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `MINIO_ENDPOINT` | string | yes | — | MinIO connection URL |
| `MINIO_ACCESS_KEY` | string | yes | — | MinIO access key |
| `MINIO_SECRET_KEY` | string | yes | — | MinIO secret key |
| `POSTGRES_DSN` | string | yes | — | PostgreSQL connection string (per service) |
| `PREFECT_API_URL` | string | yes | — | Prefect server URL |
| `SCHEDULER_URL` | string | yes | — | Scheduler service URL (used by Data Collector) |
| `API_SERVER_URL` | string | yes | — | API Server URL (used by Scheduler, Analyzer, Aggregator) |
| `ANALYZER_URL` | string | yes | — | Analyzer service URL (used by Scheduler) |

## Tech Stack

- **Language**: Python 3.12
- **Package manager**: uv
- **Framework**: FastAPI + Pydantic + pydantic-settings
- **ORM**: SQLAlchemy (API Server), psycopg3 (Translator)
- **Analytics**: Polars, NumPy, SciPy, scikit-learn, PyArrow
- **HTTP client**: httpx (inter-service), boto3 (S3/MinIO)
- **Orchestration**: Prefect 3 (self-hosted server)
- **Infrastructure**: Docker + Docker Compose, MinIO, PostgreSQL
- **Testing**: pytest + testcontainers

## Testing Strategy

- **Unit**: Validation logic, analytical computations, model serialization (~800+ tests across all services)
- **Integration**: Endpoint responses with testcontainers (Postgres, MinIO)
- **E2E**: Full pipeline run via unified docker-compose processing real TLC data through all 5 steps

## Performance & SLA

| Step | Expected Duration |
|------|-------------------|
| Descriptive Statistics | 30–60s per file |
| Data Cleaning | 45–90s per file |
| Temporal Analysis | 60–120s per file |
| Geospatial Analysis | 90–180s per file |
| Fare Revenue Analysis | 60–120s per file |
| Full pipeline (5 steps) | ~5–10 min per file |

Multiple files are processed concurrently via Prefect flows.

## Glossary

| Term | Definition |
|------|------------|
| TLC | NYC Taxi & Limousine Commission |
| Checkpoint | Persisted distributed system state after a successful processing step, enabling resume-from-failure without restarting the entire workflow |
| FHV | For-Hire Vehicle (e.g., Uber, Lyft) |
| FHVHV | For-Hire Vehicle High Volume (large operators like Uber/Lyft) |
| DSL | Domain-Specific Language — the JSON format accepted by the Translator service |
| MinIO | S3-compatible object storage used for parquet file storage |
