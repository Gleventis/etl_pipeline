# API Server

REST API service that sits in front of the Postgres database — all other services interact with the database exclusively through this service.

## Endpoints

### Files

| Method | Path | Description |
|--------|------|-------------|
| POST | `/files` | Create or get a file record (idempotent) |
| GET | `/files/{file_id}` | Get a file by ID |
| GET | `/files` | List files with filtering (status, bucket, object_name_pattern, retry_count_min) and pagination |
| PATCH | `/files/{file_id}` | Partially update a file record |

### Job Executions

| Method | Path | Description |
|--------|------|-------------|
| POST | `/job-executions` | Create a single job execution |
| POST | `/job-executions/batch` | Create multiple job executions atomically |
| GET | `/job-executions/{job_execution_id}` | Get a job execution by ID |
| GET | `/job-executions` | List job executions with filtering (file_id, pipeline_run_id, step_name, status, retry_count_min) and pagination |
| PATCH | `/job-executions/{job_execution_id}` | Partially update a job execution |

### Analytical Results

| Method | Path | Description |
|--------|------|-------------|
| POST | `/analytical-results` | Create an analytical result |
| GET | `/analytical-results/{result_id}` | Get an analytical result by ID |
| GET | `/analytical-results` | List analytical results with filtering (result_type, file_id, taxi_type, year, month, created_at range) and pagination |

### Metrics

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics/checkpoint-savings` | Calculate time saved by checkpointing (per-file or aggregate) |
| GET | `/metrics/failure-statistics` | Failure rates per analytical step |
| GET | `/metrics/pipeline-summary` | Comprehensive pipeline summary for thesis reporting |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://api_server:api_server@localhost:5432/api_server` | Postgres connection string |
| `SERVER_HOST` | `0.0.0.0` | FastAPI server host |
| `SERVER_PORT` | `8000` | FastAPI server port |
| `LOG_LEVEL` | `INFO` | Logging level |

## How to run

```bash
docker compose -f src/infrastructure/api_server/docker-compose.yml up
```

## How to test

```bash
docker compose -f src/infrastructure/api_server/docker-compose.yml run --rm api_server uv run pytest tests/ -v
```
