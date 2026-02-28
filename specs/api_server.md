# API Server

## Overview
The API Server is the single interface to the Postgres database. All other services interact with the database exclusively through this service's REST API.

## API

### Files

#### `POST /files`
Create or update a file record (idempotent).

**Request:**
```json
{
  "bucket": "raw-data",
  "object_name": "yellow/2022/01/yellow_tripdata_2022-01.parquet",
  "overall_status": "pending"
}
```

**Response:** `201 Created`
```json
{
  "file_id": 123,
  "bucket": "raw-data",
  "object_name": "yellow/2022/01/yellow_tripdata_2022-01.parquet",
  "overall_status": "pending",
  "total_computation_seconds": 0.0,
  "total_elapsed_seconds": 0.0,
  "retry_count": 0,
  "created_at": "2024-03-02T22:00:00Z",
  "updated_at": "2024-03-02T22:00:00Z"
}
```

#### `GET /files/{file_id}`
Get file by ID. Returns `404` if not found.

#### `GET /files`
List files with filtering and pagination.

**Query params:**
- `status`: Filter by overall_status
- `bucket`: Filter by bucket
- `object_name_pattern`: SQL LIKE pattern
- `retry_count_min`: Minimum retry count
- `limit` (default=100, max=1000)
- `offset` (default=0)

**Response:**
```json
{
  "files": [ /* file objects */ ],
  "total": 240,
  "limit": 100,
  "offset": 0
}
```

#### `PATCH /files/{file_id}`
Update file (all fields optional). Returns `404` if not found.

**Request:**
```json
{
  "overall_status": "completed",
  "total_computation_seconds": 487.3,
  "total_elapsed_seconds": 512.8,
  "retry_count": 1
}
```

---

### Job Executions

#### `POST /job-executions`
Create single job execution. Returns `404` if file_id doesn't exist.

**Request:**
```json
{
  "file_id": 123,
  "pipeline_run_id": "abc-def-123",
  "step_name": "descriptive_statistics",
  "status": "pending",
  "retry_count": 0
}
```

**Response:** `201 Created`
```json
{
  "job_execution_id": 456,
  "file_id": 123,
  "pipeline_run_id": "abc-def-123",
  "step_name": "descriptive_statistics",
  "status": "pending",
  "started_at": null,
  "completed_at": null,
  "computation_time_seconds": null,
  "retry_count": 0,
  "error_message": null,
  "created_at": "2024-03-02T22:00:00Z",
  "updated_at": "2024-03-02T22:00:00Z"
}
```

#### `POST /job-executions/batch`
Create multiple job executions atomically. All-or-nothing transaction.

**Request:**
```json
{
  "file_id": 123,
  "pipeline_run_id": "abc-def-123",
  "executions": [
    {"step_name": "descriptive_statistics", "status": "pending", "retry_count": 0},
    {"step_name": "data_cleaning", "status": "pending", "retry_count": 0},
    {"step_name": "temporal_analysis", "status": "pending", "retry_count": 0},
    {"step_name": "geospatial_analysis", "status": "pending", "retry_count": 0},
    {"step_name": "fare_revenue_analysis", "status": "pending", "retry_count": 0}
  ]
}
```

**Response:** `201 Created`
```json
{
  "job_execution_ids": [456, 457, 458, 459, 460],
  "created_count": 5
}
```

#### `GET /job-executions/{job_execution_id}`
Get job execution by ID. Returns `404` if not found.

#### `GET /job-executions`
List job executions with filtering and pagination.

**Query params:**
- `file_id`: Filter by file
- `pipeline_run_id`: Filter by pipeline run
- `step_name`: Filter by step
- `status`: Filter by status
- `retry_count_min`: Minimum retry count
- `limit` (default=100, max=1000)
- `offset` (default=0)

#### `PATCH /job-executions/{job_execution_id}`
Update job execution (all fields optional). Returns `404` if not found.

**Request:**
```json
{
  "status": "completed",
  "started_at": "2024-03-02T22:00:00Z",
  "completed_at": "2024-03-02T22:01:03Z",
  "computation_time_seconds": 63.2,
  "error_message": null
}
```

---

### Analytical Results

#### `POST /analytical-results`
Store analytical results. Returns `404` if job_execution_id doesn't exist.

**Request:**
```json
{
  "job_execution_id": 456,
  "result_type": "descriptive_statistics",
  "summary_data": {
    "total_rows": 2463931,
    "avg_fare": 13.52,
    "percentiles": {"fare": {"p50": 12.5, "p95": 45.0}}
  },
  "detail_s3_path": "results/yellow/2022/01/descriptive_statistics.parquet",
  "computation_time_seconds": 63.2
}
```

**Response:** `201 Created`
```json
{
  "result_id": 789,
  "job_execution_id": 456,
  "result_type": "descriptive_statistics",
  "summary_data": { /* ... */ },
  "detail_s3_path": "results/yellow/2022/01/descriptive_statistics.parquet",
  "computation_time_seconds": 63.2,
  "created_at": "2024-03-02T22:01:03Z"
}
```

#### `GET /analytical-results/{result_id}`
Get result by ID. Returns `404` if not found.

#### `GET /analytical-results`
Query results with complex filtering. Joins analytical_results → job_executions → files.

**Query params:**
- `result_type`: Filter by type
- `file_id`: Filter by file
- `taxi_type`: Extract from object_name (yellow, green, fhv, fhvhv)
- `year`: Extract from object_name
- `month`: Extract from object_name
- `created_at_from`: Results after timestamp
- `created_at_to`: Results before timestamp
- `limit` (default=100, max=1000)
- `offset` (default=0)

**Response:**
```json
{
  "results": [
    {
      "result_id": 789,
      "job_execution_id": 456,
      "result_type": "descriptive_statistics",
      "summary_data": { /* ... */ },
      "detail_s3_path": "results/yellow/2022/01/descriptive_statistics.parquet",
      "computation_time_seconds": 63.2,
      "created_at": "2024-03-02T22:01:03Z",
      "file_info": {
        "file_id": 123,
        "bucket": "raw-data",
        "object_name": "yellow/2022/01/yellow_tripdata_2022-01.parquet"
      }
    }
  ],
  "total": 12,
  "limit": 100,
  "offset": 0
}
```

---

### Metrics

#### `GET /metrics/checkpoint-savings`
Calculate time saved by checkpointing.

**Query params:**
- `file_id` (optional): Specific file or all files

**Response (specific file):**
```json
{
  "file_id": 123,
  "object_name": "yellow/2022/01/yellow_tripdata_2022-01.parquet",
  "time_saved_seconds": 125.4,
  "actual_computation_seconds": 487.3,
  "percent_saved": 25.7,
  "retry_count": 1
}
```

**Response (all files):**
```json
{
  "files_with_retries": 36,
  "total_time_saved_seconds": 4512.8,
  "total_time_saved_hours": 1.25,
  "avg_time_saved_per_file_seconds": 125.4,
  "total_computation_seconds": 17532.1,
  "percent_saved": 25.7
}
```

#### `GET /metrics/failure-statistics`
Failure rates per step.

**Response:**
```json
{
  "statistics": [
    {
      "step_name": "geospatial_analysis",
      "total_files_processed": 240,
      "files_that_failed": 18,
      "failure_rate_percent": 7.5,
      "avg_retries_when_failed": 1.2,
      "avg_computation_seconds": 135.7
    }
  ]
}
```

#### `GET /metrics/pipeline-summary`
Comprehensive pipeline summary.

**Response:**
```json
{
  "total_files": 240,
  "files_with_retries": 36,
  "retry_rate_percent": 15.0,
  "avg_computation_minutes_per_file": 8.1,
  "total_computation_hours": 32.4,
  "total_hours_saved_by_checkpointing": 1.25,
  "avg_minutes_saved_per_retry": 2.1,
  "percent_time_saved": 3.9
}
```

---

## Database Schema

See [thesis_metrics.md](thesis_metrics.md) for complete schema definitions.

**Tables:**
1. `files` - file tracking with aggregated metrics
2. `job_executions` - step-level execution tracking
3. `analytical_results` - analytical outputs

---

## Configuration (Environment Variables)

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | Postgres connection string | Required |
| `SERVER_HOST` | FastAPI server host | `0.0.0.0` |
| `SERVER_PORT` | FastAPI server port | `8000` |
| `LOG_LEVEL` | Logging level | `INFO` |

## Startup Behavior
1. Connect to Postgres
2. Initialize schema (create tables and indexes if not exist)
3. Start FastAPI server

## Tech Stack
- Python 3.12, uv
- FastAPI + Pydantic
- SQLAlchemy (ORM)
- psycopg2 or asyncpg

## Error Handling
- `400 Bad Request`: Invalid input
- `404 Not Found`: Resource doesn't exist
- `422 Unprocessable Entity`: Validation errors
- `500 Internal Server Error`: Database errors

Error format:
```json
{
  "error": "Not Found",
  "detail": "File with id 123 does not exist",
  "status_code": 404
}
```
