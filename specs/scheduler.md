# Scheduler

## Overview
The scheduler service orchestrates the analytical pipeline for NYC TLC trip data files. It receives batches of files from the data collector, processes each file through a sequence of analytical steps by dispatching jobs to the analyzer service, and tracks job state for failure recovery.

## API

### `POST /schedule`

Synchronous endpoint. Receives a batch of files from the data collector to start the analytical pipeline.

**Request body**:
```json
{
    "bucket": "raw-data",
    "objects": [
        "yellow/2022/01/yellow_tripdata_2022-01.parquet",
        "yellow/2022/02/yellow_tripdata_2022-02.parquet"
    ]
}
```

- `bucket`: the MinIO bucket where the files are stored
- `objects`: list of full object paths to process

**Response**: Status per file (started / already in progress).

### `POST /resume`

Retries all failed jobs from where they left off. Reads failed job state from Postgres and restarts each from its failed step.

**Response**: List of resumed jobs with their restart step.

## Pipeline Steps
Each file goes through these steps sequentially:
1. `descriptive_statistics` — basic stats per file (row count, column distributions, null counts)
2. `data_cleaning` — outlier detection and removal (negative fares, zero-distance trips, impossible durations)
3. `temporal_analysis` — trip volume patterns by hour, day, week, month
4. `geospatial_analysis` — pickup/dropoff hotspots, common routes, distance by zone
5. `fare_revenue_analysis` — fare distributions, tip percentages, surcharge breakdowns, revenue per zone

## Processing Flow
1. Receive batch of object paths + bucket from data collector
2. Create state entry for each file in the in-memory hashmap
3. For each file, walk through the 5 steps sequentially, sending synchronous HTTP requests to the analyzer
4. Multiple files are processed concurrently via a configurable thread pool
5. After each step completes (success or failure), persist state to Postgres
6. On failure, mark the file as failed at that step — other files continue

## Job State
In-memory hashmap for active job tracking:
```python
{
    "yellow/2022/01/yellow_tripdata_2022-01.parquet": {
        "current_step": "data_cleaning",
        "status": "in_progress",  # or "completed", "failed"
        "completed_steps": ["descriptive_statistics"],
        "failed_step": None
    }
}
```
Postgres table for persistence, history tracking, and resume capability.

## Analyzer Request
For each step, the scheduler sends to the analyzer:
```json
{
    "job": "temporal_analysis",
    "input_bucket": "cleaned-data",
    "input_object": "yellow/2022/01/yellow_tripdata_2022-01.parquet"
}
```
The input bucket is resolved from the step-to-bucket environment variable mapping.

## Step-to-Bucket Mapping
Each step's input bucket is configured via environment variables:
- `STEP_DESCRIPTIVE_STATISTICS_BUCKET` — input bucket for descriptive_statistics (default: raw-data)
- `STEP_DATA_CLEANING_BUCKET` — input bucket for data_cleaning (default: raw-data)
- `STEP_TEMPORAL_ANALYSIS_BUCKET` — input bucket for temporal_analysis (default: cleaned-data)
- `STEP_GEOSPATIAL_ANALYSIS_BUCKET` — input bucket for geospatial_analysis (default: cleaned-data)
- `STEP_FARE_REVENUE_ANALYSIS_BUCKET` — input bucket for fare_revenue_analysis (default: cleaned-data)

## Error Handling
Per-file failure isolation. A failed file is marked and stopped at its failed step. Other files in the batch continue processing. The `/resume` endpoint allows retrying failed jobs from the failed step.

## Configuration (Environment Variables)
| Variable | Description |
|---|---|
| `ANALYZER_URL` | Analyzer service URL (single instance or load balancer) |
| `SCHEDULER_THREAD_POOL_SIZE` | Number of concurrent file processing threads |
| `STEP_DESCRIPTIVE_STATISTICS_BUCKET` | Input bucket for descriptive_statistics step |
| `STEP_DATA_CLEANING_BUCKET` | Input bucket for data_cleaning step |
| `STEP_TEMPORAL_ANALYSIS_BUCKET` | Input bucket for temporal_analysis step |
| `STEP_GEOSPATIAL_ANALYSIS_BUCKET` | Input bucket for geospatial_analysis step |
| `STEP_FARE_REVENUE_ANALYSIS_BUCKET` | Input bucket for fare_revenue_analysis step |
| `DATABASE_URL` | Postgres connection string |
| `SERVER_HOST` | FastAPI server host |
| `SERVER_PORT` | FastAPI server port |

## Tech Stack
- Python 3.12, uv
- FastAPI + Pydantic
- httpx (HTTP requests to analyzer)
- psycopg2 / asyncpg (Postgres)
- MinIO (local S3)
