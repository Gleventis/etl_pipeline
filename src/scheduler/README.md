# Scheduler

Pipeline orchestration service that walks NYC TLC data files through a 5-step analytical pipeline using Prefect flows.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/scheduler/schedule` | Schedule a batch of files for pipeline processing. Accepts optional `skip_checkpoints` list to disable state persistence for specific steps (thesis experiments). Returns per-file status. |
| POST | `/scheduler/resume` | Resume all failed jobs from their failed step. |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ANALYZER_URL` | `http://localhost:8002` | Analyzer service URL |
| `PREFECT_API_URL` | `http://localhost:4200/api` | Prefect server URL |
| `DATABASE_URL` | `postgresql://scheduler:scheduler@localhost:5432/scheduler` | Postgres connection string |
| `SERVER_HOST` | `0.0.0.0` | FastAPI server host |
| `SERVER_PORT` | `8001` | FastAPI server port |
| `STEP_DESCRIPTIVE_STATISTICS_BUCKET` | `raw-data` | Input bucket for descriptive statistics step |
| `STEP_DATA_CLEANING_BUCKET` | `raw-data` | Input bucket for data cleaning step |
| `STEP_TEMPORAL_ANALYSIS_BUCKET` | `cleaned-data` | Input bucket for temporal analysis step |
| `STEP_GEOSPATIAL_ANALYSIS_BUCKET` | `cleaned-data` | Input bucket for geospatial analysis step |
| `STEP_FARE_REVENUE_ANALYSIS_BUCKET` | `cleaned-data` | Input bucket for fare revenue analysis step |

## Checkpoint Configuration

The `POST /scheduler/schedule` endpoint accepts an optional `skip_checkpoints` field — a list of step names for which state persistence is skipped after successful completion. Valid step names: `descriptive_statistics`, `data_cleaning`, `temporal_analysis`, `geospatial_analysis`, `fare_revenue_analysis`.

When a step is in the skip list, the pipeline still executes it but does not persist a checkpoint. If a later step fails, resume falls back to the last persisted checkpoint before the skipped step. Failure checkpoints are always saved regardless of the skip list.

Example request body:
```json
{
  "bucket": "raw-data",
  "objects": ["yellow_tripdata_2024-01.parquet"],
  "skip_checkpoints": ["temporal_analysis", "geospatial_analysis"]
}
```

## How to run

```bash
docker compose -f src/infrastructure/scheduler/docker-compose.yml up
```

## How to test

```bash
docker compose -f src/infrastructure/scheduler/docker-compose.yml run --rm scheduler uv run pytest tests/ -v
```
