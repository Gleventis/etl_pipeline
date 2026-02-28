# Scheduler

Pipeline orchestration service that walks NYC TLC data files through a 5-step analytical pipeline using Prefect flows.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/scheduler/schedule` | Schedule a batch of files for pipeline processing. Returns per-file status. |
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

## How to run

```bash
docker compose -f src/infrastructure/scheduler/docker-compose.yml up
```

## How to test

```bash
docker compose -f src/infrastructure/scheduler/docker-compose.yml run --rm scheduler uv run pytest tests/ -v
```
