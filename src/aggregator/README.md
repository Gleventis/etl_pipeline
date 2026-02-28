# Aggregator

Stateless service that aggregates analytical results from the API Server and returns cross-file summaries.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service health check |
| GET | `/aggregations/descriptive-stats` | Cross-file descriptive statistics |
| GET | `/aggregations/taxi-comparison` | Side-by-side comparison of all taxi types |
| GET | `/aggregations/temporal-patterns` | Hourly/daily trip volume patterns |
| GET | `/aggregations/data-quality` | Data cleaning and outlier metrics |
| GET | `/aggregations/pipeline-performance` | Computation time per analytical step (thesis-critical) |

All aggregation endpoints accept optional query parameters: `taxi_type`, `start_year`, `start_month`, `end_year`, `end_month`. The pipeline-performance endpoint also accepts `analytical_step`.

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `API_SERVER_URL` | API Server base URL | `http://localhost:8000` |
| `SERVER_HOST` | FastAPI server host | `0.0.0.0` |
| `SERVER_PORT` | FastAPI server port | `8003` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `REQUEST_TIMEOUT` | Timeout for API Server calls (seconds) | `30.0` |

## How to run

```bash
docker compose -f src/infrastructure/aggregator/docker-compose.yml up
```

## How to test

```bash
docker compose -f src/infrastructure/aggregator/docker-compose.yml run --rm aggregator uv run pytest tests/ -v
```
