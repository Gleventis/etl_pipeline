# Translator

DSL-to-HTTP translator that parses operator commands into orchestrated calls to downstream services (data collector, scheduler, aggregator). Execution is async — returns a run ID immediately, operator polls for status.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service health check |
| POST | `/translator/translate` | Submit DSL for execution, returns `202` with `run_id` |
| GET | `/translator/runs/{run_id}` | Poll run status (`pending`, `collecting`, `analyzing`, `aggregating`, `completed`, `failed`) |

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Postgres connection string | `postgresql://translator:translator@localhost:5436/translator` |
| `SERVER_HOST` | FastAPI server host | `0.0.0.0` |
| `SERVER_PORT` | FastAPI server port | `8015` |
| `COLLECTOR_URL` | Data collector base URL | `http://localhost:8000` |
| `SCHEDULER_URL` | Scheduler base URL | `http://localhost:8001` |
| `AGGREGATOR_URL` | Aggregator base URL | `http://localhost:8003` |
| `HTTP_TIMEOUT` | Timeout for downstream calls (seconds) | `300` |

## DSL

The DSL is submitted as JSON with three optional sections: `collect`, `analyze`, `aggregate`. Each section maps to one downstream service call. Sections can be submitted individually or together.

```json
{
  "collect": {"year": 2024, "month": 1, "taxi_type": "yellow"},
  "analyze": {"bucket": "data-collector", "objects": ["yellow/2024-01.parquet"], "skip_checkpoints": ["temporal_analysis"]},
  "aggregate": {"endpoint": "descriptive-stats", "params": {"taxi_type": "yellow"}}
}
```

## How to run

```bash
docker compose -f src/infrastructure/translator/docker-compose.yml up
```

## How to test

```bash
docker compose -f src/infrastructure/translator/docker-compose.yml run --rm translator uv run pytest tests/ -v
```
