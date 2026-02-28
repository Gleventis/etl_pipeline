# Data Collector

Fetches NYC TLC trip record parquet files, validates schemas, and uploads to MinIO.

## Endpoints

| Method | Path                | Description                                              |
|--------|---------------------|----------------------------------------------------------|
| POST   | `/collector/collect` | Download, validate, and upload TLC parquet files to MinIO |

## Configuration

| Variable           | Description                          | Default            |
|--------------------|--------------------------------------|--------------------|
| `THREAD_POOL_SIZE` | Number of concurrent download threads | `4`               |
| `MINIO_ENDPOINT`   | MinIO endpoint                       | `localhost:9000`   |
| `MINIO_ACCESS_KEY`  | MinIO access key                    | `minioadmin`       |
| `MINIO_SECRET_KEY`  | MinIO secret key                    | `minioadmin`       |
| `MINIO_BUCKET`      | Target bucket name                  | `data-collector`   |
| `SCHEDULER_URL`     | Scheduler service URL               | `http://localhost:8001` |
| `SERVER_HOST`       | FastAPI server host                 | `0.0.0.0`         |
| `SERVER_PORT`       | FastAPI server port                 | `8000`            |

## How to Run

```bash
docker compose -f src/infrastructure/data_collector/docker-compose.yml up data_collector
```

## How to Test

```bash
docker compose -f src/infrastructure/data_collector/docker-compose.yml run --rm data_collector uv run pytest tests/ -v
```
