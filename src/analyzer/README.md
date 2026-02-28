# Analyzer

Computationally intensive analytical processing of NYC TLC parquet data — five sequential steps per file, with per-taxi-type implementations (yellow, green, fhv, fhvhv). Outputs summary JSON to Postgres via the API Server and detailed parquet to MinIO.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/analyze/descriptive-statistics` | Percentiles, histograms, correlation matrix, skewness/kurtosis |
| POST | `/analyze/data-cleaning` | IQR/Z-score/Isolation Forest outlier detection, quality rules, cleaned output |
| POST | `/analyze/temporal-analysis` | Time-series decomposition, Fourier transforms, rolling windows, peak hours |
| POST | `/analyze/geospatial-analysis` | DBSCAN/K-means zone clustering, common routes, heatmaps, distance by zone |
| POST | `/analyze/fare-revenue-analysis` | Revenue forecasting, fare anomaly detection, tip prediction, surcharge breakdown |

All endpoints accept the same request body:

```json
{
  "input_bucket": "raw-data",
  "input_object": "yellow/2024-01.parquet",
  "taxi_type": "yellow",
  "job_execution_id": 42
}
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `API_SERVER_URL` | `http://localhost:8000` | API Server URL for posting analytical results |
| `OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS` | `descriptive-statistics-results` | Output bucket for descriptive statistics parquet |
| `OUTPUT_BUCKET_DATA_CLEANING` | `cleaned-data` | Output bucket for cleaned data parquet |
| `OUTPUT_BUCKET_TEMPORAL_ANALYSIS` | `temporal-analysis-results` | Output bucket for temporal analysis parquet |
| `OUTPUT_BUCKET_GEOSPATIAL_ANALYSIS` | `geospatial-analysis-results` | Output bucket for geospatial analysis parquet |
| `OUTPUT_BUCKET_FARE_REVENUE_ANALYSIS` | `fare-revenue-analysis-results` | Output bucket for fare revenue analysis parquet |
| `SERVER_HOST` | `0.0.0.0` | FastAPI server host |
| `SERVER_PORT` | `8002` | FastAPI server port |

## How to run

```bash
docker compose -f src/infrastructure/analyzer/docker-compose.yml up
```

## How to test

```bash
docker compose -f src/infrastructure/analyzer/docker-compose.yml run --rm analyzer uv run pytest tests/ -v
```
