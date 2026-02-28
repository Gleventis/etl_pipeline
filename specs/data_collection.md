# Data Collection

## Overview
The data collector service fetches NYC TLC trip record data (parquet files) for any user-specified year range across all taxi types, validates the downloads and their schemas, and stores them in MinIO (S3).

## Data Source
- **URL**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Format**: Parquet
- **Taxi types**: `yellow`, `green`, `fhv`, `fhvhv`
- **Date range**: Any year available on the TLC site (user-specified)

## API

### `POST /collect`

Synchronous endpoint. Downloads all requested files and returns a response with successes and failures.

**Request body**:
```json
{
  "year": {"from": 2020, "to": 2023},
  "month": {"from": 1, "to": 12},
  "taxi_type": "all"
}
```

- `year`: single value or range (`from`/`to`)
- `month`: single value or range (`from`/`to`)
- `taxi_type`: one of `yellow`, `green`, `fhv`, `fhvhv`, `all`

**Response**: List of successes and failures per file.

## Processing Flow
1. Parse and validate the request (Pydantic)
2. Generate download URLs from the TLC site
3. Download files concurrently using a configurable thread pool, batching if the number of files exceeds the pool size
4. Validate each download:
   - HTTP success and non-zero file size
   - Valid parquet with expected schema per taxi type (using pyarrow)
5. Upload valid files to MinIO at `<taxi_type>/<year>/<month>/<filename>.parquet` using the shared S3 utility (`src/utilities/`)
6. Return response listing successes and failures

## Error Handling
Partial success — failed downloads do not block the rest. The response reports which files succeeded and which failed.

## Validation
- **Download validation**: HTTP 200, non-zero file size, readable parquet
- **Schema validation**: Per-taxi-type Pydantic models that verify the parquet schema matches expected columns and types (via pyarrow)

## Configuration (Environment Variables)
| Variable | Description |
|---|---|
| `THREAD_POOL_SIZE` | Number of concurrent download threads |
| `MINIO_ENDPOINT` | MinIO endpoint (e.g., `localhost:9000`) |
| `MINIO_ACCESS_KEY` | MinIO access key |
| `MINIO_SECRET_KEY` | MinIO secret key |
| `MINIO_BUCKET` | Target bucket name |
| `SERVER_HOST` | FastAPI server host |
| `SERVER_PORT` | FastAPI server port |

## Tech Stack
- Python 3.12, uv
- FastAPI + Pydantic
- pyarrow (parquet schema reading/validation)
- boto3 / minio SDK (S3 uploads)
- MinIO (local S3)
