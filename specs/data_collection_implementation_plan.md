# Data Collection — Implementation Plan

## 1. Project Setup ✅
- Scaffold `src/data_collector/` per repo structure (`specs/README.md` § Overall requirements #5)
  - `pyproject.toml` — dependencies: fastapi, uvicorn, pydantic, pydantic-settings, pyarrow, boto3, httpx
  - `src/server/` — FastAPI app and routes
  - `src/services/` — business logic
  - `tests/` — pytest + testcontainers
- Create `Dockerfile` and `docker-compose.yml` (`specs/README.md` § Overall requirements #3)

## 2. Configuration ✅
- Create `src/data_collector/src/services/config.py` — Pydantic `BaseSettings` model loading env vars (`specs/data_collection.md` § Configuration)
  - `THREAD_POOL_SIZE`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `SERVER_HOST`, `SERVER_PORT`

## 3. Request/Response Models ✅
- Create `src/data_collector/src/server/models.py` — Pydantic models (`specs/data_collection.md` § API)
  - `CollectRequest`: year (single or range), month (single or range), taxi_type
  - `CollectResponse`: list of successes, list of failures (with reason)

## 4. Schema Validation Models ✅
- Create `src/data_collector/src/services/schemas.py` — expected parquet schemas per taxi type (`specs/data_collection.md` § Validation)
  - Define expected column names (lowercase) for yellow, green, fhv, fhvhv
  - `validate_parquet_schema()` reads parquet schema via pyarrow, case-insensitive subset check against expected columns
  - Extra columns allowed (TLC adds fields over time), missing columns reported as errors

## 5. URL Generation ✅
- Create `src/data_collector/src/services/url_generator.py` (`specs/data_collection.md` § Data Source)
  - Generate TLC download URLs from user-specified year range, month range, and taxi type
  - URL pattern: `https://d37ci6vzurychx.cloudfront.net/trip-data/<type>_tripdata_<year>-<month>.parquet`
  - No hardcoded year constraints — if the file doesn't exist on TLC, it will be reported as a failure

## 6. Download Service ✅
- Create `src/data_collector/src/services/downloader.py` (`specs/data_collection.md` § Processing Flow)
  - Download a single file via httpx, validate HTTP status and file size
  - Validate parquet schema using `schemas.py`
  - Thread pool executor with configurable size, batching downloads
  - Return per-file success/failure results

## 7. S3 Storage (Shared Utility) ✅
- Create `src/utilities/s3.py` — shared S3 client wrapper (`specs/README.md` § Overall requirements #5, `specs/data_collection.md` § Processing Flow #5)
  - Upload and retrieve objects from MinIO
  - Reusable by all services
- Data collector uses this utility to upload validated parquet files
  - Key format: `<taxi_type>/<year>/<month>/<filename>.parquet`

## 8. FastAPI Server ✅
- Create `src/data_collector/src/server/app.py` — FastAPI app (`specs/data_collection.md` § API)
- Create `src/data_collector/src/server/routes.py` — `POST /collect` endpoint
  - Validate request → generate URLs → download → validate → upload → return response
  - Synchronous endpoint, partial success handling (`specs/data_collection.md` § Error Handling)

## 9. Entrypoint ✅
- Create `src/data_collector/src/main.py` — uvicorn startup using config

## 10. Docker ✅
- `src/data_collector/Dockerfile` — Python 3.12, uv install, run uvicorn (`specs/README.md` § Overall requirements #3)
- `src/infrastructure/data_collector/docker-compose.yml` — data_collector + MinIO services (`specs/README.md` § Overall requirements #5)
  - Mounts utilities volume and sets PYTHONPATH for cross-service imports

## 11. Integration Tests ✅
- Create `src/data_collector/tests/test_integration.py` — integration tests for `POST /collect` with real MinIO (`specs/README.md` § Overall requirements #4)
  - Mock `download_batch` (avoids hitting TLC CDN), use real MinIO for S3 upload verification
  - Tests: single file upload, partial failure (only successes uploaded), multiple files uploaded
  - Verifies uploaded files are retrievable from MinIO and match original bytes
  - 73 total tests passing (70 unit + 3 integration)

## 12. Service README ✅
- Create `src/data_collector/README.md` per Service README Rule
  - Service name, endpoints, configuration, how to run, how to test
  - 81 total tests passing (78 unit + 3 integration)
