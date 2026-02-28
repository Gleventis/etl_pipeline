# Analyzer Implementation Plan

## 1. Project Setup
- [x] Create `src/analyzer/` directory structure: `src/server/`, `src/services/`, `src/services/base/`, `src/services/yellow/`, `src/services/green/`, `src/services/fhv/`, `src/services/fhvhv/`, `tests/` — **~15% context used**
- [x] Create `src/analyzer/pyproject.toml` with dependencies: `fastapi`, `uvicorn`, `pydantic`, `pydantic-settings`, `polars`, `numpy`, `scipy`, `scikit-learn`, `pyarrow`, `httpx`, `boto3`
  - Reference: `src/data_collector/pyproject.toml` for structure
- [x] Create `src/analyzer/Dockerfile`
  - Reference: `src/data_collector/Dockerfile`
- [x] Create `src/infrastructure/analyzer/docker-compose.yml` with analyzer + MinIO services
  - Reference: `src/infrastructure/data_collector/docker-compose.yml`
- [x] Add `__init__.py` files to all packages
- [x] Verify: `docker compose build` succeeds

## 2. Configuration
- [x] Create `src/analyzer/src/services/config.py` with `Settings` class
  - `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
  - `API_SERVER_URL`
  - `OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS`, `OUTPUT_BUCKET_DATA_CLEANING`, `OUTPUT_BUCKET_TEMPORAL_ANALYSIS`, `OUTPUT_BUCKET_GEOSPATIAL_ANALYSIS`, `OUTPUT_BUCKET_FARE_REVENUE_ANALYSIS`
  - `SERVER_HOST`, `SERVER_PORT`
  - Reference: `specs/analyzer.md` § Configuration
  - Reference: `src/data_collector/src/services/config.py` for pattern
- [x] Create `tests/test_config.py` — defaults, env override, port range
  - Reference: `src/data_collector/tests/test_config.py`
- [x] Verify: tests pass via docker compose — **~10% context used**

## 3. Request/Response Models
- [x] Create `src/analyzer/src/server/models.py`
  - `AnalyzeRequest`: `input_bucket`, `input_object`, `taxi_type`, `job_execution_id`
  - `AnalyzeResponse`: `success`, `error`
  - `StepResult`: internal model for `summary_data` (dict), `detail_bytes` (bytes), `detail_s3_key` (str)
  - Reference: `specs/analyzer.md` § Request Payload, Response Payload
  - Reference: `src/scheduler/src/services/analyzer_client.py` § `AnalyzerRequest`, `AnalyzerResponse` for compatibility
- [x] Create `tests/test_models.py` — validation, frozen config, taxi_type enum
- [x] Verify: tests pass via docker compose — **~20% context used**

## 4. API Server Client
- [x] Create `src/analyzer/src/services/api_server_client.py`
  - `post_analytical_result(api_server_url, job_execution_id, result_type, summary_data, detail_s3_path, computation_time_seconds)` → bool
  - Uses `httpx.Client` as context manager
  - Reference: `specs/api_server.md` § `POST /analytical-results`
  - Reference: `src/scheduler/src/services/analyzer_client.py` for httpx pattern
- [x] Create `tests/test_api_server_client.py` — success, HTTP error, network error
- [x] Verify: tests pass via docker compose — **~25% context used**

## 5. Abstract Base Classes
- [x] Create `src/analyzer/src/services/base/descriptive_statistics.py`
  - `BaseDescriptiveStatistics` ABC with `analyze(df: polars.DataFrame) -> StepResult`
  - Reference: `specs/analyzer.md` § Descriptive Statistics
- [x] Create `src/analyzer/src/services/base/data_cleaning.py`
  - `BaseDataCleaning` ABC with `analyze(df: polars.DataFrame) -> StepResult`
  - Reference: `specs/analyzer.md` § Data Cleaning
- [x] Create `src/analyzer/src/services/base/temporal_analysis.py`
  - `BaseTemporalAnalysis` ABC with `analyze(df: polars.DataFrame) -> StepResult`
  - Reference: `specs/analyzer.md` § Temporal Analysis
- [x] Create `src/analyzer/src/services/base/geospatial_analysis.py`
  - `BaseGeospatialAnalysis` ABC with `analyze(df: polars.DataFrame) -> StepResult`
  - Reference: `specs/analyzer.md` § Geospatial Analysis
- [x] Create `src/analyzer/src/services/base/fare_revenue_analysis.py`
  - `BaseFareRevenueAnalysis` ABC with `analyze(df: polars.DataFrame) -> StepResult`
  - Reference: `specs/analyzer.md` § Fare Revenue Analysis
- [x] Verify: ABCs importable, cannot be instantiated directly — **~10% context used**

## 6. Registry
- [x] Create `src/analyzer/src/services/registry.py`
  - `get_analyzer(step_name: StepName, taxi_type: TaxiType) -> base class instance`
  - Maps `(step, taxi_type)` → concrete class (20 stub implementations created)
  - Added `StepName` enum to `src/server/models.py`
  - Raises `ValueError` for unknown combinations
  - Reference: `specs/analyzer.md` § Registry
- [x] Create `tests/test_registry.py` — all 20 combinations resolve, unknown raises, instance isolation
- [x] Verify: tests pass via docker compose (60/60 pass) — **~30% context used**

## 7. Concrete Implementation — Descriptive Statistics
- [x] Create `src/analyzer/src/services/yellow/descriptive_statistics.py`
  - Percentiles (1st, 5th, 10th, 25th, 50th, 75th, 90th, 95th, 99th) across all numeric columns
  - Histograms with 100 bins per column (numpy)
  - Correlation matrix between all numeric column pairs (numpy)
  - Distribution statistics: skewness, kurtosis (scipy.stats)
  - Reference: `specs/analyzer.md` § Descriptive Statistics
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.YELLOW]` for column names
- [x] Create `src/analyzer/src/services/green/descriptive_statistics.py`
  - Same as Yellow but using Green column names (`lpep_*` datetimes, `ehail_fee`, `trip_type`)
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.GREEN]`
- [x] Create `src/analyzer/src/services/fhv/descriptive_statistics.py`
  - Limited: only `sr_flag` is numeric-ish; compute basic stats on available columns
  - Skip histogram/correlation for non-numeric columns
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHV]`
- [x] Create `src/analyzer/src/services/fhvhv/descriptive_statistics.py`
  - Partial: `trip_miles`, `trip_time`, `base_passenger_fare`, `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`, `tips`, `driver_pay`
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHVHV]`
- [x] Create `tests/test_descriptive_statistics.py` — test each taxi type with sample DataFrames
- [x] Verify: tests pass via docker compose — **~35% context used**

## 8. Concrete Implementation — Data Cleaning

### 8.1 Yellow Data Cleaning
- [x] Create `src/analyzer/src/services/yellow/data_cleaning.py`
  - IQR outlier detection on fare/distance/duration columns (numpy)
  - Z-score outlier detection (scipy.stats.zscore)
  - Isolation Forest (scikit-learn)
  - Quality rules: negative fares, zero distances, impossible durations (dropoff < pickup), passenger_count validation
  - Compare cleaning strategies: removal vs. capping
  - Reference: `specs/analyzer.md` § Data Cleaning
- [x] Verify: class is importable and instantiable — **~35% context used**

### 8.2 Green Data Cleaning
- [x] Create `src/analyzer/src/services/green/data_cleaning.py`
  - Same as Yellow with Green-specific columns (`lpep_*` datetimes, `ehail_fee`, `trip_type`)
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.GREEN]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 8.3 FHV Data Cleaning
- [x] Create `src/analyzer/src/services/fhv/data_cleaning.py`
  - Limited: duration rules only (dropoff < pickup), no fare/distance rules
  - Skip IQR/Z-score/Isolation Forest on fare columns (not available)
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHV]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 8.4 FHVHV Data Cleaning
- [x] Create `src/analyzer/src/services/fhvhv/data_cleaning.py`
  - Partial: `trip_miles`, `trip_time` rules + fare rules on `base_passenger_fare`, `tips`, `driver_pay`
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHVHV]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 8.5 Data Cleaning Tests
- [x] Create `tests/test_data_cleaning.py` — test each taxi type with sample DataFrames
  - Verify outlier counts per method (IQR, Z-score, Isolation Forest)
  - Verify cleaned output shape
  - Verify quality rule violation counts
  - Verify FHV limited behavior (no fare rules)
  - Verify FHVHV partial behavior
  - Verify detail_bytes is valid parquet
  - Verify summary_data structure
  - Edge cases: empty DataFrame, single row, all-clean data
- [x] Verify: tests pass via docker compose (23/23 pass) — **~25% context used**

## 9. Concrete Implementation — Temporal Analysis

### 9.1 Yellow Temporal Analysis
- [x] Create `src/analyzer/src/services/yellow/temporal_analysis.py`
  - Time-series decomposition on trip counts by hour (manual trend/seasonal/residual)
  - Fourier transforms for frequency analysis (numpy.fft)
  - Rolling window statistics: hourly, daily, weekly trip counts and fare averages
  - Peak hour detection
  - Uses `tpep_pickup_datetime`, `tpep_dropoff_datetime`
  - Reference: `specs/analyzer.md` § Temporal Analysis
- [x] Verify: class is importable and instantiable — **~25% context used**

### 9.2 Green Temporal Analysis
- [x] Create `src/analyzer/src/services/green/temporal_analysis.py`
  - Same logic as Yellow, uses `lpep_pickup_datetime`, `lpep_dropoff_datetime`
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.GREEN]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 9.3 FHV Temporal Analysis
- [x] Create `src/analyzer/src/services/fhv/temporal_analysis.py`
  - Uses `pickup_datetime`, `dropoff_datetime`
  - No fare-based temporal aggregations (no fare columns)
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHV]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 9.4 FHVHV Temporal Analysis
- [x] Create `src/analyzer/src/services/fhvhv/temporal_analysis.py`
  - Uses `request_datetime`, `on_scene_datetime`, `pickup_datetime`, `dropoff_datetime`
  - Additional: wait time analysis (request → on_scene → pickup)
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHVHV]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 9.5 Temporal Analysis Tests
- [x] Create `tests/test_temporal_analysis.py` — test each taxi type with time-series sample data
  - Verify time-series decomposition output (trend, seasonal, residual)
  - Verify Fourier transform output structure
  - Verify rolling window statistics
  - Verify peak hour detection
  - Verify FHV has no fare aggregations
  - Verify FHVHV wait time analysis
  - Verify detail_bytes is valid parquet
  - Verify summary_data structure
  - Edge cases: empty DataFrame, single row, single hour of data
- [x] Verify: tests pass via docker compose (26/26 pass) — **~25% context used**

## 10. Concrete Implementation — Geospatial Analysis

### 10.1 Yellow Geospatial Analysis
- [x] Create `src/analyzer/src/services/yellow/geospatial_analysis.py`
  - DBSCAN clustering on `pulocationid`/`dolocationid` zone pairs (scikit-learn)
  - K-means clustering on zone trip volumes (scikit-learn)
  - Common route detection: top N pickup-dropoff zone pairs
  - Zone-level heatmap data (trip counts per zone)
  - Distance distribution by zone (using `trip_distance`)
  - Reference: `specs/analyzer.md` § Geospatial Analysis
- [x] Verify: class is importable and instantiable — **~25% context used**

### 10.2 Green Geospatial Analysis
- [x] Create `src/analyzer/src/services/green/geospatial_analysis.py`
  - Same as Yellow (same zone ID columns + `trip_distance`)
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.GREEN]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 10.3 FHV Geospatial Analysis
- [x] Create `src/analyzer/src/services/fhv/geospatial_analysis.py`
  - Zone clustering and route detection (has `pulocationid`/`dolocationid`)
  - No distance distribution (no `trip_distance`)
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHV]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 10.4 FHVHV Geospatial Analysis
- [x] Create `src/analyzer/src/services/fhvhv/geospatial_analysis.py`
  - Zone clustering and route detection
  - Distance distribution using `trip_miles`
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHVHV]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 10.5 Geospatial Analysis Tests
- [x] Create `tests/test_geospatial_analysis.py` — test each taxi type with sample DataFrames
  - Verify DBSCAN cluster assignments
  - Verify K-means cluster assignments
  - Verify top route counts
  - Verify zone heatmap data
  - Verify FHV has no distance distribution
  - Verify FHVHV uses `trip_miles` for distance
  - Verify detail_bytes is valid parquet
  - Verify summary_data structure
  - Edge cases: empty DataFrame, single row, single zone
- [x] Verify: tests pass via docker compose (27/27 pass) — **~25% context used**

## 11. Concrete Implementation — Fare Revenue Analysis

### 11.1 Yellow Fare Revenue Analysis
- [x] Create `src/analyzer/src/services/yellow/fare_revenue_analysis.py`
  - Revenue forecasting: linear regression on daily revenue time-series (scikit-learn)
  - Fare anomaly detection: Z-score on `fare_amount`, `total_amount`
  - Tip prediction: regression on `tip_amount` using distance, duration, fare as features (scikit-learn)
  - Fare distribution by zone (`pulocationid`), time-of-day, distance bucket
  - Surcharge breakdown: `extra`, `mta_tax`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`
  - Reference: `specs/analyzer.md` § Fare Revenue Analysis
- [x] Verify: class is importable and instantiable — **~25% context used**

### 11.2 Green Fare Revenue Analysis
- [x] Create `src/analyzer/src/services/green/fare_revenue_analysis.py`
  - Same as Yellow with Green columns (includes `ehail_fee`, `trip_type`)
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.GREEN]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 11.3 FHV Fare Revenue Analysis
- [x] Create `src/analyzer/src/services/fhv/fare_revenue_analysis.py`
  - Skipped entirely: no fare data available
  - Returns summary: `{"skipped": true, "reason": "no fare columns available for FHV"}`
  - Reports `success: true`
  - Reference: `specs/analyzer.md` § Taxi-Type Variations
- [x] Verify: class is importable and instantiable — **~25% context used**

### 11.4 FHVHV Fare Revenue Analysis
- [x] Create `src/analyzer/src/services/fhvhv/fare_revenue_analysis.py`
  - Partial: revenue analysis on `base_passenger_fare`, `driver_pay`
  - Tip prediction on `tips`
  - Surcharge breakdown: `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`
  - No `fare_amount`/`total_amount` — use `base_passenger_fare` as primary
  - Reference: `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS[TaxiType.FHVHV]`
- [x] Verify: class is importable and instantiable — **~25% context used**

### 11.5 Fare Revenue Analysis Tests
- [x] Create `tests/test_fare_revenue_analysis.py` — test each taxi type with sample DataFrames
  - Verify revenue forecasting output (regression coefficients, predictions)
  - Verify fare anomaly detection counts
  - Verify tip prediction output
  - Verify fare distribution by zone/time/distance
  - Verify surcharge breakdown
  - Verify FHV skip behavior (summary has `skipped: true`)
  - Verify FHVHV partial behavior (uses `base_passenger_fare`)
  - Verify detail_bytes is valid parquet
  - Verify summary_data structure
  - Edge cases: empty DataFrame, single row, all-zero fares
- [x] Verify: tests pass via docker compose (30/30 pass) — **~25% context used**

## 12. Route Handler — Shared Logic

### 12.1 Step Executor
- [x] Create `src/analyzer/src/services/step_executor.py`
  - `execute_step(step_name, request, settings)` — shared orchestration logic:
    1. Download parquet from MinIO (`src/utilities/s3.py` § `download_object`)
    2. Load into Polars DataFrame
    3. Resolve analyzer via registry (`src/analyzer/src/services/registry.py`)
    4. Call `analyzer.analyze(df)` and time it
    5. Upload detail parquet to output bucket (`src/utilities/s3.py` § `upload_object`, `ensure_bucket`)
    6. POST summary to API Server (`src/analyzer/src/services/api_server_client.py`)
    7. Return `AnalyzeResponse`
  - Reference: `specs/analyzer.md` § Data Flow Per Request
- [x] Verify: ruff check + format pass — **~25% context used**

### 12.2 Step Executor Tests
- [x] Create `tests/test_step_executor.py`
  - Mock MinIO (download_object, upload_object, ensure_bucket)
  - Mock API Server (post_analytical_result)
  - Mock analyzer (registry.get_analyzer returns mock with .analyze())
  - Verify orchestration flow: download → analyze → upload → post → respond
  - Verify timing is captured
  - Verify error handling: download failure, analysis failure, upload failure, API post failure
  - Verify API post failure does NOT fail the step (logged as warning)
- [x] Verify: tests pass via docker compose (11/11 pass) — **~25% context used**

## 13. FastAPI Routes

### 13.1 Routes Module
- [x] Create `src/analyzer/src/server/routes.py`
  - `APIRouter` with prefix `/analyze` and tag `Analyzer`
  - 5 endpoints, each calling `execute_step` with the appropriate step name:
    - `POST /analyze/descriptive-statistics`
    - `POST /analyze/data-cleaning`
    - `POST /analyze/temporal-analysis`
    - `POST /analyze/geospatial-analysis`
    - `POST /analyze/fare-revenue-analysis`
  - Reference: `specs/analyzer.md` § Endpoints
- [x] Verify: ruff check + format pass, module importable with 5 routes, 198 existing tests pass — **~25% context used**

### 13.2 Wire Router into FastAPI App
- [x] Update `src/analyzer/src/server/main.py`
  - `app.include_router(router)` from routes module
  - Reference: `src/api_server/src/server/main.py` for pattern
  - Verify: ruff clean, 198/198 tests pass — **~10% context used**

### 13.3 Routes Tests
- [x] Create `tests/test_routes.py`
  - Test each of the 5 endpoints with mocked `step_executor.execute_step`
  - Verify request validation (invalid taxi_type, missing fields)
  - Verify response shape matches `AnalyzeResponse`
  - Verify correct `StepName` is passed to `execute_step`
- [x] Verify: tests pass via docker compose (218/218 pass, 20 new) — **~25% context used**

### 13.4 Swagger Test
- [x] Create `tests/test_swagger.py`
  - Verify Swagger UI accessible at `/docs`
  - Reference: `src/data_collector/tests/test_swagger.py`
- [x] Verify: tests pass via docker compose (3/3 pass) — **~10% context used**

### 13.5 Verify Routes
- [x] Verify: all route tests pass via docker compose (221/221 pass) — **~5% context used**

## 14. Integration Tests

### 14.1 Integration Test — Descriptive Statistics with Real MinIO
- [x] Create `tests/test_integration.py` — test descriptive statistics step end-to-end
  - Upload sample parquet to MinIO
  - Call `/analyze/descriptive-statistics` endpoint
  - Verify output bucket has result parquet
  - Verify response is `AnalyzeResponse(success=True)`
  - Reference: `src/data_collector/tests/test_integration.py` for MinIO test pattern
- [x] Verify: tests pass via docker compose (222/222 pass, 1 new) — **~25% context used**

### 14.2 Integration Test — All Taxi Types
- [x] Add integration tests for each taxi type (yellow, green, fhv, fhvhv) with a small sample parquet file
  - Verify each taxi type produces valid output for at least one step
- [x] Verify: tests pass via docker compose (225/225 pass, 3 new) — **~25% context used**

### 14.3 Integration Test — FHV Fare Revenue Skip
- [x] Add integration test for FHV fare revenue skip behavior
  - Call `/analyze/fare-revenue-analysis` with FHV data
  - Verify response is `success: true` with skip summary
- [x] Verify: tests pass via docker compose (5/5 integration pass, 1 new) — **~15% context used**

### 14.4 Integration Test — API Server Mock
- [x] Add integration test with mocked API Server
  - Verify POST payload shape matches `POST /analytical-results` contract
  - Verify API Server failure does not fail the step
- [x] Verify: tests pass via docker compose (230/230 pass, 4 new) — **~25% context used**

### 14.5 Verify Integration Tests
- [x] Verify: all integration tests pass via docker compose (230/230 pass, 5 integration) — **~5% context used**

## 15. Documentation

### 15.1 Service README
- [x] Create `src/analyzer/README.md`
  - Service name and purpose
  - Endpoints (method, path, description)
  - Configuration (environment variables with defaults)
  - How to run (docker compose command)
  - How to test (docker compose command)
  - Reference: `specs/analyzer.md` for content
  - Reference: `src/api_server/README.md` for format
- [x] Verify: tests pass via docker compose (230/230 pass) — **~10% context used**