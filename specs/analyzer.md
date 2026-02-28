# Analyzer

## Overview
The analyzer service performs computationally intensive analytical jobs on NYC TLC trip data. It receives requests from the scheduler, runs one of five analytical steps on a parquet file, and writes results to both Postgres (summary via the API Server) and S3 (detailed parquet). Each step is designed to be expensive (~30–180 seconds) to demonstrate the value of checkpointing in the pipeline.

## Data Source
- **Input**: Parquet files stored in MinIO, downloaded by the data collector
- **Taxi types**: `yellow`, `green`, `fhv`, `fhvhv` — each has different columns, so every analytical step has a per-taxi-type concrete implementation
- **Column schemas**: Defined in `src/data_collector/src/services/schemas.py` § `EXPECTED_COLUMNS`

## API

### `POST /analyze/descriptive-statistics`
### `POST /analyze/data-cleaning`
### `POST /analyze/temporal-analysis`
### `POST /analyze/geospatial-analysis`
### `POST /analyze/fare-revenue-analysis`

All five endpoints share the same request/response contract. Each maps to one analytical step.

**Request body**:
```json
{
  "input_bucket": "raw-data",
  "input_object": "yellow/2022/01/yellow_tripdata_2022-01.parquet",
  "taxi_type": "yellow",
  "job_execution_id": 456
}
```

- `input_bucket`: MinIO bucket containing the input parquet file
- `input_object`: Full object path within the bucket
- `taxi_type`: One of `yellow`, `green`, `fhv`, `fhvhv`
- `job_execution_id`: ID from the `job_executions` table, used when posting results to the API Server

**Response**:
```json
{
  "success": true,
  "error": null
}
```

On failure:
```json
{
  "success": false,
  "error": "description of what went wrong"
}
```

## Request Payload
Defined as `AnalyzeRequest` in `src/analyzer/src/server/models.py`:
- `input_bucket: str` (min_length=1)
- `input_object: str` (min_length=1)
- `taxi_type: TaxiType` (enum: yellow, green, fhv, fhvhv)
- `job_execution_id: int` (ge=1)
- Frozen (immutable)

## Response Payload
Defined as `AnalyzeResponse` in `src/analyzer/src/server/models.py`:
- `success: bool`
- `error: str | None`
- Frozen (immutable)

## Data Flow Per Request

1. Receive `AnalyzeRequest` at the step-specific endpoint
2. Download parquet from MinIO (`input_bucket` / `input_object`) using `src/utilities/s3.py` § `download_object`
3. Load bytes into a Polars DataFrame
4. Resolve the concrete analyzer via the registry (`src/analyzer/src/services/registry.py` § `get_analyzer`)
5. Call `analyzer.analyze(df)` and time the execution
6. Upload `StepResult.detail_bytes` to the step's output bucket in MinIO (`src/utilities/s3.py` § `upload_object`, `ensure_bucket`)
7. POST `StepResult.summary_data` to the API Server (`POST /analytical-results`) with `job_execution_id`, `result_type`, `detail_s3_path`, and `computation_time_seconds`
8. Return `AnalyzeResponse` to the scheduler

## Analytical Steps

### 1. Descriptive Statistics (~30–60s per file)
- Percentiles (1st, 5th, 10th, 25th, 50th, 75th, 90th, 95th, 99th) across all numeric columns
- Histograms with 100 bins per column (numpy)
- Correlation matrix between all numeric column pairs (numpy)
- Distribution statistics: skewness, kurtosis (scipy.stats)
- **Output summary**: Percentiles, distribution stats, correlation column list, row/column counts
- **Output detail**: Histograms and correlation matrix as parquet

### 2. Data Cleaning (~45–90s per file)
- IQR outlier detection on fare/distance/duration columns (numpy)
- Z-score outlier detection (scipy.stats.zscore)
- Isolation Forest outlier detection (scikit-learn)
- Data quality rules: negative fares, zero distances, impossible durations (dropoff < pickup), passenger_count validation
- Compare cleaning strategies: removal vs. capping
- **Output summary**: Outlier counts per method, removal stats, quality rule violations
- **Output detail**: Cleaned dataframe as parquet

### 3. Temporal Analysis (~60–120s per file)
- Time-series decomposition (trend, seasonality, residuals)
- Fourier transforms for frequency analysis (numpy.fft)
- Rolling window statistics: hourly, daily, weekly trip counts and fare averages
- Peak hour detection and trip volume patterns
- **Output summary**: Temporal patterns, peak hours, aggregated stats
- **Output detail**: Full time-series data as parquet

### 4. Geospatial Analysis (~90–180s per file)
- DBSCAN clustering on pickup/dropoff zone pairs (scikit-learn)
- K-means clustering on zone trip volumes (scikit-learn)
- Common route detection: top N pickup-dropoff zone pairs
- Zone-level heatmap data (trip counts per zone)
- Distance distribution by geographic zone
- **Output summary**: Hotspot summaries, cluster metadata, top routes
- **Output detail**: Spatial clusters and zone-level data as parquet

### 5. Fare Revenue Analysis (~60–120s per file)
- Revenue forecasting: linear regression on daily revenue time-series (scikit-learn)
- Fare anomaly detection: Z-score on fare columns
- Tip prediction: regression using distance, duration, fare as features (scikit-learn)
- Fare distribution by zone, time-of-day, distance bucket
- Surcharge and revenue breakdown calculations
- **Output summary**: Revenue statistics, predictions, anomaly counts
- **Output detail**: Detailed fare analysis as parquet

## Taxi-Type Variations

| Step | Yellow | Green | FHV | FHVHV |
|---|---|---|---|---|
| Descriptive Statistics | Full (14 numeric cols) | Full (green-specific cols: `lpep_*`, `ehail_fee`, `trip_type`) | Limited (`sr_flag`, location IDs only) | Partial (`trip_miles`, `trip_time`, fare cols) |
| Data Cleaning | Full (fare, distance, duration rules) | Same as Yellow with green columns | Limited (duration rules only, no fare/distance) | Partial (`trip_miles`, `trip_time`, `base_passenger_fare`) |
| Temporal Analysis | `tpep_pickup_datetime`, `tpep_dropoff_datetime` | `lpep_pickup_datetime`, `lpep_dropoff_datetime` | `pickup_datetime`, `dropoff_datetime` (no fare aggregations) | `request_datetime`, `on_scene_datetime`, `pickup_datetime`, `dropoff_datetime` + wait time analysis |
| Geospatial Analysis | Zone clustering + `trip_distance` | Same as Yellow | Zone clustering only (no distance) | Zone clustering + `trip_miles` |
| Fare Revenue Analysis | Full (all fare/surcharge cols) | Full (includes `ehail_fee`, `trip_type`) | Skipped entirely (no fare data) | Partial (`base_passenger_fare`, `driver_pay`, `tips`) |

## Output Format

Each analytical step produces two outputs:

1. **Summary results** → Stored in Postgres via API Server (`POST /analytical-results`, JSONB format, ~10–50 KB per step)
   - Queryable aggregates and statistics
   - Computation time metrics for thesis evaluation
   - Key findings and metadata

2. **Detailed results** → Stored in S3 as parquet files (~1–10 MB per step)
   - Full distributions, clusters, time-series data
   - Intermediate computation artifacts
   - Detailed breakdowns for deep analysis

## Internal Model
`StepResult` in `src/analyzer/src/server/models.py`:
- `summary_data: dict` — JSONB payload for the API Server
- `detail_bytes: bytes` — parquet bytes for S3 upload
- `detail_s3_key: str` — object key within the output bucket

## Registry

The registry (`src/analyzer/src/services/registry.py`) maps `(StepName, TaxiType)` → concrete analyzer class. There are 20 combinations (5 steps × 4 taxi types). Each concrete class lives under `src/analyzer/src/services/<taxi_type>/<step_name>.py`.

All concrete analyzers inherit from one of five ABCs in `src/analyzer/src/services/base/`:
- `BaseDescriptiveStatistics` — template method pattern: ABC implements `analyze()`, subclasses provide `_numeric_columns()`
- `BaseDataCleaning` — abstract `analyze()`, fully implemented per taxi type
- `BaseTemporalAnalysis` — abstract `analyze()`, fully implemented per taxi type
- `BaseGeospatialAnalysis` — abstract `analyze()`, fully implemented per taxi type
- `BaseFareRevenueAnalysis` — abstract `analyze()`, fully implemented per taxi type

## Step Executor

`src/analyzer/src/services/step_executor.py` contains the shared orchestration logic called by each route handler. It encapsulates the full data flow (download → analyze → upload → post results → respond).

## Scheduler Contract

The scheduler dispatches jobs to step-specific endpoints. The current scheduler client (`src/scheduler/src/services/analyzer_client.py`) sends to a single `POST /analyze` endpoint — this needs updating to route to per-step endpoints and include `taxi_type` and `job_execution_id` fields. This is tracked as a future task in the implementation plan.

## Error Handling
- Download failures (MinIO unreachable, object not found) → `AnalyzeResponse(success=False, error=...)`
- Analysis failures (computation errors) → `AnalyzeResponse(success=False, error=...)`
- Upload failures (S3 write error) → `AnalyzeResponse(success=False, error=...)`
- API Server post failures → logged as warning, does not fail the step (result is still in S3)
- The service never crashes on a single request failure — errors are captured and returned in the response

## Configuration (Environment Variables)

| Variable | Description | Default |
|---|---|---|
| `MINIO_ENDPOINT` | MinIO endpoint (e.g., `minio:9000`) | `localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `API_SERVER_URL` | API Server base URL | `http://localhost:8000` |
| `OUTPUT_BUCKET_DESCRIPTIVE_STATISTICS` | Output bucket for descriptive statistics detail parquet | `descriptive-statistics-results` |
| `OUTPUT_BUCKET_DATA_CLEANING` | Output bucket for cleaned data parquet | `cleaned-data` |
| `OUTPUT_BUCKET_TEMPORAL_ANALYSIS` | Output bucket for temporal analysis detail parquet | `temporal-analysis-results` |
| `OUTPUT_BUCKET_GEOSPATIAL_ANALYSIS` | Output bucket for geospatial analysis detail parquet | `geospatial-analysis-results` |
| `OUTPUT_BUCKET_FARE_REVENUE_ANALYSIS` | Output bucket for fare revenue analysis detail parquet | `fare-revenue-analysis-results` |
| `SERVER_HOST` | FastAPI server host | `0.0.0.0` |
| `SERVER_PORT` | FastAPI server port | `8002` |

## Startup Behavior
1. Load configuration from environment variables
2. Start FastAPI server (stateless — no schema init, no DB connection)

## Tech Stack
- Python 3.12, uv
- FastAPI + Pydantic + pydantic-settings
- Polars (dataframe operations)
- NumPy (percentiles, histograms, correlation, FFT)
- SciPy (skewness, kurtosis, Z-score)
- scikit-learn (Isolation Forest, DBSCAN, K-means, linear regression)
- PyArrow (parquet read/write)
- httpx (HTTP client for API Server)
- boto3 (S3/MinIO operations via `src/utilities/s3.py`)
- MinIO (local S3)
