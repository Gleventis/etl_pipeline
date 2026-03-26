## Overview
ETL pipeline that implements checkpoints between each step of the pipeline.

## Architecture
Micro-service based architecture, where each service is a server, to allow easier scalability, receiving HTTP requests. The following microservices are to be constructed.

### Services
1. Data collector
2. Scheduler
3. Trigger
4. Analyzer
5. Aggregator
6. API Server

### Data storage
1. Postgres
2. S3 storage solution

## Data Collector
### Overview
The data-collector on a high level is responsible for fetching available data from a data source.

### Tasks
1. Collect data from an external data source
2. Validate the data
3. Store it in an S3 bucket

## Scheduler
### Overview
The scheduler on a high level is responsible for scheduling the various analytic jobs across a pool of analyzers.

### Tasks
1. Receive rqeuest with bucket and filename
2. Based on the state of the pipeline forward it to the next available analyzer
3. Collect the results
4. Update the database via the writer

## Trigger (Removed)
The trigger service has been removed. The data collector now calls the scheduler directly after downloading files, passing the list of uploaded object paths and bucket.

## Analyzer
### Overview
The analyzer is responsible for the various analytical jobs that are to be performed on the data. Each analytical step is computationally intensive to demonstrate the value of checkpointing in the pipeline.

### Tasks
1. Expose endpoint for receiving a request with the following information:

    1.1 Analytical job

    1.2 Bucket and filename

2. Perform the analytics job
3. Write to the database or upload to the S3 bucket via the API Server
4. Return status back to Scheduler

### Analytical Steps

Each file goes through these computationally intensive steps sequentially:

1. **Descriptive Statistics** (~30-60s per file)
   - Compute detailed percentiles (1st, 5th, 10th, 25th, 50th, 75th, 90th, 95th, 99th) across all numeric columns
   - Calculate histograms with 100 bins per column
   - Compute correlation matrix between all numeric column pairs
   - Generate distribution statistics (skewness, kurtosis)
   - Output: Summary statistics in JSONB + detailed distributions in S3 parquet

2. **Data Cleaning** (~45-90s per file)
   - Run multiple outlier detection algorithms (IQR, Z-score, Isolation Forest)
   - Apply and compare multiple cleaning strategies
   - Validate data quality rules (negative fares, zero distances, impossible durations)
   - Generate cleaned parquet file and write to S3
   - Output: Cleaning metadata (outlier counts, removal stats) in JSONB + cleaned data in S3

3. **Temporal Analysis** (~60-120s per file)
   - Time-series decomposition (trend, seasonality, residuals)
   - Fourier transforms for frequency analysis
   - Rolling window statistics (hourly, daily, weekly patterns)
   - Peak hour detection and trip volume forecasting
   - Output: Temporal patterns and aggregates in JSONB + detailed time-series in S3

4. **Geospatial Analysis** (~90-180s per file)
   - Spatial clustering algorithms (DBSCAN, K-means on pickup/dropoff coordinates)
   - Route optimization and common path detection
   - Heatmap generation at multiple zoom levels
   - Distance distribution by geographic zone
   - Output: Hotspot summaries and cluster metadata in JSONB + spatial clusters in S3

5. **Fare Revenue Analysis** (~60-120s per file)
   - Revenue forecasting models (time-series prediction)
   - Anomaly detection in fare patterns
   - Tip prediction modeling (regression analysis)
   - Fare distribution analysis by zone, time, and distance
   - Surcharge and revenue breakdown calculations
   - Output: Revenue statistics and predictions in JSONB + detailed fare analysis in S3

### Output Format

Each analytical step produces two outputs:

1. **Summary results** → Stored in Postgres via API Server (JSONB format, ~10-50 KB per step)
   - Queryable aggregates and statistics
   - Computation time metrics for thesis evaluation
   - Key findings and metadata

2. **Detailed results** → Stored in S3 as parquet files (~1-10 MB per step)
   - Full distributions, clusters, time-series data
   - Intermediate computation artifacts
   - Detailed breakdowns for deep analysis

## Aggregator
### Overview
The aggregator is responsible for aggregating the results produced by the analyzer based on the needs each time.

### Tasks
1. Expose an endpoint that allows the user to define what type of data they want to aggregate
2. Fetch the data using the API Server
3. Transform the data to the needs of the client
4. Return the data back

## API Server
### Overview
The API Server is the service that sits in front of the Postgres database and performs reads and writes.

### Tasks
1. Expose endpoint to write data to the appropriate table.
2. Expose endpoint to read data from the appropriate table.

## Overall requirements
1. The language to be used is Python3.12
2. Use uv for managing packages
3. Each service should have a corresponding Dockerfile and a docker-compose to bring it up
4. Use TestContainers and pytest for testing
5. The structure of the repo is the following
<details>
<summary>project-root/</summary>

```text
project-root/
├── specs/
└── src/
    ├── infrastructure/   # docker-compose files
    │   └── <service_name>/
    │       └── docker-compose.yml
    ├── utilities/        # shared functionality across services
    ├── <service_name>/
    │   ├── pyproject.toml
    │   ├── src/
    │   │   ├── server/
    │   │   └── services/
    │   └── tests/
    └── ... (more services)
</details>
```
6. The S3 solution to be used locally is MinIO.

## Features

**Data collection**: NYC TLC data, concurrent downloads, schema validation, MinIO storage. [Spec](data_collection.md) | [Plan](data_collection_implementation_plan.md)
**Scheduling jobs**: Pipeline orchestration, sequential analytical steps, concurrent file processing, job state tracking with resume capability. [Spec](scheduler.md) | [Plan](scheduler_implementation_plan.md)
**Scheduler Prefect refactor**: Replace custom thread pool orchestration with Prefect flows/tasks, self-hosted Prefect server, per-file flow runs, Postgres audit trail retained. [Spec](scheduler_prefect_refactor.md) | [Plan](scheduler_prefect_refactor_implementation_plan.md)
**API Server**: REST API for all database operations. Three tables (files, job_executions, analytical_results), CRUD endpoints, complex filtering, batch inserts, metrics endpoints for thesis reporting. SQLAlchemy ORM, schema initialization on startup. [Spec](api_server.md) | [Plan](api_server_implementation_plan.md)
**Analyzer**: Computationally intensive analytical processing of NYC TLC data. Five sequential steps (descriptive statistics, data cleaning, temporal analysis, geospatial analysis, fare revenue analysis), per-taxi-type implementations, dual output to Postgres (summary) and S3 (detail parquet). [Spec](analyzer.md) | [Plan](analyzer_implementation_plan.md)
**Scheduler-Analyzer Integration**: Refactor scheduler to work with analyzer's per-step endpoints. Add `taxi_type` extraction from object paths, `job_execution_id` creation via API Server, step-specific endpoint routing. [Plan](scheduler_analyzer_integration_plan.md)
**Aggregation**: Stateless service that aggregates analytical results via the API Server. Five fixed endpoints: cross-file descriptive stats, taxi type comparison, temporal patterns, data quality summary, pipeline performance metrics. [Spec](aggregator.md) | [Plan](aggregator_implementation_plan.md)
**End-to-End Compose**: Unified docker-compose that brings up all services on a shared network for full pipeline runs. Shared MinIO, two Postgres instances, Prefect server, all five application services with correct cross-service wiring. [Spec](end_to_end.md) | [Plan](end_to_end_implementation_plan.md)
**Thesis Metrics**: Additional metrics endpoints for thesis evaluation — step-level performance, pipeline efficiency, recovery time improvement. [Spec](thesis_metrics.md) | [Plan](thesis_metrics_implementation_plan.md)
**Pipeline Metrics Tracking**: Wire scheduler to update job execution status/timing and file aggregates via API Server PATCH endpoints after each step, so thesis metrics return real data. [Plan](pipeline_metrics_tracking_plan.md)
**Scheduler Checkpoint Configuration**: Add optional checkpoint control to the scheduler API. New `skip_checkpoints` parameter on `ScheduleRequest` allows callers to disable state persistence for specific pipeline steps, enabling thesis experiments comparing pipeline recovery with and without checkpoints. [Spec](scheduler_checkpoint_config.md) | [Plan](scheduler_checkpoint_config_implementation_plan.md)
**Translator service**: DSL-to-HTTP translator. Parses operator DSL, calls data collector/scheduler/aggregator. Async execution with run ID polling, selective checkpoint disabling, independent section execution. [Spec](translator_service.md) | [Plan](translator_service_implementation_plan.md)

