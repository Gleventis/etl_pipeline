# Aggregator

## Overview
The aggregator is a stateless service that combines and reshapes analytical results stored in Postgres. It queries the API Server's existing read endpoints, aggregates the JSONB summaries in-memory, and returns JSON responses. It does not access S3 or the database directly.

## Data Source
- **API Server** (`GET /analytical-results`, `GET /job-executions`, `GET /metrics/*`)
- All data fetched over HTTP — no direct DB or S3 access

## API

### Health

#### `GET /health`
Returns service health status.

**Response:** `200 OK`
```json
{"status": "ok"}
```

### Aggregation Endpoints

All endpoints are `GET`, stateless, and return JSON. Filters are passed as query parameters.

**Common query parameters** (all optional):
- `taxi_type`: Filter to a specific taxi type (`yellow`, `green`, `fhv`, `fhvhv`)
- `start_year`: Start year (e.g., `2022`)
- `start_month`: Start month (e.g., `01`)
- `end_year`: End year
- `end_month`: End month

Date range filtering uses `start_year`/`start_month` and `end_year`/`end_month` because the API Server's `GET /analytical-results` filters by `year` and `month` extracted from the object name — not by arbitrary timestamps.

---

#### `GET /aggregations/descriptive-stats`
Aggregates descriptive statistics across all files matching the filters.

**Additional query params:** None.

**Internal flow:**
1. Call API Server `GET /analytical-results?result_type=descriptive_statistics` with taxi_type/year/month filters
2. Paginate through all results (loop with offset until all fetched)
3. Extract `summary_data` from each result
4. Compute cross-file aggregates: mean of means, min of mins, max of maxes, averaged percentiles, total row counts

**Response:** `200 OK`
```json
{
  "file_count": 12,
  "total_rows": 29567172,
  "aggregated_stats": {
    "avg_fare": {
      "mean": 13.52,
      "min": 8.21,
      "max": 18.73,
      "percentiles": {"p50": 12.5, "p95": 45.0}
    }
  },
  "filters_applied": {
    "taxi_type": "yellow",
    "start_year": "2022",
    "start_month": "01",
    "end_year": "2022",
    "end_month": "12"
  }
}
```

---

#### `GET /aggregations/taxi-comparison`
Compares key metrics between taxi types. Ignores `taxi_type` filter (compares all types).

**Additional query params:** None.

**Internal flow:**
1. For each taxi type (`yellow`, `green`, `fhv`, `fhvhv`):
   - Call API Server `GET /analytical-results?result_type=descriptive_statistics&taxi_type=<type>` with year/month filters
   - Paginate through all results
2. Extract and average key metrics per taxi type from `summary_data`
3. Return side-by-side comparison

**Response:** `200 OK`
```json
{
  "comparison": {
    "yellow": {
      "file_count": 12,
      "total_rows": 29567172,
      "avg_fare": 13.52,
      "avg_trip_distance": 3.2,
      "avg_tip_percentage": 18.5
    },
    "green": {
      "file_count": 12,
      "total_rows": 1234567,
      "avg_fare": 12.10,
      "avg_trip_distance": 4.1,
      "avg_tip_percentage": 16.2
    },
    "fhv": {
      "file_count": 12,
      "total_rows": 5678901,
      "avg_fare": null,
      "avg_trip_distance": null,
      "avg_tip_percentage": null
    },
    "fhvhv": {
      "file_count": 12,
      "total_rows": 8901234,
      "avg_fare": 25.30,
      "avg_trip_distance": 5.7,
      "avg_tip_percentage": 12.1
    }
  },
  "filters_applied": {
    "start_year": "2022",
    "start_month": "01",
    "end_year": "2022",
    "end_month": "12"
  }
}
```

Note: `fhv` has no fare data (see analyzer spec § Taxi-Type Variations), so fare-related fields are `null`.

---

#### `GET /aggregations/temporal-patterns`
Aggregates hourly/daily trip volume patterns across files.

**Additional query params:** None.

**Internal flow:**
1. Call API Server `GET /analytical-results?result_type=temporal_analysis` with taxi_type/year/month filters
2. Paginate through all results
3. Extract temporal patterns from `summary_data` (peak hours, hourly volumes)
4. Average hourly volumes across files, identify overall peak hours

**Response:** `200 OK`
```json
{
  "file_count": 12,
  "hourly_avg_trips": {
    "0": 1523.4,
    "1": 987.2,
    "...": "...",
    "23": 2103.8
  },
  "peak_hours": [17, 18, 19],
  "daily_avg_trips": {
    "monday": 45231.0,
    "tuesday": 47892.0,
    "...": "..."
  },
  "filters_applied": {
    "taxi_type": "yellow",
    "start_year": "2022",
    "start_month": "01",
    "end_year": "2022",
    "end_month": "12"
  }
}
```

---

#### `GET /aggregations/data-quality`
Summarizes data quality and cleaning metrics across files.

**Additional query params:** None.

**Internal flow:**
1. Call API Server `GET /analytical-results?result_type=data_cleaning` with taxi_type/year/month filters
2. Paginate through all results
3. Extract cleaning metadata from `summary_data` (outlier counts per method, removal stats, quality rule violations)
4. Sum counts, compute overall rates

**Response:** `200 OK`
```json
{
  "file_count": 12,
  "total_rows_processed": 29567172,
  "outlier_summary": {
    "iqr": {"total_outliers": 234567, "avg_rate_percent": 0.79},
    "zscore": {"total_outliers": 198432, "avg_rate_percent": 0.67},
    "isolation_forest": {"total_outliers": 312456, "avg_rate_percent": 1.06}
  },
  "quality_violations": {
    "negative_fares": 1234,
    "zero_distances": 5678,
    "impossible_durations": 890
  },
  "overall_removal_rate_percent": 1.2,
  "filters_applied": {
    "taxi_type": "yellow",
    "start_year": "2022",
    "start_month": "01",
    "end_year": "2022",
    "end_month": "12"
  }
}
```

---

#### `GET /aggregations/pipeline-performance`
Reports computation time per analytical step per file, with averages. This is the thesis-critical endpoint for demonstrating checkpointing value.

**Additional query params:**
- `analytical_step`: Filter to a specific step (`descriptive_statistics`, `data_cleaning`, `temporal_analysis`, `geospatial_analysis`, `fare_revenue_analysis`)

**Internal flow:**
1. Call API Server `GET /analytical-results` with result_type (if `analytical_step` provided), taxi_type, year/month filters
2. Paginate through all results
3. Group by `result_type`, compute per-step averages and totals of `computation_time_seconds`
4. Also call API Server `GET /metrics/pipeline-summary` for overall checkpoint savings

**Response:** `200 OK`
```json
{
  "file_count": 12,
  "steps": {
    "descriptive_statistics": {
      "files_processed": 12,
      "avg_computation_seconds": 45.2,
      "total_computation_seconds": 542.4,
      "min_computation_seconds": 31.0,
      "max_computation_seconds": 58.7
    },
    "data_cleaning": {
      "files_processed": 12,
      "avg_computation_seconds": 67.8,
      "total_computation_seconds": 813.6,
      "min_computation_seconds": 45.1,
      "max_computation_seconds": 89.2
    }
  },
  "total_computation_seconds": 3456.7,
  "avg_computation_per_file_seconds": 288.1,
  "pipeline_summary": {
    "total_hours_saved_by_checkpointing": 1.25,
    "percent_time_saved": 3.9
  },
  "filters_applied": {
    "taxi_type": "yellow",
    "analytical_step": null,
    "start_year": "2022",
    "start_month": "01",
    "end_year": "2022",
    "end_month": "12"
  }
}
```

---

## Error Handling

- `200 OK`: Successful aggregation (even if 0 results — returns empty/zero aggregates)
- `422 Unprocessable Entity`: Invalid query parameter values
- `502 Bad Gateway`: API Server unreachable or returned an error

Error format:
```json
{
  "error": "Bad Gateway",
  "detail": "API Server unreachable at http://api_server:8000",
  "status_code": 502
}
```

## Configuration (Environment Variables)

| Variable | Description | Default |
|---|---|---|
| `API_SERVER_URL` | API Server base URL | `http://localhost:8000` |
| `SERVER_HOST` | FastAPI server host | `0.0.0.0` |
| `SERVER_PORT` | FastAPI server port | `8003` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `REQUEST_TIMEOUT` | Timeout for API Server calls (seconds) | `30.0` |

## Startup Behavior
1. Load configuration from environment variables
2. Start FastAPI server (stateless — no DB, no schema init)

## Tech Stack
- Python 3.12, uv
- FastAPI + Pydantic + pydantic-settings
- httpx (HTTP client for API Server calls)

## API Server Dependency

The aggregator depends on these existing API Server endpoints (see [api_server.md](api_server.md)):

| Aggregator endpoint | API Server endpoint(s) used |
|---|---|
| `GET /aggregations/descriptive-stats` | `GET /analytical-results?result_type=descriptive_statistics` |
| `GET /aggregations/taxi-comparison` | `GET /analytical-results?result_type=descriptive_statistics&taxi_type=<type>` (×4 taxi types) |
| `GET /aggregations/temporal-patterns` | `GET /analytical-results?result_type=temporal_analysis` |
| `GET /aggregations/data-quality` | `GET /analytical-results?result_type=data_cleaning` |
| `GET /aggregations/pipeline-performance` | `GET /analytical-results` + `GET /metrics/pipeline-summary` |

All required filtering (`result_type`, `taxi_type`, `year`, `month`, pagination) is already supported by the API Server's `GET /analytical-results` endpoint.
