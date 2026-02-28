# Thesis Metrics: Measuring Checkpoint Value

## Overview
This document defines how to track and measure the value of checkpointing in the ETL pipeline. The metrics demonstrate that checkpointing improves recovery speed and resource efficiency in computationally intensive workflows.

## Database Schema for Metrics

### `files` Table - Aggregated Metrics
```sql
CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    bucket TEXT NOT NULL,
    object_name TEXT NOT NULL,
    overall_status TEXT NOT NULL DEFAULT 'pending',  -- pending, in_progress, completed, failed
    total_computation_seconds FLOAT DEFAULT 0,       -- sum of all completed job execution times
    total_elapsed_seconds FLOAT DEFAULT 0,           -- wall-clock time from start to finish
    retry_count INT DEFAULT 0,                       -- number of times this file was retried
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (bucket, object_name)
);
```

### `job_executions` Table - Granular Tracking
```sql
CREATE TABLE job_executions (
    id SERIAL PRIMARY KEY,
    file_id INT NOT NULL REFERENCES files(id),
    pipeline_run_id UUID NOT NULL,                   -- groups steps in one attempt
    step_name TEXT NOT NULL,                         -- descriptive_statistics, data_cleaning, etc.
    status TEXT NOT NULL DEFAULT 'pending',          -- pending, running, completed, failed
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    computation_time_seconds FLOAT,                  -- completed_at - started_at
    retry_count INT DEFAULT 0,                       -- 0 = first attempt, 1 = first retry, etc.
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_job_executions_file_id ON job_executions(file_id);
CREATE INDEX idx_job_executions_pipeline_run_id ON job_executions(pipeline_run_id);
CREATE INDEX idx_job_executions_status ON job_executions(status);
```

### `analytical_results` Table - Output Tracking
```sql
CREATE TABLE analytical_results (
    id SERIAL PRIMARY KEY,
    job_execution_id INT NOT NULL REFERENCES job_executions(id),
    result_type TEXT NOT NULL,                       -- matches step_name
    summary_data JSONB NOT NULL,                     -- queryable summaries and statistics
    detail_s3_path TEXT,                             -- path to detailed results in S3
    computation_time_seconds FLOAT NOT NULL,         -- for validation against job_executions
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_analytical_results_result_type ON analytical_results(result_type);
CREATE INDEX idx_analytical_results_job_execution_id ON analytical_results(job_execution_id);
```

## Key Thesis Metrics

### 1. Time Saved by Checkpointing
**Definition**: Computation time that would have been wasted without checkpointing (re-running already completed steps).

```sql
-- Per file that experienced failures
SELECT 
    f.id as file_id,
    f.object_name,
    -- Time saved: sum of completed steps from first attempt that didn't need re-running
    (SELECT SUM(computation_time_seconds) 
     FROM job_executions 
     WHERE file_id = f.id 
       AND status = 'completed' 
       AND retry_count = 0
    ) as time_saved_seconds,
    -- Total actual computation time (including retries)
    f.total_computation_seconds,
    -- Percentage saved
    ROUND(
        100.0 * (SELECT SUM(computation_time_seconds) 
                 FROM job_executions 
                 WHERE file_id = f.id AND status = 'completed' AND retry_count = 0)
        / NULLIF(f.total_computation_seconds, 0),
        2
    ) as percent_saved
FROM files f
WHERE f.retry_count > 0 AND f.overall_status = 'completed';
```

### 2. Average Recovery Time Improvement
**Definition**: Compare recovery time with checkpoints vs hypothetical full restart.

```sql
-- Average time to recover from failure
SELECT 
    AVG(time_with_checkpoint) as avg_recovery_with_checkpoint_seconds,
    AVG(time_without_checkpoint) as avg_recovery_without_checkpoint_seconds,
    AVG(time_without_checkpoint - time_with_checkpoint) as avg_time_saved_seconds,
    ROUND(
        100.0 * AVG(time_without_checkpoint - time_with_checkpoint) / AVG(time_without_checkpoint),
        2
    ) as percent_improvement
FROM (
    SELECT 
        f.id,
        -- Time with checkpoint: only retry failed and subsequent steps
        (SELECT SUM(computation_time_seconds) 
         FROM job_executions 
         WHERE file_id = f.id AND retry_count > 0) as time_with_checkpoint,
        -- Time without checkpoint: would re-run all steps
        f.total_computation_seconds as time_without_checkpoint
    FROM files f
    WHERE f.retry_count > 0 AND f.overall_status = 'completed'
) recovery_stats;
```

### 3. Failure and Retry Statistics
**Definition**: Which steps fail most often and require retries.

```sql
-- Failure rate per step
SELECT 
    step_name,
    COUNT(DISTINCT file_id) as total_files_processed,
    COUNT(DISTINCT CASE WHEN status = 'failed' THEN file_id END) as files_that_failed,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN status = 'failed' THEN file_id END) 
        / COUNT(DISTINCT file_id),
        2
    ) as failure_rate_percent,
    AVG(CASE WHEN retry_count > 0 THEN retry_count END) as avg_retries_when_failed,
    AVG(computation_time_seconds) as avg_computation_seconds
FROM job_executions
GROUP BY step_name
ORDER BY failure_rate_percent DESC;
```

### 4. Total Resource Savings
**Definition**: Aggregate computation time saved across all files.

```sql
-- Total hours saved by checkpointing
SELECT 
    COUNT(*) as files_with_retries,
    SUM(time_saved_seconds) / 3600.0 as total_hours_saved,
    AVG(time_saved_seconds) / 60.0 as avg_minutes_saved_per_file,
    SUM(total_computation_seconds) / 3600.0 as total_computation_hours
FROM (
    SELECT 
        f.id,
        f.total_computation_seconds,
        (SELECT SUM(computation_time_seconds) 
         FROM job_executions 
         WHERE file_id = f.id AND status = 'completed' AND retry_count = 0
        ) as time_saved_seconds
    FROM files f
    WHERE f.retry_count > 0 AND f.overall_status = 'completed'
) savings;
```

### 5. Pipeline Efficiency
**Definition**: Ratio of actual computation time to wall-clock elapsed time.

```sql
-- Efficiency: how much time is spent computing vs waiting
SELECT 
    overall_status,
    COUNT(*) as file_count,
    AVG(total_computation_seconds / NULLIF(total_elapsed_seconds, 0)) as avg_efficiency_ratio,
    AVG(total_computation_seconds) / 60.0 as avg_computation_minutes,
    AVG(total_elapsed_seconds) / 60.0 as avg_elapsed_minutes
FROM files
WHERE total_elapsed_seconds > 0
GROUP BY overall_status;
```

### 6. Step-Level Performance
**Definition**: Average computation time per analytical step.

```sql
-- Performance breakdown by step
SELECT 
    step_name,
    COUNT(*) as executions,
    AVG(computation_time_seconds) as avg_seconds,
    MIN(computation_time_seconds) as min_seconds,
    MAX(computation_time_seconds) as max_seconds,
    STDDEV(computation_time_seconds) as stddev_seconds
FROM job_executions
WHERE status = 'completed'
GROUP BY step_name
ORDER BY avg_seconds DESC;
```

## Thesis Reporting

### Key Claims to Demonstrate

1. **Speed Improvement**: "Checkpointing reduced average recovery time by X% (Y minutes → Z minutes)"
2. **Resource Efficiency**: "Saved X hours of computation across Y files with Z% failure rate"
3. **Scalability**: "As pipeline complexity increases (5 steps, 5-10 min each), checkpoint value grows proportionally"
4. **Failure Resilience**: "X% of files required retries; checkpointing prevented full pipeline restarts"

### Sample Report Query

```sql
-- Comprehensive thesis summary
WITH file_stats AS (
    SELECT 
        COUNT(*) as total_files,
        COUNT(CASE WHEN retry_count > 0 THEN 1 END) as files_with_retries,
        AVG(total_computation_seconds) as avg_computation_seconds,
        SUM(total_computation_seconds) / 3600.0 as total_computation_hours
    FROM files
    WHERE overall_status = 'completed'
),
savings_stats AS (
    SELECT 
        SUM(time_saved) / 3600.0 as total_hours_saved,
        AVG(time_saved) / 60.0 as avg_minutes_saved_per_retry
    FROM (
        SELECT 
            f.id,
            (SELECT SUM(computation_time_seconds) 
             FROM job_executions 
             WHERE file_id = f.id AND status = 'completed' AND retry_count = 0
            ) as time_saved
        FROM files f
        WHERE f.retry_count > 0 AND f.overall_status = 'completed'
    ) s
)
SELECT 
    fs.total_files,
    fs.files_with_retries,
    ROUND(100.0 * fs.files_with_retries / fs.total_files, 2) as retry_rate_percent,
    ROUND(fs.avg_computation_seconds / 60.0, 2) as avg_computation_minutes_per_file,
    ROUND(fs.total_computation_hours, 2) as total_computation_hours,
    ROUND(ss.total_hours_saved, 2) as total_hours_saved_by_checkpointing,
    ROUND(ss.avg_minutes_saved_per_retry, 2) as avg_minutes_saved_per_retry,
    ROUND(100.0 * ss.total_hours_saved / fs.total_computation_hours, 2) as percent_time_saved
FROM file_stats fs, savings_stats ss;
```

## Implementation Notes

1. **Update `files` aggregates** after each `job_execution` completes:
   - Increment `total_computation_seconds`
   - Update `total_elapsed_seconds` (current_time - first started_at)
   - Increment `retry_count` if this is a retry

2. **Track `pipeline_run_id`** for each attempt:
   - Generate new UUID when starting/resuming a file
   - All steps in that attempt share the same `pipeline_run_id`

3. **Set `retry_count`** correctly:
   - First attempt: `retry_count = 0`
   - First retry: `retry_count = 1`
   - Increment for each subsequent retry of the same step

4. **Store `computation_time_seconds`** in both tables:
   - `job_executions`: per-step timing
   - `analytical_results`: validation (should match job_execution timing)
