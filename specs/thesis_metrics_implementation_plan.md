# Thesis Metrics Implementation Plan

Add missing metrics endpoints from `thesis_metrics.md` to the API server.

## Step-Level Performance (Metric #6)
- [x] Step 1: Add `StepPerformanceStatistic` and `StepPerformanceResponse` models, `calculate_step_performance` function, `GET /metrics/step-performance` route, and tests — **~45% context used**
  - Verify: 159 api_server tests pass ✅

## Pipeline Efficiency (Metric #5)
- [x] Step 2: Add `PipelineEfficiencyStatistic` and `PipelineEfficiencyResponse` models — **~10% context used**
- [x] Step 3: Add `calculate_pipeline_efficiency` function to `metrics.py` — **~10% context used**
- [x] Step 4: Add `GET /metrics/pipeline-efficiency` route to `routes.py` — **~10% context used**
- [x] Step 5: Add tests for `calculate_pipeline_efficiency` in `test_metrics.py` — **~45% context used**
  - Tests: empty result, single file, multiple status groups, averages across files, excludes zero-elapsed
  - Verify: 164 api_server tests pass (was 159, +5 new) ✅
- [x] Step 6: Add route tests for `GET /metrics/pipeline-efficiency` in `test_routes.py` — **~40% context used**
  - Tests: empty result, single file efficiency ratio, groups by status, excludes zero elapsed
  - Verify: 168 api_server tests pass (was 164, +4 new) ✅

## Average Recovery Time Improvement (Metric #2)
- [x] Step 7: Add `RecoveryTimeResponse` model — **~10% context used**
  - Fields: `avg_recovery_with_checkpoint_seconds`, `avg_recovery_without_checkpoint_seconds`, `avg_time_saved_seconds`, `percent_improvement`
  - Verify: 168 api_server tests pass (no regressions) ✅
- [x] Step 8: Add `calculate_recovery_time_improvement` function to `metrics.py` — **~10% context used**
  - Queries files with retry_count > 0 and overall_status == 'completed'
  - For each: sums computation_time_seconds where retry_count > 0 (with checkpoint) vs total_computation_seconds (without)
  - Returns averages and percent_improvement matching RecoveryTimeResponse
  - Verify: 168 api_server tests pass (no regressions) ✅
- [x] Step 9: Add `GET /metrics/recovery-time` route to `routes.py` — **~10% context used**
  - Imported `calculate_recovery_time_improvement` and `RecoveryTimeResponse`
  - Added route handler following existing pattern (e.g., `get_pipeline_efficiency`)
  - Verify: 168 api_server tests pass (no regressions) ✅
- [x] Step 10: Add tests for `calculate_recovery_time_improvement` in `test_metrics.py` — **~40% context used**
  - Tests: empty result, excludes non-completed files, single file with retries, averages across multiple files, edge case with no retry job executions
  - Verify: 173 api_server tests pass (was 168, +5 new) ✅
- [x] Step 11: Add route tests for `GET /metrics/recovery-time` in `test_routes.py` — **~40% context used**
  - Tests: empty result returns zeros, single file with retries, excludes non-completed files, averages across multiple files
  - Note: POST /job-executions doesn't accept computation_time_seconds — must PATCH after creation
  - Verify: 177 api_server tests pass (was 173, +4 new) ✅
