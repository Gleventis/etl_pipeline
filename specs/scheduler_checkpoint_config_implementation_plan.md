# Scheduler Checkpoint Configuration — Implementation Plan

Spec: [scheduler_checkpoint_config.md](scheduler_checkpoint_config.md)

## Steps

- [x] Step 1: Add `validate_step_names()` function to `src/scheduler/src/services/pipeline.py` — **~15% context used**
  - Accepts a `list[str]`, returns list of invalid names by checking against `STEPS`
  - Source: `src/scheduler/src/services/pipeline.py` (contains `STEPS` list)

- [x] Step 2: Add `skip_checkpoints: list[str] = Field(default_factory=list)` to `ScheduleRequest` in `src/scheduler/src/server/models.py` — **~20% context used**
  - Source: `src/scheduler/src/server/models.py` (`ScheduleRequest` class)

- [x] Step 3: Add validation in `routes.py` — call `validate_step_names()` on `request.skip_checkpoints`, return 422 if invalid — **~15% context used**
  - Source: `src/scheduler/src/server/routes.py` (`schedule` function)
  - Source: `src/scheduler/src/services/pipeline.py` (`validate_step_names` from Step 1)

- [x] Step 4: Thread `skip_checkpoints` through `SchedulerService.schedule_batch()` to `_run_flows_concurrently()` to flow args — **~25% context used**
  - Source: `src/scheduler/src/services/scheduler.py` (`schedule_batch` and `_run_flows_concurrently` methods)

- [x] Step 5: Add `skip_checkpoints: list[str]` parameter to `process_file_flow()` in `prefect_flows.py` — **~25% context used**
  - Conditionally skip `save_job_state()` call after step completion when the just-completed step is in `skip_checkpoints`
  - Note: the `save_job_state` call on failure should ALWAYS run regardless of skip list
  - Source: `src/scheduler/src/services/prefect_flows.py` (`process_file_flow` function, the while loop's `save_job_state` call after `completed_steps.append`)

- [x] Step 6: Write tests for `validate_step_names()` — **~15% context used**
  - Test valid names return empty list, invalid names are returned, mixed input works
  - Already implemented in `tests/test_pipeline.py::TestValidateStepNames` (6 tests)

- [x] Step 7: Write test for `ScheduleRequest` with `skip_checkpoints` field — **~20% context used**
  - Test default is empty list, valid step names accepted, serialization works
  - Added 3 tests to `tests/test_models.py::TestScheduleRequest`

- [x] Step 8: Write route test — POST /scheduler/schedule with invalid `skip_checkpoints` returns 422 — **~15% context used**
  - Source: `src/scheduler/tests/test_routes.py`

- [x] Step 9: Write route test — POST /scheduler/schedule with valid `skip_checkpoints` returns 202 — **~15% context used**
  - Source: `src/scheduler/tests/test_routes.py`

- [x] Step 10: Write test for `process_file_flow` — verify `save_job_state` is skipped for steps in `skip_checkpoints` — **~20% context used**
  - Added 3 tests to `tests/test_prefect_flows.py::TestProcessFileFlow`:
    - `test_skip_checkpoints_skips_save_for_specified_steps` (skip 2 of 5 → 4 saves instead of 6)
    - `test_skip_checkpoints_still_saves_on_failure` (failure on skipped step still saves)
    - `test_skip_all_checkpoints_only_saves_initial` (skip all → only initial save)

- [x] Step 11: Run ruff check and format on all modified files — **~10% context used**
  - All 9 modified files pass `ruff check` and `ruff format` (0 issues, 0 reformats)
  - All 136 tests pass

- [x] Step 12: Update scheduler service README if it exists — **~5% context used**
  - Added `skip_checkpoints` description to the `/scheduler/schedule` endpoint row
  - Added new "Checkpoint Configuration" section with behavior explanation and example request body
