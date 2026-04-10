# Branching Pipeline Variant — Implementation Plan

- **Git parent commit**: `23119363fb26f5bdf8c3bc70aec9ff765786862f`
- **Spec reference**: `specs/branching_pipeline_variant.md`

---

## Phase 1: Xtext Grammar

- [x] Task 1: Add optional `after` dependency list to the `Step` rule — **~10% context used**
  - **Action**: `refactor` — `src/cflDSL.xtext`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — DSL `after` field
  - **Verify**: Xtext grammar parses a step with `after: [step1, step2]` without errors; existing steps without `after` still parse
  - **Commit**: `b2189d0` — 2026-04-10

- [x] Task 2: Remove ETL action types and keep only analytical step types in the `ActionTypes` enum — **~10% context used**
  - **Action**: `refactor` — `src/cflDSL.xtext`
  - **Reference**: `specs/branching_pipeline_variant.md` § Data Sources — DSL input file
  - **Verify**: `ActionTypes` enum contains exactly 5 values: `DESCRIPTIVE_STATISTICS`, `DATA_CLEANING`, `TEMPORAL_ANALYSIS`, `GEOSPATIAL_ANALYSIS`, `FARE_REVENUE_ANALYSIS`; all 12 ETL action types (`EXTRACT_LOG_DATA`, `CREATE_DF`, etc.) are removed
  - **Commit**: `e175759` — 2026-04-10

---

## Phase 2: Xtext Validation

- [x] Task 3: Add validation rule — cycle detection in step dependency graph — **~25% context used**
  - **Action**: `refactor` — `src/translator/src/services/parser.py` (adapted from Xtext validator — no Eclipse project exists)
  - **Reference**: `specs/branching_pipeline_variant.md` § Validation — `after` graph must be acyclic
  - **Verify**: A DSL with a cycle (e.g., A after B, B after A) produces a ValueError; 9 tests cover direct/indirect/self-loop cycles, diamond DAG, and parse_dsl integration
  - **Commit**: `98ad9d3` — 2026-04-10

- [x] Task 4: Add validation rule — all `after` names must reference existing step names — **~25% context used**
  - **Action**: `refactor` — `src/translator/src/services/parser.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Validation — `after` step names
  - **Verify**: A DSL referencing an undefined step name in `after` produces a validation error; 6 tests cover valid refs, undefined ref, multiple deps with one undefined, no deps, empty steps, and parse_dsl integration
  - **Commit**: `9391c2a` — 2026-04-10

- [x] Task 5: Add validation rule — at least one entry point (step with no `after`) — **~25% context used**
  - **Action**: `refactor` — `src/translator/src/services/parser.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Validation — Entry point
  - **Verify**: A DSL where all steps have `after` produces a validation error; 6 tests cover valid DAG, all-deps-raises, single step, self-dep, empty steps, parse_dsl integration
  - **Commit**: `96c0b64` — 2026-04-10

- [x] Task 6: Add validation rule — at least one exit point (step not referenced by any `after`) — **~15% context used**
  - **Action**: `refactor` — `src/translator/src/services/parser.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Validation — Exit point
  - **Verify**: A DSL where every step is depended on by another produces a validation error; 6 tests cover valid DAG, all-referenced raises, single step, diamond DAG, empty steps, parse_dsl integration
  - **Commit**: `ba690da` — 2026-04-10

---

## Phase 3: Xtend Code Generator

- [x] Task 7: Create Python generator skeleton that reads the parsed DSL model — **~25% context used**
  - **Action**: `create` — `src/translator/src/services/generator.py` (adapted from Xtend — no Eclipse project exists)
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 2
  - **Verify**: Generator runs without error on a minimal valid DSL model; produces an empty or stub output file
  - **Commit**: `71d09c1` — 2026-04-10

- [x] Task 8: Implement topological sort for step ordering in the generator — **~25% context used**
  - **Action**: `refactor` — `src/translator/src/services/generator.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 2
  - **Verify**: Generator orders steps correctly for the example DAG (Descriptive Stats → Data Cleaning → [Geospatial, Temporal] → Fare Revenue)
  - **Commit**: `c8608c1` — 2026-04-10

- [x] Task 9: Implement JSON output generation with steps, dependencies, and checkpoint config — **~15% context used**
  - **Action**: `refactor` — Xtend generator file (Task 7 artifact)
  - **Reference**: `specs/branching_pipeline_variant.md` § Data Sources — Generated JSON pipeline definition
  - **Verify**: Generated JSON matches the specified output format; `after` lists are correct; `checkpoint` field is present per step
  - **Note**: Already implemented as part of Task 8 (tightly coupled — topological sort + JSON output). `generate()` returns `{"steps": [{"name", "action", "checkpoint", "after"}]}` in topological order. 11 tests pass including `test_output_includes_all_fields`.
  - **Commit**: `c8608c1` — 2026-04-10 (same as Task 8)

---

## Phase 4: API Server — DAG Storage

- [x] Task 10: Create `StepDependencies` SQLAlchemy model — **~20% context used**
  - **Action**: `refactor` — `src/api_server/src/services/database.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § API & Endpoints — POST /api/step-dependencies
  - **Verify**: `StepDependencies` table is created on migration; columns `id`, `pipeline_run_id`, `step_name`, `depends_on_step_name`, `created_at` exist
  - **Commit**: `eb0c1e0` — 2026-04-10

- [x] Task 11: Create `POST /api/step-dependencies` endpoint — **~35% context used**
  - **Action**: `refactor` — `src/api_server/src/server/` (NOTE: locate router file — TBD)
  - **Reference**: `specs/branching_pipeline_variant.md` § API & Endpoints — POST /api/step-dependencies
  - **Verify**: POST with valid payload returns 200 and inserts rows into `step_dependencies`; POST with invalid payload returns 422
  - **Commit**: `5e77628` — 2026-04-10

- [x] Task 12: Create `GET /api/step-dependencies/{pipeline_run_id}` endpoint — **~35% context used**
  - **Action**: `refactor` — `src/api_server/src/server/` (NOTE: same router file as Task 11)
  - **Reference**: `specs/branching_pipeline_variant.md` § API & Endpoints — GET /api/step-dependencies/{pipeline_run_id}
  - **Verify**: GET returns correct edges for a known `pipeline_run_id`; returns 404 for unknown ID
  - **Commit**: `980c85a` — 2026-04-10

---

## Phase 5: Translator Service

- [x] Task 13: Add `StepDefinition` Pydantic model — **~15% context used**
  - **Action**: `refactor` — `src/translator/src/services/parser.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 4
  - **Verify**: `StepDefinition(name="x", action="DESCRIPTIVE_STATISTICS", checkpoint=True, after=[])` instantiates without error
  - **Commit**: `9cf0e75` — 2026-04-10

- [x] Task 14: Extend `AnalyzeCommand` with optional `steps` field — **~15% context used**
  - **Action**: `refactor` — `src/translator/src/services/parser.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 4
  - **Verify**: `AnalyzeCommand` accepts `steps=None` and `steps=[StepDefinition(...)]` without error
  - **Commit**: `80b6831` — 2026-04-10

- [x] Task 15: Update `parse_dsl` to parse the `steps` list from generated JSON — **~25% context used**
  - **Action**: `verify` — `src/translator/src/services/parser.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 4
  - **Verify**: `parse_dsl` correctly populates `AnalyzeCommand.steps` from a JSON payload containing a `steps` array; returns `steps=None` when field is absent
  - **Note**: No code changes needed — `parse_dsl` uses `AnalyzeCommand(**raw["analyze"])` which handles `steps` automatically via Pydantic since Tasks 13-14 added the models. Existing tests `test_analyze_with_steps` and `test_analyze_defaults_steps_to_none` already verify this.
  - **Commit**: `7e85678` — 2026-04-10

- [x] Task 16: Update `call_scheduler` to pass `steps` field to scheduler — **~25% context used**
  - **Action**: `verify` — `src/translator/src/services/http_client.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 5
  - **Verify**: HTTP request body sent to scheduler includes `steps` when present; sends `null` when `None`
  - **Note**: No code changes needed — `call_scheduler` uses `cmd.model_dump()` which serializes `steps` automatically. Added `test_sends_steps_to_scheduler` and `test_sends_null_steps_when_absent` tests to verify.
  - **Commit**: `7e85678` — 2026-04-10

---

## Phase 6: Scheduler — Models & DAG Utilities

- [x] Task 17: Extend `ScheduleRequest` with optional `steps` field — **~15% context used**
  - **Action**: `refactor` — `src/scheduler/src/server/models.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 6
  - **Verify**: `ScheduleRequest` accepts `steps=None` and `steps=[...]` without error
  - **Commit**: `ad977cf` — 2026-04-09

- [x] Task 18: Create `dag.py` module with topological sort utility — **~15% context used**
  - **Action**: `create` — `src/scheduler/src/services/dag.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 8
  - **Verify**: Topological sort returns correct order for the example DAG; raises on cycle
  - **Commit**: `cfad169` — 2026-04-09

- [x] Task 19: Add `get_ready_steps(completed_steps, dag_edges)` to `dag.py` — **~15% context used**
  - **Action**: `refactor` — `src/scheduler/src/services/dag.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Glossary — `get_ready_steps`
  - **Verify**: Returns only steps whose all dependencies appear in `completed_steps`; returns entry points when `completed_steps` is empty
  - **Commit**: `34e0d23` — 2026-04-09

---

## Phase 7: Scheduler — DAG-Aware Flow Execution

- [x] Task 20: Update `process_file_flow` to accept DAG structure — **~30% context used**
  - **Action**: `refactor` — `src/scheduler/src/services/prefect_flows.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 8
  - **Verify**: Flow accepts optional `steps` parameter; existing linear flow still works when `steps` is `None`
  - **Commit**: `893c3be` — 2026-04-09

- [x] Task 21: Implement parallel step execution using Prefect `wait_for` — **~45% context used**
  - **Action**: `refactor` — `src/scheduler/src/services/prefect_flows.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 9
  - **Verify**: Independent steps (e.g., 4a and 4b) are submitted concurrently; Step 5 does not start until both complete
  - **Commit**: `179ecc9` — 2026-04-10

- [x] Task 22: Persist DAG edges to API server at pipeline start — **~45% context used**
  - **Action**: `refactor` — `src/scheduler/src/services/prefect_flows.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § API & Endpoints — POST /api/step-dependencies
  - **Verify**: On flow start, edges are POSTed to `/api/step-dependencies`; flow proceeds even if POST fails (NOTE: failure behavior TBD)
  - **Commit**: `27642a8` — 2026-04-10

- [x] Task 23: Update `schedule_batch` to pass DAG to flow — **~25% context used**
  - **Action**: `refactor` — `src/scheduler/src/services/scheduler.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 8
  - **Verify**: `schedule_batch` forwards `steps` from `ScheduleRequest` to `process_file_flow`
  - **Commit**: `3b7196c` — 2026-04-10

---

## Phase 8: Scheduler — DAG-Aware Resume

- [x] Task 24: Add `get_incomplete_with_dependents(completed_steps, dag_edges)` to `dag.py` — **~20% context used**
  - **Action**: `refactor` — `src/scheduler/src/services/dag.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Glossary — `get_incomplete_with_dependents`
  - **Verify**: Given completed=[4a], all_steps=[4a,4b,5], returns [4b, 5] (4b incomplete + 5 downstream of 4b)
  - **Commit**: `3ae0f42` — 2026-04-09

- [x] Task 25: Update `resume_failed` to use DAG-aware incomplete step computation — **~45% context used**
  - **Action**: `refactor` — `src/scheduler/src/services/scheduler.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — step 11
  - **Verify**: Resume re-runs only 4b and 5 when 4a has completed; does not re-run 4a
  - **Commit**: `7a24ca1` — 2026-04-10

- [x] Task 26: Update `save_job_state` to store DAG edges alongside job state — **~45% context used**
  - **Action**: `refactor` — `src/scheduler/src/services/database.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Data Sources — `job_state` table
  - **Verify**: `job_state` record includes DAG edges (or reference to `step_dependencies` table) sufficient for resume
  - **Commit**: `7a24ca1` — 2026-04-10

---

## Phase 9: Backward Compatibility

- [x] Task 27: Ensure `process_file_flow` falls back to linear `STEPS` when no DAG provided — **~25% context used**
  - **Action**: `verify` — `src/scheduler/src/services/prefect_flows.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — Fallback (no DAG)
  - **Verify**: Existing linear pipeline test passes unchanged with `steps=None`
  - **Note**: No code changes needed — fallback already implemented in Tasks 20-23. All 22 tests pass (11 linear + 11 DAG including `test_dag_none_falls_back_to_linear`).
  - **Commit**: verification only — 2026-04-10

- [x] Task 28: Ensure `parse_dsl` accepts existing JSON format without `steps` field — **~25% context used**
  - **Action**: `verify` — `src/translator/src/services/parser.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — Fallback (no DAG)
  - **Verify**: Existing translator tests pass unchanged; `steps` defaults to `None` when absent from payload
  - **Note**: No code changes needed — `AnalyzeCommand.steps` defaults to `None`. Existing test `test_analyze_defaults_steps_to_none` verifies this. All 93 translator tests pass.
  - **Commit**: `7e85678` — 2026-04-10

---

## Phase 10: Unit Tests

- [x] Task 29: Unit tests for topological sort in `dag.py` — **~15% context used**
  - **Action**: `create` — `src/scheduler/tests/test_dag_topo_sort.py`
  - **Reference**: `src/scheduler/src/services/dag.py` — topological sort utility
  - **Verify**: Tests cover correct ordering, cycle detection (raises), and single-node graph
  - **Commit**: `cfad169` — 2026-04-09

- [x] Task 30: Unit tests for cycle detection in `dag.py` — **~15% context used**
  - **Action**: `create` — `src/scheduler/tests/test_dag_cycle_detection.py`
  - **Reference**: `src/scheduler/src/services/dag.py` — topological sort utility
  - **Verify**: Tests cover direct cycle (A→B→A), self-loop (A→A), and valid acyclic graph (no raise)
  - **Commit**: `d6b9dce` — 2026-04-10

- [x] Task 31: Unit tests for `get_ready_steps` — **~15% context used**
  - **Action**: `create` — `src/scheduler/tests/test_dag_ready_steps.py`
  - **Reference**: `src/scheduler/src/services/dag.py` — `get_ready_steps`
  - **Verify**: Tests cover empty completed set (returns entry points), partial completion, and all-complete (returns empty)
  - **Commit**: `34e0d23` — 2026-04-09

- [x] Task 32: Unit tests for `get_incomplete_with_dependents` — **~20% context used**
  - **Action**: `create` — `src/scheduler/tests/test_dag_resume.py`
  - **Reference**: `src/scheduler/src/services/dag.py` — `get_incomplete_with_dependents`
  - **Verify**: Tests cover partial branch failure (4a done, 4b failed → returns [4b, 5]) and full failure (returns all steps)
  - **Commit**: `3ae0f42` — 2026-04-09

- [x] Task 33: Unit tests for Xtext validation rules — **~15% context used**
  - **Action**: `create` — Xtext test class (NOTE: path TBD — locate in Eclipse test project)
  - **Reference**: `specs/branching_pipeline_variant.md` § Validation
  - **Verify**: Tests cover cycle, unknown reference, missing entry point, and missing exit point — each produces the expected validation error
  - **Note**: Already implemented in `src/translator/tests/test_parser.py` (Tasks 3-6). Validation was done in Python since no Eclipse project exists. 27 tests across 4 test classes: `TestValidateNoCycles` (9), `TestValidateAfterReferences` (6), `TestValidateHasEntryPoint` (6), `TestValidateHasExitPoint` (6). All pass.
  - **Commit**: covered by commits `98ad9d3`, `9391c2a`, `96c0b64`, `ba690da` — 2026-04-10

---

## Phase 11: Integration Tests

- [x] Task 34: Integration test — full branching pipeline execution (parallel steps complete) — **~45% context used**
  - **Action**: `create` — `src/scheduler/tests/integration/test_branching_pipeline.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — Pipeline Shape example
  - **Verify**: All 5 steps complete; Steps 4a and 4b ran concurrently (verifiable via timestamps or Prefect run metadata)
  - **Commit**: `735a0ca` — 2026-04-10

- [x] Task 35: Integration test — partial branch failure and DAG-aware resume — **~45% context used**
  - **Action**: `create` — `src/scheduler/tests/integration/test_branching_resume.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Error Handling — Partial branch failure
  - **Verify**: After 4b fails, resume re-runs only 4b and 5; 4a is not re-run; final state shows all steps complete
  - **Commit**: `8d85fab` — 2026-04-10

- [x] Task 36: Integration test — backward compatibility with linear pipeline (no DAG) — **~20% context used**
  - **Action**: `create` — `src/scheduler/tests/integration/test_linear_compat.py`
  - **Reference**: `specs/branching_pipeline_variant.md` § Processing Flow — Fallback (no DAG)
  - **Verify**: Existing linear pipeline executes correctly end-to-end with `steps=None`; no regressions
  - **Commit**: `1b4bfb0` — 2026-04-10
