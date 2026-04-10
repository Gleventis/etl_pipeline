# Branching Pipeline Variant

## Overview

- Adds arbitrary DAG-based pipeline topology support to the existing linear pipeline.
- Steps declare explicit `after` dependencies, enabling parallel branches and fan-in/fan-out patterns.
- The `after` field is optional — when omitted, the pipeline falls back to linear ordering (backward compatible).
- Serves the thesis evaluation by enabling partial branch failure, recovery, and parallel speedup experiments.

## Dependencies & Prerequisites

- Existing linear pipeline must be functional (translator → scheduler → analyzer flow).
- Prefect must support `wait_for` task dependency injection (already available in current stack).
- API server database must be migrated to include the `StepDependencies` table before scheduler can persist DAG edges.
- Xtext/Xtend toolchain (Eclipse-based) must be available for grammar and generator changes.

## Data Sources

| Source | Type | Details |
|---|---|---|
| DSL input file | Xtext grammar | Parsed by `cflDSL.xtext`; step definitions with optional `after` lists. `ActionTypes` enum contains only analytical actions: `DESCRIPTIVE_STATISTICS`, `DATA_CLEANING`, `TEMPORAL_ANALYSIS`, `GEOSPATIAL_ANALYSIS`, `FARE_REVENUE_ANALYSIS` |
| Generated JSON pipeline definition | File / POST body | Produced by Xtend generator; consumed by translator service |
| `job_state` table | PostgreSQL JSONB | `src/scheduler/src/services/database.py`; tracks per-step completion |
| `step_dependencies` table | PostgreSQL | `src/api_server/src/services/database.py`; stores DAG edges per pipeline run |

## API & Endpoints

### `POST /api/step-dependencies`

- **Purpose**: Batch-insert DAG edges when a pipeline run starts.
- **Request body**: `{ "pipeline_run_id": "uuid", "edges": [{"step_name": "string", "depends_on_step_name": "string"}] }`
- **Response 200**: `{ "inserted": <count> }`
- **Response 422**: Validation error details.

### `GET /api/step-dependencies/{pipeline_run_id}`

- **Purpose**: Retrieve DAG edges for a given pipeline run (used during resume).
- **Response 200**: `{ "pipeline_run_id": "uuid", "edges": [...] }`
- **Response 404**: Pipeline run not found.

*N/A — no changes to existing endpoints; new endpoints are additive.*

## Processing Flow

### Pipeline Shape (example)

```
Step1 (Descriptive Stats) → Step2 (Data Cleaning) → fork
                                                      ├→ Step4a (Geospatial Analysis)  ─┬→ Step5 (Fare Revenue)
                                                      └→ Step4b (Temporal Analysis)     ─┘
```

- Steps 4a and 4b run in parallel.
- Step 5 waits for both (fan-in).

### End-to-End Flow

1. User authors a DSL file with optional `after` declarations on steps.
2. Xtend generator performs topological sort and emits a JSON pipeline definition.
3. JSON is POSTed to the translator service.
4. Translator parses `steps` list into `AnalyzeCommand` (via `src/translator/src/services/parser.py`).
5. Translator calls scheduler via `src/translator/src/services/http_client.py`, passing `steps` field.
6. Scheduler receives `ScheduleRequest` with optional `steps` (via `src/scheduler/src/server/models.py`).
7. Scheduler persists DAG edges to API server (`POST /api/step-dependencies`).
8. `process_file_flow` in `src/scheduler/src/services/prefect_flows.py` builds execution plan from DAG.
9. Independent steps are submitted in parallel using Prefect `wait_for`; convergence points wait for all dependencies.
10. Per-step completion is recorded in `job_state` JSONB (`src/scheduler/src/services/database.py`).
11. On failure, `resume_failed` in `src/scheduler/src/services/scheduler.py` fetches DAG edges, computes incomplete steps + downstream dependents, and re-runs only those.

### Fallback (no DAG)

- When `steps` is `None`, scheduler uses the existing linear `STEPS` list unchanged.

## Error Handling

| Error Condition | Behavior | User-facing message |
|---|---|---|
| Cycle detected in `after` graph | Xtext validation rejects DSL at authoring time | Validation error in IDE |
| Unknown step name in `after` list | Xtext validation rejects DSL | Validation error in IDE |
| No entry point (all steps have `after`) | Xtext validation rejects DSL | Validation error in IDE |
| No exit point (all steps are depended on) | Xtext validation rejects DSL | Validation error in IDE |
| Partial branch failure (e.g., 4b fails, 4a succeeds) | Scheduler marks 4b and downstream (5) as incomplete; 4a result preserved | Resume re-runs only 4b + 5 |
| DAG edges missing at resume time | OPEN QUESTION — fallback to full re-run or error? | TBD |

## Validation

| Field | Rule | On failure |
|---|---|---|
| `after` step names | Must reference existing step names in the same DSL file | Xtext validation error |
| `after` graph | Must be acyclic (topological sort succeeds) | Xtext validation error |
| Entry point | At least one step with no `after` | Xtext validation error |
| Exit point | At least one step not referenced by any other `after` | Xtext validation error |
| `steps` in `AnalyzeCommand` | Optional; each `StepDefinition` validated by Pydantic | 422 from translator |
| `steps` in `ScheduleRequest` | Optional; each `StepDefinition` validated by Pydantic | 422 from scheduler |

## Security & Authorization

*N/A — no changes to existing auth model. New endpoints follow the same authorization rules as existing API server endpoints.*

## Configuration

| Variable | Type | Required | Default | Description |
|---|---|---|---|---|
| TBD — DAG edge persistence toggle | bool | no | `true` | NOTE: may want a flag to disable DAG storage for lightweight runs |

- No new required environment variables identified at this time.
- Existing `DATABASE_URL`, Prefect, and service URLs remain unchanged.

## Tech Stack

- **DSL / Grammar**: Xtext (`cflDSL.xtext`)
- **Code Generator**: Xtend (Eclipse-based; rebuilt in this feature)
- **Translator**: Python, FastAPI, Pydantic v2 (`src/translator/`)
- **Scheduler**: Python, FastAPI, Prefect (`src/scheduler/`)
- **API Server**: Python, FastAPI, SQLAlchemy (`src/api_server/`)
- **Database**: PostgreSQL (JSONB for `job_state`, normalized table for DAG edges)
- **Orchestration**: Prefect (`wait_for` for parallel step coordination)

## Testing Strategy

- **Unit**: Topological sort, cycle detection, `get_ready_steps`, `get_incomplete_with_dependents`, Xtext validation rules.
- **Integration**: Full branching pipeline execution (parallel steps complete); partial branch failure and DAG-aware resume; backward compatibility with linear pipeline (no DAG).
- **E2E**: NOTE — full DSL-to-execution E2E test scope TBD.

## Performance & SLA

- **Parallel speedup**: Branching reduces total pipeline time vs sequential; to be measured in evaluation.
- **Checkpoint overhead**: Per-step checkpointing in DAGs preserves partial progress; overhead vs linear to be compared.
- **NOTE**: No hard SLA targets defined — performance is an evaluation metric, not a production requirement.

## Motivation

<!-- PRESERVED FROM ORIGINAL -->

- The linear pipeline variant processes steps sequentially, which does not reflect real-world analytical workflows where independent analyses can proceed in parallel.
- A branching (DAG-based) pipeline variant allows the thesis to evaluate:
  - Whether the DSL can express non-trivial topologies without becoming unreadable.
  - Whether the checkpoint/resume mechanism correctly handles partial branch failures.
  - Whether parallel execution yields measurable speedup over sequential execution.
- This variant is a deliberate complexity injection to stress-test the framework's generality.

## Impact on Thesis Evaluation

<!-- PRESERVED FROM ORIGINAL -->

- **Partial branch failure scenario**: Step 4a succeeds, Step 4b fails → resume re-runs only 4b and its downstream dependent (Step 5). Validates that checkpointing is branch-aware.
- **Parallel speedup measurement**: Branching reduces total pipeline wall-clock time vs sequential. Quantifies the practical benefit of DAG topology.
- **Checkpoint granularity in DAGs**: Per-step checkpointing preserves partial progress across branches. Demonstrates that the framework does not require full re-runs on partial failure.
- **Overhead comparison**: Linear vs branching checkpoint overhead can be directly compared using the same dataset.
- **RQ1**: DSL grammar becomes non-trivial (DAG definition with `after` lists); evaluates expressiveness and readability.
- **RQ2**: Recovery time analysis includes partial branch recovery, not just full pipeline recovery.
- **RQ3**: Overhead analysis can compare linear vs branching topologies on identical workloads.

## Glossary

| Term | Definition |
|---|---|
| DAG | Directed Acyclic Graph — a pipeline topology where steps have explicit dependencies and no cycles |
| `after` | Optional DSL field on a Step declaring which steps must complete before this step runs |
| Entry point | A step with no `after` dependencies; the start of a pipeline branch |
| Exit point | A step that no other step depends on; the end of a pipeline branch |
| Fan-out | A step whose completion triggers multiple independent downstream steps |
| Fan-in | A step that waits for multiple upstream steps before executing |
| `get_ready_steps` | Scheduler function returning all steps whose dependencies are fully satisfied |
| `get_incomplete_with_dependents` | Scheduler function computing steps to re-run on resume: incomplete steps plus their downstream dependents |
