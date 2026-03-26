# Translator Service

## Overview
HTTP service that parses a human-readable DSL into orchestrated HTTP calls to downstream services (data collector, scheduler, aggregator). Execution is async — the service returns a run ID immediately and the operator polls for status.

## Dependency
Requires [Scheduler Checkpoint Configuration](scheduler_checkpoint_config.md) to be implemented first.

## API

### POST /translator/translate
Accepts a JSON body:
```json
{"dsl": "<DSL string>"}
```
Returns `202 Accepted` with:
```json
{"run_id": "<uuid>"}
```

### GET /translator/runs/{run_id}
Returns current run state:
```json
{
  "run_id": "<uuid>",
  "phase": "collecting | analyzing | aggregating | completed | failed",
  "error": "<message or null>"
}
```
Returns `404` if `run_id` is unknown.

## DSL Grammar
**TODO** — grammar definition to be provided by the operator. The parser module will be a placeholder until the grammar is finalized. The DSL supports three sections that can appear independently or together:
- **COLLECT** — triggers data collection
- **ANALYZE** — triggers scheduling/analysis with optional checkpoint control
- **AGGREGATE** — triggers aggregation queries

Each section maps to one downstream service call. Sections can be submitted individually (e.g., only AGGREGATE on already-processed data).

## Execution Flow

1. Parse DSL string into structured commands
2. Store run record in Postgres with status `pending`
3. Return `run_id` to caller
4. Execute commands in background thread:
   - If COLLECT section present: `POST /collector/collect` → update phase to `collecting`
   - If ANALYZE section present: `POST /scheduler/schedule` (with `skip_checkpoints` if specified) → update phase to `analyzing`
   - If AGGREGATE section present: `GET /aggregations/<endpoint>` → update phase to `aggregating`
5. On success: update phase to `completed`
6. On failure: update phase to `failed`, store error message, stop execution

## Error Handling
- DSL parse failure → `400 Bad Request` with parse error details (synchronous, before run creation)
- Downstream service failure → run phase set to `failed`, error stored, no retry
- Aggregator returns no data → `412 Precondition Failed` stored as error in run record

## State Storage
Own Postgres table `translator_runs`:
| Column     | Type        |
|------------|-------------|
| run_id     | UUID PK     |
| dsl        | TEXT        |
| phase      | VARCHAR     |
| error      | TEXT NULL   |
| created_at | TIMESTAMPTZ |
| updated_at | TIMESTAMPTZ |

## Downstream Service Contracts

### Data Collector
- Endpoint: `POST /collector/collect`
- Request: `CollectRequest` — `year`, `month`, `taxi_type`
- Source: `src/data_collector/src/server/models.py`

### Scheduler
- Endpoint: `POST /scheduler/schedule`
- Request: `ScheduleRequest` — `bucket`, `objects`, `skip_checkpoints`
- Source: `src/scheduler/src/server/models.py`

### Aggregator
- Endpoints: `GET /aggregations/descriptive-stats`, `/taxi-comparison`, `/temporal-patterns`, `/data-quality`, `/pipeline-performance`
- Query params: `taxi_type`, `start_year`, `start_month`, `end_year`, `end_month`
- Source: `src/aggregator/src/server/routes.py`
