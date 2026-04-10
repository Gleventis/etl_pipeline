# Lessons Learned

## 2026-02-28: Hatchling build backend + nested source layout
- When using hatchling with a `src/` layout where the directory name doesn't match the project name, you must explicitly set `[tool.hatch.build.targets.wheel] packages = ["src"]` in `pyproject.toml`.
- The build backend string is `hatchling.build`, not `hatchling.backends`.

## 2026-02-28: Testing pydantic-settings defaults inside docker
- Docker-compose environment variables override pydantic-settings defaults. Tests for default values must use `monkeypatch.delenv()` to clear injected env vars.

## 2026-03-01: Testcontainers don't work inside docker-compose containers
- Testcontainers requires access to the Docker socket (`/var/run/docker.sock`), which is not available inside a docker-compose service container by default.
- For integration tests that need a database, use the Postgres service already defined in docker-compose instead of testcontainers.
- Read the DATABASE_URL from Settings/env vars to connect to the compose Postgres.

## 2026-03-02: Wiring FastAPI routes to services via app.state requires test updates
- When routes switch from inline stubs to `request.app.state.service`, existing `TestClient` tests break because the lifespan doesn't run (no DB connection in test).
- Fix: set `app.state.scheduler_service = MagicMock()` at module level in the test file before creating `TestClient`.
- This avoids needing a real DB for route-level tests while still verifying HTTP contract + delegation.

## 2026-03-02: Mutable list aliasing in mock assertions
- When passing a mutable list to a mocked function, `unittest.mock` captures a reference, not a copy.
- If the list is mutated after the call, `call_args` will reflect the final state, not the state at call time.
- Fix: always pass `list(original)` to create a snapshot when the caller needs to assert on intermediate states.

## 2026-03-02: Service-internal DB connections require schema init in test fixtures
- When a service opens its own DB connections internally (e.g., `get_connection(database_url=self._db_url)`), test fixtures must ensure the schema is initialized before the service runs.
- Even tests that don't seed data need the `conn` fixture (or equivalent) to guarantee `init_schema()` + table cleanup has happened.
- Without this, tests fail with "relation does not exist" because the service's internal connection hits an uninitialized database.

## 2026-03-02: Prefect Docker image has no curl/wget
- The `prefecthq/prefect:3-latest` image only includes Python — no `curl` or `wget`.
- For docker-compose healthchecks, use `python -c "import urllib.request; urllib.request.urlopen('http://localhost:4200/api/health')"` instead of `curl -f`.

## 2026-03-02: Prefect @flow calls block when invoked directly
- Calling a `@flow`-decorated function directly (e.g., `process_file_flow(...)`) blocks until the flow run completes.
- To run multiple flow runs concurrently from non-Prefect code, use `concurrent.futures.ThreadPoolExecutor` to submit each call in a separate thread.
- Prefect's `.submit()` is for tasks within a flow, not for top-level flow invocations from outside.

## 2026-03-05: Always verify file existence before using `create`
- `glob` can miss files if the pattern doesn't match exactly (e.g., searching `**/history.md` from the wrong root).
- Before using `fs_write create` on a file that *might* exist, use `fs_read` on the exact expected path first.
- If the file exists, use `append` or `str_replace`. Only use `create` when confirmed the file doesn't exist.

## 2026-03-05: Use polars.to_arrow() instead of pyarrow.Table.from_pandas()
- The analyzer Docker image does not include `pandas`. Using `pa.Table.from_pandas(df.to_pandas())` fails with `ModuleNotFoundError`.
- Use `df.to_arrow()` directly on a Polars DataFrame to get a PyArrow Table without the pandas dependency.
- This applies to all analyzer service code that serializes DataFrames to parquet.

## 2026-03-09: FastAPI sync lifespan crashes uvicorn on startup
- Using `@contextmanager` (sync) for FastAPI's `lifespan` parameter works with `TestClient` but crashes when running under uvicorn with `TypeError: '_GeneratorContextManager' object does not support the asynchronous context manager protocol`.
- Fix: use `@asynccontextmanager` with `async def` and `AsyncGenerator` return type.
- `TestClient` handles both sync and async lifespans internally, so unit tests won't catch this — only real server startup reveals the bug.

## 2026-03-09: Integration tests with persistent DB need unique identifiers
- When integration tests seed data into a database that persists between test runs (containers stay up), hardcoded assertions on exact counts will fail on re-runs.
- Fix: use unique identifiers per test run (e.g., `uuid.uuid4().hex[:8]` in object names) and use `>=` assertions instead of `==` for counts.
- Alternatively, clean the database before seeding, but this requires DELETE endpoints or direct DB access.

## 2026-03-09: ThreadPoolExecutor silently swallows missing keyword arguments
- When `executor.submit(fn, **kwargs)` is missing a required keyword argument, the `TypeError` is captured inside the `Future` object.
- `wait(futures)` does NOT re-raise exceptions from completed futures — it only waits for them to finish.
- This means a missing parameter causes the function to silently fail with no logs, no errors, and no side effects.
- Fix: always verify that the arguments passed to `executor.submit` match the target function's full signature, especially after the target function's signature changes.
- Consider calling `future.result()` after `wait()` to surface exceptions, or use `as_completed()` with explicit error handling.

## 2026-03-09: Hardcoded bucket in SchedulerService caused bucket mismatch
- The `SchedulerService` was initialized with `bucket="raw-data"` hardcoded in `main.py`, ignoring the bucket sent by the data collector in the schedule request.
- The data collector uploads to `data-collector` bucket but the scheduler told the analyzer to read from `raw-data` (which didn't exist).
- Fix: remove hardcoded bucket from `SchedulerService.__init__`, accept `bucket` as a parameter in `schedule_batch()`, and use `record.bucket` from DB in `resume_failed()`.
- Lesson: when a value flows through multiple services, trace the full path end-to-end before assuming it's correct.

## 2026-03-09: Analyzer timeout too short for computationally intensive steps
- The analyzer client had a 60s default timeout, but data_cleaning on a 48MB yellow taxi file took ~100s.
- Fix: add `ANALYZER_TIMEOUT` config setting (300s default) and pass it through the `execute_step` Prefect task to `send_job`.
- Lesson: always check the expected computation time from the spec when setting HTTP timeouts for inter-service calls.

## 2026-03-12: Monkeypatching httpx.Client requires __init__ override
- When monkeypatching `httpx.Client` to inject a `MockTransport`, do NOT use a lambda wrapper like `lambda **kwargs: httpx.Client(transport=transport, **kwargs)` — the production code also passes `transport` (or `verify`) via kwargs, causing "multiple values for keyword argument".
- Fix: monkeypatch `httpx.Client.__init__` directly, override `transport` in kwargs, and pop conflicting params like `verify`.

## 2026-03-13: Subagent file existence reports can be stale or wrong
- When a subagent reports a file doesn't exist, verify with `git status` or `ls` before using `fs_write create` — the file may already be committed.
- `fs_write create` silently overwrites existing files. If the file exists, use `str_replace` to make surgical edits instead.
- This is an extension of the 2026-03-05 lesson about verifying file existence before using `create`.

## 2026-04-10: Prefect task.submit() requires flow run context
- `task.submit()` can only be called from within a flow run. Calling `flow.fn()` bypasses the `@flow` decorator entirely — no flow run context is created.
- Tests that need `.submit()` must call the flow directly (`flow(...)`) instead of `flow.fn(...)`.
- Calling a Prefect flow directly works without a Prefect server — it runs in-process with the default ConcurrentTaskRunner.
- Module-level `unittest.mock.patch` decorators still work correctly when tasks run in threads via `.submit()`, because the mock replaces the module-level attribute visible to all threads.

## 2026-04-10: Tightly coupled plan tasks should be implemented together
- When two tasks in an implementation plan have circular dependencies (e.g., "persist X" and "use persisted X"), treat them as a single atomic unit.
- Don't force artificial separation that would leave the codebase in a broken intermediate state.
- Still keep the commit message clear about which plan tasks are covered.

## 2026-04-10: mock.reset_mock() does not clear side_effect
- `mock.reset_mock()` resets call args and call count but does NOT reset `side_effect` or `return_value` by default.
- When reusing a mock across phases (e.g., initial run with `side_effect=fn` then resume with `return_value=val`), the old `side_effect` takes precedence over `return_value`.
- Fix: explicitly set `mock.side_effect = None` before setting a new `return_value`.
- Alternative: use `mock.reset_mock(side_effect=True, return_value=True)` to clear everything.
