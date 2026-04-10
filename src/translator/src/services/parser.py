"""DSL parser for the translator service.

Defines command models that map to downstream service contracts
and a parse function to convert DSL strings into structured commands.
"""

import json
from collections import defaultdict, deque

from pydantic import BaseModel, ConfigDict, Field


class CollectCommand(BaseModel):
    """Maps to data collector's CollectRequest.

    Attributes:
        year: Single year or range dict with 'from'/'to' keys.
        month: Single month or range dict with 'from'/'to' keys.
        taxi_type: One of yellow, green, fhv, fhvhv, all.
    """

    model_config = ConfigDict(frozen=True)

    year: int | dict[str, int]
    month: int | dict[str, int]
    taxi_type: str


class StepDefinition(BaseModel):
    """Single step in a DAG-based pipeline topology.

    Attributes:
        name: Unique step identifier within the pipeline.
        action: Analytical action to perform (e.g. DESCRIPTIVE_STATISTICS).
        checkpoint: Whether to persist state after this step completes.
        after: Step names that must complete before this step runs.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    action: str = Field(min_length=1)
    checkpoint: bool = True
    after: list[str] = Field(default_factory=list)


class AnalyzeCommand(BaseModel):
    """Maps to scheduler's ScheduleRequest.

    Attributes:
        bucket: S3 bucket name.
        objects: List of S3 object paths.
        skip_checkpoints: Analytical steps to skip checkpointing for.
        steps: Optional DAG step definitions for branching pipeline.
    """

    model_config = ConfigDict(frozen=True)

    bucket: str
    objects: list[str]
    skip_checkpoints: list[str] = Field(default_factory=list)
    steps: list[StepDefinition] | None = None


class AggregateCommand(BaseModel):
    """Maps to aggregator GET endpoints.

    Attributes:
        endpoint: Aggregator endpoint name (e.g. 'descriptive-stats').
        params: Query parameters for the aggregation request.
    """

    model_config = ConfigDict(frozen=True)

    endpoint: str
    params: dict[str, str | int] = Field(default_factory=dict)


class ParsedDSL(BaseModel):
    """Result of parsing a DSL string into structured commands.

    At least one section must be present. Each section is optional
    to support independent execution (e.g., only AGGREGATE).

    Attributes:
        collect: Optional data collection command.
        analyze: Optional analysis/scheduling command.
        aggregate: Optional aggregation command.
    """

    model_config = ConfigDict(frozen=True)

    collect: CollectCommand | None = None
    analyze: AnalyzeCommand | None = None
    aggregate: AggregateCommand | None = None


def _validate_after_references(steps: list[StepDefinition]) -> None:
    """Validate that all after references point to existing step names.

    Args:
        steps: Step definitions with dependency declarations.

    Raises:
        ValueError: When an after reference names a step that does not exist.
    """
    names = {s.name for s in steps}
    for step in steps:
        for dep in step.after:
            if dep not in names:
                raise ValueError(
                    f"step '{step.name}' references undefined step '{dep}' in after"
                )


def _validate_has_entry_point(steps: list[StepDefinition]) -> None:
    """Validate that at least one step has no dependencies (entry point).

    Args:
        steps: Step definitions with dependency declarations.

    Raises:
        ValueError: When every step has at least one after dependency.
    """
    if steps and all(step.after for step in steps):
        raise ValueError(
            "step dependency graph has no entry point: every step has dependencies"
        )


def _validate_has_exit_point(steps: list[StepDefinition]) -> None:
    """Validate that at least one step is not depended on by any other step (exit point)."""
    if not steps:
        return
    referenced = {dep for step in steps for dep in step.after}
    names = {s.name for s in steps}
    if names <= referenced:
        raise ValueError(
            "step dependency graph has no exit point: every step is depended on by another"
        )


def _validate_no_cycles(steps: list[StepDefinition]) -> None:
    """Validate that the step dependency graph contains no cycles.

    Uses Kahn's algorithm: if topological sort cannot visit all nodes,
    a cycle exists.

    Args:
        steps: Step definitions with dependency declarations.

    Raises:
        ValueError: When the dependency graph contains a cycle.
    """
    names = {s.name for s in steps}
    in_degree: dict[str, int] = {s.name: 0 for s in steps}
    successors: dict[str, list[str]] = defaultdict(list)

    for step in steps:
        for dep in step.after:
            successors[dep].append(step.name)
            in_degree[step.name] += 1

    queue = deque(name for name, deg in in_degree.items() if deg == 0)
    visited = 0

    while queue:
        current = queue.popleft()
        visited += 1
        for successor in successors[current]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                queue.append(successor)

    if visited != len(names):
        raise ValueError("step dependency graph contains a cycle")


def parse_dsl(dsl: str) -> ParsedDSL:
    """Parse a JSON-based DSL string into structured commands.

    Placeholder implementation using JSON until the operator finalizes
    the grammar. Expects a JSON object with optional keys: collect,
    analyze, aggregate. At least one must be present.

    Args:
        dsl: JSON string representing the DSL input.

    Returns:
        Parsed command structure.

    Raises:
        ValueError: If the input is not valid JSON, not a dict,
            or contains no recognized sections.
    """
    try:
        raw = json.loads(dsl)
    except json.JSONDecodeError as e:
        raise ValueError(f"invalid JSON: {e}") from e

    if not isinstance(raw, dict):
        raise ValueError("DSL must be a JSON object")

    collect = CollectCommand(**raw["collect"]) if "collect" in raw else None
    analyze = AnalyzeCommand(**raw["analyze"]) if "analyze" in raw else None
    aggregate = AggregateCommand(**raw["aggregate"]) if "aggregate" in raw else None

    if collect is None and analyze is None and aggregate is None:
        raise ValueError(
            "DSL must contain at least one section: collect, analyze, or aggregate"
        )

    if analyze is not None and analyze.steps is not None:
        _validate_after_references(steps=analyze.steps)
        _validate_no_cycles(steps=analyze.steps)
        _validate_has_entry_point(steps=analyze.steps)
        _validate_has_exit_point(steps=analyze.steps)

    return ParsedDSL(collect=collect, analyze=analyze, aggregate=aggregate)


if __name__ == "__main__":
    sample = json.dumps(
        {
            "collect": {"year": 2024, "month": 1, "taxi_type": "yellow"},
            "analyze": {
                "bucket": "data-collector",
                "objects": ["yellow/2024-01.parquet"],
            },
        }
    )
    result = parse_dsl(dsl=sample)
    print(f"Parsed DSL: {result.model_dump_json(indent=2)}")
