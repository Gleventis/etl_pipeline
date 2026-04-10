"""DAG utilities for branching pipeline topology."""

import logging
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class CycleDetectedError(Exception):
    """Raised when a cycle is detected in the step dependency graph."""


def topological_sort(
    *,
    steps: list[str],
    edges: list[tuple[str, str]],
) -> list[str]:
    """Return steps in topological order respecting dependency edges.

    Uses Kahn's algorithm (BFS-based) which naturally detects cycles
    when not all nodes can be visited.

    Args:
        steps: All step names in the pipeline.
        edges: Dependency edges as (step_name, depends_on_step_name) tuples,
            meaning step_name depends on depends_on_step_name.

    Returns:
        Steps ordered so that each step appears after all its dependencies.

    Raises:
        CycleDetectedError: When the dependency graph contains a cycle.
    """
    in_degree: dict[str, int] = {step: 0 for step in steps}
    successors: dict[str, list[str]] = defaultdict(list)

    for step_name, depends_on in edges:
        successors[depends_on].append(step_name)
        in_degree[step_name] += 1

    queue = deque(step for step in steps if in_degree[step] == 0)
    result: list[str] = []

    while queue:
        current = queue.popleft()
        result.append(current)
        for successor in successors[current]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                queue.append(successor)

    if len(result) != len(steps):
        raise CycleDetectedError("dependency graph contains a cycle")

    return result


def get_ready_steps(
    *,
    all_steps: list[str],
    edges: list[tuple[str, str]],
    completed_steps: set[str],
) -> list[str]:
    """Return steps whose dependencies are all satisfied.

    A step is ready when every step it depends on appears in completed_steps.
    Entry points (no dependencies) are ready when completed_steps is empty.

    Args:
        all_steps: All step names in the pipeline.
        edges: Dependency edges as (step_name, depends_on_step_name) tuples.
        completed_steps: Steps that have already finished.

    Returns:
        Steps that are not yet completed and whose dependencies are all met.
    """
    dependencies: dict[str, set[str]] = {step: set() for step in all_steps}
    for step_name, depends_on in edges:
        dependencies[step_name].add(depends_on)

    return [
        step
        for step in all_steps
        if step not in completed_steps and dependencies[step].issubset(completed_steps)
    ]


def get_incomplete_with_dependents(
    *,
    all_steps: list[str],
    edges: list[tuple[str, str]],
    completed_steps: set[str],
) -> list[str]:
    """Return steps that still need to run, in topological order.

    Computes all steps not yet completed — including transitive dependents
    of any incomplete step — and returns them in execution order.

    Args:
        all_steps: All step names in the pipeline.
        edges: Dependency edges as (step_name, depends_on_step_name) tuples.
        completed_steps: Steps that have already finished successfully.

    Returns:
        Incomplete steps ordered topologically for re-execution.
    """
    ordered = topological_sort(steps=all_steps, edges=edges)
    return [step for step in ordered if step not in completed_steps]


if __name__ == "__main__":
    example_steps = [
        "descriptive_statistics",
        "data_cleaning",
        "temporal_analysis",
        "geospatial_analysis",
        "fare_revenue_analysis",
    ]
    example_edges = [
        ("data_cleaning", "descriptive_statistics"),
        ("temporal_analysis", "data_cleaning"),
        ("geospatial_analysis", "data_cleaning"),
        ("fare_revenue_analysis", "temporal_analysis"),
        ("fare_revenue_analysis", "geospatial_analysis"),
    ]
    order = topological_sort(steps=example_steps, edges=example_edges)
    print(f"topological order: {order}")

    ready = get_ready_steps(
        all_steps=example_steps,
        edges=example_edges,
        completed_steps={"descriptive_statistics", "data_cleaning"},
    )
    print(f"ready steps after cleaning: {ready}")

    incomplete = get_incomplete_with_dependents(
        all_steps=example_steps,
        edges=example_edges,
        completed_steps={
            "descriptive_statistics",
            "data_cleaning",
            "geospatial_analysis",
        },
    )
    print(f"incomplete after partial branch: {incomplete}")
