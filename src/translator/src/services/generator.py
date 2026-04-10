"""DSL model to JSON pipeline definition generator.

Reads a parsed Xtext grammar model (represented as Python types) and produces
a JSON-compatible dict consumable by the translator's parse_dsl function.
"""

from collections import defaultdict, deque

from pydantic import BaseModel, ConfigDict, Field


class GrammarStep(BaseModel):
    """A step from the Xtext grammar AST.

    Attributes:
        name: Step identifier.
        action: ActionTypes enum value (e.g. DESCRIPTIVE_STATISTICS).
        checkpoint: Whether to checkpoint after this step.
        after: Names of steps this step depends on.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    action: str = Field(min_length=1)
    checkpoint: bool = True
    after: list[str] = Field(default_factory=list)


class GrammarWorkflow(BaseModel):
    """A workflow from the Xtext grammar AST.

    Attributes:
        name: Workflow identifier.
        steps: Ordered list of pipeline steps.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    steps: list[GrammarStep] = Field(default_factory=list)


def _topological_sort(steps: list[GrammarStep]) -> list[GrammarStep]:
    """Sort steps in dependency order using Kahn's algorithm.

    Args:
        steps: Unordered list of grammar steps with after dependencies.

    Returns:
        Steps reordered so that every step appears after its dependencies.

    Raises:
        ValueError: If the dependency graph contains a cycle.
    """
    by_name: dict[str, GrammarStep] = {s.name: s for s in steps}
    in_degree: dict[str, int] = {s.name: len(s.after) for s in steps}
    successors: dict[str, list[str]] = defaultdict(list)
    for step in steps:
        for dep in step.after:
            successors[dep].append(step.name)

    queue = deque(name for name, deg in in_degree.items() if deg == 0)
    ordered: list[GrammarStep] = []
    while queue:
        current = queue.popleft()
        ordered.append(by_name[current])
        for successor in successors[current]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                queue.append(successor)

    if len(ordered) != len(steps):
        raise ValueError("step dependency graph contains a cycle")
    return ordered


def generate(workflow: GrammarWorkflow) -> dict:
    """Generate a JSON-compatible pipeline definition from a parsed DSL workflow.

    Args:
        workflow: Parsed workflow model from the Xtext grammar.

    Returns:
        Pipeline definition dict with steps in topological order.
    """
    sorted_steps = _topological_sort(steps=workflow.steps)
    return {
        "steps": [
            {
                "name": s.name,
                "action": s.action,
                "checkpoint": s.checkpoint,
                "after": s.after,
            }
            for s in sorted_steps
        ],
    }


if __name__ == "__main__":
    step = GrammarStep(name="stats", action="DESCRIPTIVE_STATISTICS")
    wf = GrammarWorkflow(name="example", steps=[step])
    result = generate(workflow=wf)
    print(f"generated: {result}")
