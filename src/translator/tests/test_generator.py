"""Tests for the DSL generator."""

import pytest

from src.services.generator import GrammarStep, GrammarWorkflow, generate


class TestGrammarModels:
    """Tests for grammar model instantiation."""

    def test_grammar_step_minimal(self) -> None:
        step = GrammarStep(name="s1", action="DESCRIPTIVE_STATISTICS")
        assert step.name == "s1"
        assert step.action == "DESCRIPTIVE_STATISTICS"
        assert step.checkpoint is True
        assert step.after == []

    def test_grammar_step_with_deps(self) -> None:
        step = GrammarStep(
            name="fare",
            action="FARE_REVENUE_ANALYSIS",
            checkpoint=False,
            after=["geo", "temporal"],
        )
        assert step.after == ["geo", "temporal"]
        assert step.checkpoint is False

    def test_grammar_workflow_empty_steps(self) -> None:
        wf = GrammarWorkflow(name="empty", steps=[])
        assert wf.name == "empty"
        assert wf.steps == []


class TestGenerateTopologicalSort:
    """Tests for topological ordering in generate()."""

    def test_empty_workflow(self) -> None:
        wf = GrammarWorkflow(name="test", steps=[])
        result = generate(workflow=wf)
        assert result == {"steps": []}

    def test_single_step(self) -> None:
        step = GrammarStep(name="s1", action="DESCRIPTIVE_STATISTICS")
        wf = GrammarWorkflow(name="test", steps=[step])
        result = generate(workflow=wf)
        assert len(result["steps"]) == 1
        assert result["steps"][0]["name"] == "s1"

    def test_linear_chain_preserves_order(self) -> None:
        steps = [
            GrammarStep(name="a", action="DESCRIPTIVE_STATISTICS"),
            GrammarStep(name="b", action="DATA_CLEANING", after=["a"]),
            GrammarStep(name="c", action="TEMPORAL_ANALYSIS", after=["b"]),
        ]
        wf = GrammarWorkflow(name="test", steps=steps)
        result = generate(workflow=wf)
        names = [s["name"] for s in result["steps"]]
        assert names == ["a", "b", "c"]

    def test_linear_chain_reorders_reversed_input(self) -> None:
        steps = [
            GrammarStep(name="c", action="TEMPORAL_ANALYSIS", after=["b"]),
            GrammarStep(name="b", action="DATA_CLEANING", after=["a"]),
            GrammarStep(name="a", action="DESCRIPTIVE_STATISTICS"),
        ]
        wf = GrammarWorkflow(name="test", steps=steps)
        result = generate(workflow=wf)
        names = [s["name"] for s in result["steps"]]
        assert names == ["a", "b", "c"]

    def test_full_dag_ordering(self) -> None:
        """Verify the example DAG: stats → cleaning → [geo, temporal] → fare."""
        steps = [
            GrammarStep(
                name="fare", action="FARE_REVENUE_ANALYSIS", after=["geo", "temporal"]
            ),
            GrammarStep(
                name="temporal", action="TEMPORAL_ANALYSIS", after=["cleaning"]
            ),
            GrammarStep(name="geo", action="GEOSPATIAL_ANALYSIS", after=["cleaning"]),
            GrammarStep(name="cleaning", action="DATA_CLEANING", after=["stats"]),
            GrammarStep(name="stats", action="DESCRIPTIVE_STATISTICS"),
        ]
        wf = GrammarWorkflow(name="pipeline", steps=steps)
        result = generate(workflow=wf)
        names = [s["name"] for s in result["steps"]]

        assert names.index("stats") < names.index("cleaning")
        assert names.index("cleaning") < names.index("geo")
        assert names.index("cleaning") < names.index("temporal")
        assert names.index("geo") < names.index("fare")
        assert names.index("temporal") < names.index("fare")

    def test_parallel_branches_both_before_join(self) -> None:
        steps = [
            GrammarStep(name="root", action="DESCRIPTIVE_STATISTICS"),
            GrammarStep(name="left", action="DATA_CLEANING", after=["root"]),
            GrammarStep(name="right", action="TEMPORAL_ANALYSIS", after=["root"]),
            GrammarStep(
                name="join", action="FARE_REVENUE_ANALYSIS", after=["left", "right"]
            ),
        ]
        wf = GrammarWorkflow(name="test", steps=steps)
        result = generate(workflow=wf)
        names = [s["name"] for s in result["steps"]]

        assert names[0] == "root"
        assert names[-1] == "join"
        assert set(names[1:3]) == {"left", "right"}

    def test_cycle_raises_value_error(self) -> None:
        steps = [
            GrammarStep(name="a", action="DESCRIPTIVE_STATISTICS", after=["b"]),
            GrammarStep(name="b", action="DATA_CLEANING", after=["a"]),
        ]
        wf = GrammarWorkflow(name="test", steps=steps)
        with pytest.raises(ValueError, match="cycle"):
            generate(workflow=wf)

    def test_output_includes_all_fields(self) -> None:
        step = GrammarStep(
            name="s1", action="DESCRIPTIVE_STATISTICS", checkpoint=False, after=[]
        )
        wf = GrammarWorkflow(name="test", steps=[step])
        result = generate(workflow=wf)
        assert result["steps"][0] == {
            "name": "s1",
            "action": "DESCRIPTIVE_STATISTICS",
            "checkpoint": False,
            "after": [],
        }
