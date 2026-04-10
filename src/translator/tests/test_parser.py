"""Tests for the DSL parser."""

import json

import pytest

from src.services.parser import (
    AggregateCommand,
    AnalyzeCommand,
    CollectCommand,
    StepDefinition,
    _validate_after_references,
    _validate_has_entry_point,
    _validate_has_exit_point,
    _validate_no_cycles,
    parse_dsl,
)


class TestParseDslValidInputs:
    """Tests for valid DSL inputs."""

    def test_all_sections(self) -> None:
        dsl = json.dumps(
            {
                "collect": {"year": 2024, "month": 1, "taxi_type": "yellow"},
                "analyze": {"bucket": "b", "objects": ["o1"]},
                "aggregate": {"endpoint": "descriptive-stats"},
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.collect is not None
        assert result.analyze is not None
        assert result.aggregate is not None

    def test_collect_only(self) -> None:
        dsl = json.dumps({"collect": {"year": 2024, "month": 6, "taxi_type": "green"}})
        result = parse_dsl(dsl=dsl)
        assert result.collect == CollectCommand(year=2024, month=6, taxi_type="green")
        assert result.analyze is None
        assert result.aggregate is None

    def test_analyze_only(self) -> None:
        dsl = json.dumps({"analyze": {"bucket": "b", "objects": ["a.parquet"]}})
        result = parse_dsl(dsl=dsl)
        assert result.analyze == AnalyzeCommand(bucket="b", objects=["a.parquet"])
        assert result.collect is None
        assert result.aggregate is None

    def test_aggregate_only(self) -> None:
        dsl = json.dumps(
            {
                "aggregate": {
                    "endpoint": "taxi-comparison",
                    "params": {"taxi_type": "yellow"},
                }
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.aggregate == AggregateCommand(
            endpoint="taxi-comparison", params={"taxi_type": "yellow"}
        )
        assert result.collect is None
        assert result.analyze is None

    def test_collect_and_analyze(self) -> None:
        dsl = json.dumps(
            {
                "collect": {"year": 2024, "month": 1, "taxi_type": "yellow"},
                "analyze": {"bucket": "b", "objects": ["o"]},
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.collect is not None
        assert result.analyze is not None
        assert result.aggregate is None

    def test_year_range(self) -> None:
        dsl = json.dumps(
            {
                "collect": {
                    "year": {"from": 2023, "to": 2024},
                    "month": 1,
                    "taxi_type": "yellow",
                }
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.collect is not None
        assert result.collect.year == {"from": 2023, "to": 2024}

    def test_month_range(self) -> None:
        dsl = json.dumps(
            {
                "collect": {
                    "year": 2024,
                    "month": {"from": 1, "to": 6},
                    "taxi_type": "fhv",
                }
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.collect is not None
        assert result.collect.month == {"from": 1, "to": 6}

    def test_analyze_with_skip_checkpoints(self) -> None:
        dsl = json.dumps(
            {
                "analyze": {
                    "bucket": "b",
                    "objects": ["o"],
                    "skip_checkpoints": ["descriptive_statistics"],
                }
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.analyze is not None
        assert result.analyze.skip_checkpoints == ["descriptive_statistics"]

    def test_analyze_defaults_skip_checkpoints_to_empty(self) -> None:
        dsl = json.dumps({"analyze": {"bucket": "b", "objects": ["o"]}})
        result = parse_dsl(dsl=dsl)
        assert result.analyze is not None
        assert result.analyze.skip_checkpoints == []

    def test_analyze_defaults_steps_to_none(self) -> None:
        dsl = json.dumps({"analyze": {"bucket": "b", "objects": ["o"]}})
        result = parse_dsl(dsl=dsl)
        assert result.analyze is not None
        assert result.analyze.steps is None

    def test_analyze_with_steps(self) -> None:
        dsl = json.dumps(
            {
                "analyze": {
                    "bucket": "b",
                    "objects": ["o"],
                    "steps": [
                        {"name": "s1", "action": "DESCRIPTIVE_STATISTICS"},
                        {
                            "name": "s2",
                            "action": "DATA_CLEANING",
                            "checkpoint": False,
                            "after": ["s1"],
                        },
                    ],
                }
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.analyze is not None
        assert result.analyze.steps is not None
        assert len(result.analyze.steps) == 2
        assert result.analyze.steps[0] == StepDefinition(
            name="s1", action="DESCRIPTIVE_STATISTICS"
        )
        assert result.analyze.steps[1] == StepDefinition(
            name="s2", action="DATA_CLEANING", checkpoint=False, after=["s1"]
        )

    def test_aggregate_defaults_params_to_empty(self) -> None:
        dsl = json.dumps({"aggregate": {"endpoint": "data-quality"}})
        result = parse_dsl(dsl=dsl)
        assert result.aggregate is not None
        assert result.aggregate.params == {}

    def test_extra_keys_ignored(self) -> None:
        dsl = json.dumps(
            {
                "collect": {"year": 2024, "month": 1, "taxi_type": "yellow"},
                "unknown": "ignored",
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.collect is not None
        assert result.analyze is None

    def test_whitespace_around_dsl(self) -> None:
        dsl = (
            "  \n"
            + json.dumps({"collect": {"year": 2024, "month": 1, "taxi_type": "yellow"}})
            + "  \n"
        )
        result = parse_dsl(dsl=dsl)
        assert result.collect is not None
        assert result.collect.year == 2024


class TestParseDslInvalidInputs:
    """Tests for invalid DSL inputs."""

    def test_empty_string(self) -> None:
        with pytest.raises(ValueError, match="invalid JSON"):
            parse_dsl(dsl="")

    def test_not_json(self) -> None:
        with pytest.raises(ValueError, match="invalid JSON"):
            parse_dsl(dsl="not json at all")

    def test_json_array(self) -> None:
        with pytest.raises(ValueError, match="must be a JSON object"):
            parse_dsl(dsl="[1, 2, 3]")

    def test_json_string(self) -> None:
        with pytest.raises(ValueError, match="must be a JSON object"):
            parse_dsl(dsl='"just a string"')

    def test_no_recognized_sections(self) -> None:
        with pytest.raises(ValueError, match="at least one section"):
            parse_dsl(dsl='{"foo": "bar"}')

    def test_empty_object(self) -> None:
        with pytest.raises(ValueError, match="at least one section"):
            parse_dsl(dsl="{}")

    def test_collect_missing_required_field(self) -> None:
        dsl = json.dumps({"collect": {"year": 2024}})
        with pytest.raises(Exception):
            parse_dsl(dsl=dsl)

    def test_analyze_missing_required_field(self) -> None:
        dsl = json.dumps({"analyze": {"bucket": "b"}})
        with pytest.raises(Exception):
            parse_dsl(dsl=dsl)

    def test_aggregate_missing_endpoint(self) -> None:
        dsl = json.dumps({"aggregate": {"params": {"k": "v"}}})
        with pytest.raises(Exception):
            parse_dsl(dsl=dsl)

    def test_collect_year_wrong_type(self) -> None:
        dsl = json.dumps(
            {"collect": {"year": "abc", "month": 1, "taxi_type": "yellow"}}
        )
        with pytest.raises(Exception):
            parse_dsl(dsl=dsl)


class TestParsedDslFrozen:
    """Tests that parsed results are immutable."""

    def test_parsed_dsl_is_frozen(self) -> None:
        dsl = json.dumps({"collect": {"year": 2024, "month": 1, "taxi_type": "yellow"}})
        result = parse_dsl(dsl=dsl)
        with pytest.raises(Exception):
            result.collect = None  # type: ignore[misc]

    def test_collect_command_is_frozen(self) -> None:
        dsl = json.dumps({"collect": {"year": 2024, "month": 1, "taxi_type": "yellow"}})
        result = parse_dsl(dsl=dsl)
        assert result.collect is not None
        with pytest.raises(Exception):
            result.collect.year = 9999  # type: ignore[misc]

    def test_analyze_command_is_frozen(self) -> None:
        dsl = json.dumps({"analyze": {"bucket": "b", "objects": ["o"]}})
        result = parse_dsl(dsl=dsl)
        assert result.analyze is not None
        with pytest.raises(Exception):
            result.analyze.bucket = "other"  # type: ignore[misc]


class TestStepDefinition:
    """Tests for StepDefinition model."""

    def test_instantiates_with_all_fields(self) -> None:
        step = StepDefinition(
            name="x",
            action="DESCRIPTIVE_STATISTICS",
            checkpoint=True,
            after=[],
        )
        assert step.name == "x"
        assert step.action == "DESCRIPTIVE_STATISTICS"
        assert step.checkpoint is True
        assert step.after == []

    def test_defaults_checkpoint_true(self) -> None:
        step = StepDefinition(name="s1", action="DATA_CLEANING")
        assert step.checkpoint is True

    def test_defaults_after_empty(self) -> None:
        step = StepDefinition(name="s1", action="DATA_CLEANING")
        assert step.after == []

    def test_with_dependencies(self) -> None:
        step = StepDefinition(
            name="fare",
            action="FARE_REVENUE_ANALYSIS",
            after=["geospatial", "temporal"],
        )
        assert step.after == ["geospatial", "temporal"]

    def test_is_frozen(self) -> None:
        step = StepDefinition(name="s1", action="DATA_CLEANING")
        with pytest.raises(Exception):
            step.name = "other"  # type: ignore[misc]

    def test_rejects_empty_name(self) -> None:
        with pytest.raises(Exception):
            StepDefinition(name="", action="DATA_CLEANING")

    def test_rejects_empty_action(self) -> None:
        with pytest.raises(Exception):
            StepDefinition(name="s1", action="")


class TestValidateNoCycles:
    """Tests for cycle detection in step dependency graph."""

    def test_acyclic_graph_passes(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
            StepDefinition(name="c", action="TEMPORAL_ANALYSIS", after=["b"]),
        ]
        _validate_no_cycles(steps=steps)  # should not raise

    def test_direct_cycle_raises(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS", after=["b"]),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
        ]
        with pytest.raises(ValueError, match="cycle"):
            _validate_no_cycles(steps=steps)

    def test_self_loop_raises(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS", after=["a"]),
        ]
        with pytest.raises(ValueError, match="cycle"):
            _validate_no_cycles(steps=steps)

    def test_indirect_cycle_raises(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS", after=["c"]),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
            StepDefinition(name="c", action="TEMPORAL_ANALYSIS", after=["b"]),
        ]
        with pytest.raises(ValueError, match="cycle"):
            _validate_no_cycles(steps=steps)

    def test_single_step_no_deps_passes(self) -> None:
        steps = [StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS")]
        _validate_no_cycles(steps=steps)  # should not raise

    def test_diamond_dag_passes(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
            StepDefinition(name="c", action="TEMPORAL_ANALYSIS", after=["a"]),
            StepDefinition(name="d", action="FARE_REVENUE_ANALYSIS", after=["b", "c"]),
        ]
        _validate_no_cycles(steps=steps)  # should not raise

    def test_parse_dsl_rejects_cyclic_steps(self) -> None:
        dsl = json.dumps(
            {
                "analyze": {
                    "bucket": "b",
                    "objects": ["o"],
                    "steps": [
                        {
                            "name": "a",
                            "action": "DESCRIPTIVE_STATISTICS",
                            "after": ["b"],
                        },
                        {"name": "b", "action": "DATA_CLEANING", "after": ["a"]},
                    ],
                }
            }
        )
        with pytest.raises(ValueError, match="cycle"):
            parse_dsl(dsl=dsl)

    def test_parse_dsl_accepts_acyclic_steps(self) -> None:
        dsl = json.dumps(
            {
                "analyze": {
                    "bucket": "b",
                    "objects": ["o"],
                    "steps": [
                        {"name": "a", "action": "DESCRIPTIVE_STATISTICS"},
                        {"name": "b", "action": "DATA_CLEANING", "after": ["a"]},
                    ],
                }
            }
        )
        result = parse_dsl(dsl=dsl)
        assert result.analyze is not None
        assert len(result.analyze.steps) == 2

    def test_parse_dsl_skips_validation_when_no_steps(self) -> None:
        dsl = json.dumps({"analyze": {"bucket": "b", "objects": ["o"]}})
        result = parse_dsl(dsl=dsl)
        assert result.analyze is not None
        assert result.analyze.steps is None


class TestValidateAfterReferences:
    """Tests for after-reference validation in step dependency graph."""

    def test_valid_references_pass(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
        ]
        _validate_after_references(steps=steps)  # should not raise

    def test_undefined_reference_raises(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING", after=["nonexistent"]),
        ]
        with pytest.raises(ValueError, match="undefined step 'nonexistent'"):
            _validate_after_references(steps=steps)

    def test_multiple_deps_one_undefined_raises(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
            StepDefinition(
                name="c", action="TEMPORAL_ANALYSIS", after=["a", "missing"]
            ),
        ]
        with pytest.raises(ValueError, match="undefined step 'missing'"):
            _validate_after_references(steps=steps)

    def test_no_deps_passes(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING"),
        ]
        _validate_after_references(steps=steps)  # should not raise

    def test_empty_steps_passes(self) -> None:
        _validate_after_references(steps=[])  # should not raise

    def test_parse_dsl_rejects_undefined_after_reference(self) -> None:
        dsl = json.dumps(
            {
                "analyze": {
                    "bucket": "b",
                    "objects": ["o"],
                    "steps": [
                        {"name": "a", "action": "DESCRIPTIVE_STATISTICS"},
                        {
                            "name": "b",
                            "action": "DATA_CLEANING",
                            "after": ["ghost"],
                        },
                    ],
                }
            }
        )
        with pytest.raises(ValueError, match="undefined step 'ghost'"):
            parse_dsl(dsl=dsl)


class TestValidateHasEntryPoint:
    """Tests for entry point validation in step dependency graph."""

    def test_step_without_deps_passes(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
        ]
        _validate_has_entry_point(steps=steps)  # should not raise

    def test_all_steps_have_deps_raises(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS", after=["b"]),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
        ]
        with pytest.raises(ValueError, match="no entry point"):
            _validate_has_entry_point(steps=steps)

    def test_single_step_no_deps_passes(self) -> None:
        steps = [StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS")]
        _validate_has_entry_point(steps=steps)  # should not raise

    def test_single_step_with_self_dep_raises(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS", after=["a"]),
        ]
        with pytest.raises(ValueError, match="no entry point"):
            _validate_has_entry_point(steps=steps)

    def test_empty_steps_passes(self) -> None:
        _validate_has_entry_point(steps=[])  # should not raise

    def test_parse_dsl_rejects_no_entry_point(self) -> None:
        dsl = json.dumps(
            {
                "analyze": {
                    "bucket": "b",
                    "objects": ["o"],
                    "steps": [
                        {
                            "name": "a",
                            "action": "DESCRIPTIVE_STATISTICS",
                            "after": ["b"],
                        },
                        {"name": "b", "action": "DATA_CLEANING", "after": ["a"]},
                    ],
                }
            }
        )
        with pytest.raises(ValueError, match="no entry point|cycle"):
            parse_dsl(dsl=dsl)


class TestValidateHasExitPoint:
    """Tests for exit point validation in step dependency graph."""

    def test_step_not_referenced_passes(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
        ]
        _validate_has_exit_point(steps=steps)

    def test_all_steps_referenced_raises(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS", after=["b"]),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
        ]
        with pytest.raises(ValueError, match="no exit point"):
            _validate_has_exit_point(steps=steps)

    def test_single_step_no_deps_passes(self) -> None:
        steps = [StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS")]
        _validate_has_exit_point(steps=steps)

    def test_diamond_dag_has_exit_point(self) -> None:
        steps = [
            StepDefinition(name="a", action="DESCRIPTIVE_STATISTICS"),
            StepDefinition(name="b", action="DATA_CLEANING", after=["a"]),
            StepDefinition(name="c", action="TEMPORAL_ANALYSIS", after=["a"]),
            StepDefinition(name="d", action="FARE_REVENUE_ANALYSIS", after=["b", "c"]),
        ]
        _validate_has_exit_point(steps=steps)

    def test_empty_steps_passes(self) -> None:
        _validate_has_exit_point(steps=[])

    def test_parse_dsl_rejects_no_exit_point(self) -> None:
        dsl = json.dumps(
            {
                "analyze": {
                    "bucket": "b",
                    "objects": ["o"],
                    "steps": [
                        {
                            "name": "a",
                            "action": "DESCRIPTIVE_STATISTICS",
                            "after": ["b"],
                        },
                        {"name": "b", "action": "DATA_CLEANING", "after": ["a"]},
                    ],
                }
            }
        )
        with pytest.raises(ValueError, match="no exit point|cycle"):
            parse_dsl(dsl=dsl)
