"""Tests for the DSL parser."""

import json

import pytest

from src.services.parser import (
    AggregateCommand,
    AnalyzeCommand,
    CollectCommand,
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
