"""DSL parser for the translator service.

Defines command models that map to downstream service contracts
and a parse function to convert DSL strings into structured commands.
"""

import json

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


class AnalyzeCommand(BaseModel):
    """Maps to scheduler's ScheduleRequest.

    Attributes:
        bucket: S3 bucket name.
        objects: List of S3 object paths.
        skip_checkpoints: Analytical steps to skip checkpointing for.
    """

    model_config = ConfigDict(frozen=True)

    bucket: str
    objects: list[str]
    skip_checkpoints: list[str] = Field(default_factory=list)


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
