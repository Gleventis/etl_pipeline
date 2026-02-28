"""Request and response models for the data collector API."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TaxiType(StrEnum):
    """Supported NYC TLC taxi types."""

    YELLOW = "yellow"
    GREEN = "green"
    FHV = "fhv"
    FHVHV = "fhvhv"
    ALL = "all"


class IntRange(BaseModel):
    """Inclusive integer range."""

    model_config = ConfigDict(frozen=True)

    from_: int = Field(alias="from")
    to: int

    @model_validator(mode="after")
    def validate_range(self) -> IntRange:
        """Ensure from <= to."""
        if self.from_ > self.to:
            msg = f"'from' ({self.from_}) must be <= 'to' ({self.to})"
            raise ValueError(msg)
        return self


class YearField(BaseModel):
    """Year as single value or range."""

    model_config = ConfigDict(frozen=True)

    single: int | None = None
    range: IntRange | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_input(cls, data: dict | int) -> dict:
        """Accept an int or a dict with from/to keys."""
        if isinstance(data, int):
            return {"single": data}
        if isinstance(data, dict) and "from" in data and "to" in data:
            return {"range": data}
        if isinstance(data, dict) and ("single" in data or "range" in data):
            return data
        msg = "year must be an integer or an object with 'from' and 'to'"
        raise ValueError(msg)

    def expand(self) -> list[int]:
        """Return the list of years represented."""
        if self.single is not None:
            return [self.single]
        assert self.range is not None
        return list(range(self.range.from_, self.range.to + 1))


class MonthField(BaseModel):
    """Month as single value or range."""

    model_config = ConfigDict(frozen=True)

    single: int | None = Field(default=None, ge=1, le=12)
    range: IntRange | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_input(cls, data: dict | int) -> dict:
        """Accept an int or a dict with from/to keys."""
        if isinstance(data, int):
            return {"single": data}
        if isinstance(data, dict) and "from" in data and "to" in data:
            return {"range": data}
        if isinstance(data, dict) and ("single" in data or "range" in data):
            return data
        msg = "month must be an integer or an object with 'from' and 'to'"
        raise ValueError(msg)

    @model_validator(mode="after")
    def validate_month_bounds(self) -> MonthField:
        """Ensure month values are 1-12."""
        if self.range is not None:
            if not (1 <= self.range.from_ <= 12 and 1 <= self.range.to <= 12):
                msg = "month values must be between 1 and 12"
                raise ValueError(msg)
        return self

    def expand(self) -> list[int]:
        """Return the list of months represented."""
        if self.single is not None:
            return [self.single]
        assert self.range is not None
        return list(range(self.range.from_, self.range.to + 1))


class CollectRequest(BaseModel):
    """Request body for POST /collect."""

    model_config = ConfigDict(frozen=True)

    year: YearField
    month: MonthField
    taxi_type: TaxiType


class FileSuccess(BaseModel):
    """A successfully collected file."""

    model_config = ConfigDict(frozen=True)

    file_name: str
    s3_key: str


class FileFailure(BaseModel):
    """A file that failed collection."""

    model_config = ConfigDict(frozen=True)

    file_name: str
    reason: str


class CollectResponse(BaseModel):
    """Response body for POST /collect."""

    model_config = ConfigDict(frozen=True)

    successes: list[FileSuccess] = Field(default_factory=list)
    failures: list[FileFailure] = Field(default_factory=list)


if __name__ == "__main__":
    request = CollectRequest.model_validate(
        {
            "year": {"from": 2020, "to": 2023},
            "month": {"from": 1, "to": 12},
            "taxi_type": "all",
        }
    )
    print(f"Request: {request.model_dump()}")
    print(f"Years: {request.year.expand()}")
    print(f"Months: {request.month.expand()}")
