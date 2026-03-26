"""Pipeline step definitions and resolution for the scheduler service."""

from src.services.config import Settings

STEPS: list[str] = [
    "descriptive_statistics",
    "data_cleaning",
    "temporal_analysis",
    "geospatial_analysis",
    "fare_revenue_analysis",
]

_STEP_TO_BUCKET_ATTR: dict[str, str] = {
    "descriptive_statistics": "STEP_DESCRIPTIVE_STATISTICS_BUCKET",
    "data_cleaning": "STEP_DATA_CLEANING_BUCKET",
    "temporal_analysis": "STEP_TEMPORAL_ANALYSIS_BUCKET",
    "geospatial_analysis": "STEP_GEOSPATIAL_ANALYSIS_BUCKET",
    "fare_revenue_analysis": "STEP_FARE_REVENUE_ANALYSIS_BUCKET",
}


def validate_step_names(*, step_names: list[str]) -> list[str]:
    """Return any step names not in the valid STEPS list.

    Args:
        step_names: List of step names to validate.

    Returns:
        List of invalid step names. Empty if all are valid.
    """
    valid = set(STEPS)
    return [name for name in step_names if name not in valid]


def get_input_bucket(*, step: str, settings: Settings) -> str:
    """Resolve the input bucket for a pipeline step from config.

    Args:
        step: Pipeline step name.
        settings: Scheduler settings instance.

    Returns:
        The configured input bucket name for the step.

    Raises:
        ValueError: If the step name is unknown.
    """
    attr = _STEP_TO_BUCKET_ATTR.get(step)
    if attr is None:
        raise ValueError(f"unknown pipeline step: {step}")
    return getattr(settings, attr)


def get_next_step(*, completed_steps: list[str]) -> str | None:
    """Determine the next pipeline step given completed steps.

    Args:
        completed_steps: List of step names already completed.

    Returns:
        The next step name, or None if all steps are complete.
    """
    completed_set = set(completed_steps)
    for step in STEPS:
        if step not in completed_set:
            return step
    return None


if __name__ == "__main__":
    settings = Settings()
    for step in STEPS:
        bucket = get_input_bucket(step=step, settings=settings)
        print(f"{step} -> {bucket}")

    print(f"next after []: {get_next_step(completed_steps=[])}")
    print(f"next after first two: {get_next_step(completed_steps=STEPS[:2])}")
    print(f"next after all: {get_next_step(completed_steps=STEPS)}")

    invalid = validate_step_names(step_names=["data_cleaning", "bogus", "fake"])
    print(f"invalid step names: {invalid}")
