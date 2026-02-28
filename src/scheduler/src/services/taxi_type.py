"""Extract taxi type from MinIO object paths."""

_VALID_TAXI_TYPES = {"yellow", "green", "fhv", "fhvhv"}


def extract_taxi_type(*, object_name: str) -> str:
    """Extract taxi type from the first segment of an object path.

    Args:
        object_name: Object path (e.g. 'yellow/2022/01/file.parquet').

    Returns:
        Taxi type string (e.g. 'yellow').

    Raises:
        ValueError: If the path prefix is not a recognized taxi type.
    """
    prefix = object_name.split("/")[0].lower()
    if prefix not in _VALID_TAXI_TYPES:
        raise ValueError(
            f"unrecognized taxi type '{prefix}' in object path: {object_name}"
        )
    return prefix


if __name__ == "__main__":
    result = extract_taxi_type(
        object_name="yellow/2022/01/yellow_tripdata_2022-01.parquet"
    )
    print(f"Taxi type: {result}")
