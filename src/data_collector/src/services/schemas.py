"""Expected parquet schemas per NYC TLC taxi type and validation logic."""

import logging

import pyarrow.parquet as pq

from src.server.models import TaxiType

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS: dict[TaxiType, set[str]] = {
    TaxiType.YELLOW: {
        "vendorid",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "store_and_fwd_flag",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    },
    TaxiType.GREEN: {
        "vendorid",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "store_and_fwd_flag",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "ehail_fee",
        "improvement_surcharge",
        "total_amount",
        "payment_type",
        "trip_type",
        "congestion_surcharge",
    },
    TaxiType.FHV: {
        "dispatching_base_num",
        "pickup_datetime",
        "dropoff_datetime",
        "pulocationid",
        "dolocationid",
        "sr_flag",
        "affiliated_base_number",
    },
    TaxiType.FHVHV: {
        "hvfhs_license_num",
        "dispatching_base_num",
        "originating_base_num",
        "request_datetime",
        "on_scene_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "pulocationid",
        "dolocationid",
        "trip_miles",
        "trip_time",
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        "shared_request_flag",
        "shared_match_flag",
        "access_a_ride_flag",
        "wav_request_flag",
        "wav_match_flag",
    },
}


def validate_parquet_schema(file_bytes: bytes, taxi_type: TaxiType) -> list[str]:
    """Validate a parquet file's schema against expected columns for a taxi type.

    Checks that all expected columns are present (case-insensitive).
    Extra columns in the file are allowed — TLC occasionally adds new fields.

    Args:
        file_bytes: Raw bytes of the parquet file.
        taxi_type: The taxi type to validate against.

    Returns:
        List of validation error messages. Empty list means valid.
    """
    import io

    errors: list[str] = []

    try:
        pf = pq.ParquetFile(io.BytesIO(file_bytes))
    except Exception as exc:
        errors.append(f"not a valid parquet file: {exc}")
        return errors

    actual_columns = {
        pf.schema_arrow.field(i).name.lower() for i in range(len(pf.schema_arrow))
    }
    expected = EXPECTED_COLUMNS[taxi_type]
    missing = expected - actual_columns

    if missing:
        errors.append(f"missing columns: {sorted(missing)}")
        logger.warning(
            "schema validation failed for %s: missing columns %s",
            taxi_type,
            sorted(missing),
        )

    return errors


if __name__ == "__main__":
    print("Expected columns per taxi type:")
    for taxi_type, columns in EXPECTED_COLUMNS.items():
        print(f"  {taxi_type}: {len(columns)} columns")
