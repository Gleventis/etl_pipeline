"""URL generation for NYC TLC trip record data downloads."""

from src.server.models import TaxiType

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

TAXI_TYPES_EXCLUDING_ALL = [
    TaxiType.YELLOW,
    TaxiType.GREEN,
    TaxiType.FHV,
    TaxiType.FHVHV,
]


def generate_urls(
    years: list[int],
    months: list[int],
    taxi_type: TaxiType,
) -> list[str]:
    """Generate TLC download URLs for the given years, months, and taxi type.

    Args:
        years: List of years to generate URLs for.
        months: List of months to generate URLs for.
        taxi_type: Taxi type or 'all' for all types.

    Returns:
        List of download URLs.
    """
    types = TAXI_TYPES_EXCLUDING_ALL if taxi_type == TaxiType.ALL else [taxi_type]
    return [
        f"{BASE_URL}/{t}_tripdata_{year}-{month:02d}.parquet"
        for year in years
        for month in months
        for t in types
    ]


if __name__ == "__main__":
    urls = generate_urls(years=[2023], months=[1, 2], taxi_type=TaxiType.YELLOW)
    for url in urls:
        print(url)
