"""FHV taxi fare revenue analysis implementation."""

import polars as pl

from src.server.models import StepResult
from src.services.base.fare_revenue_analysis import BaseFareRevenueAnalysis


class FhvFareRevenueAnalysis(BaseFareRevenueAnalysis):
    """Fare revenue analysis for FHV taxi data.

    FHV data has no fare columns, so this step is skipped entirely.
    """

    def analyze(self, df: pl.DataFrame) -> StepResult:
        """Skip fare revenue analysis — FHV has no fare data.

        Args:
            df: Input dataframe loaded from parquet.

        Returns:
            StepResult with skipped summary, empty detail bytes, and s3 key.
        """
        return StepResult(
            summary_data={
                "skipped": True,
                "reason": "no fare columns available for FHV",
                "num_rows": df.height,
            },
            detail_bytes=b"",
            detail_s3_key="fare_revenue_analysis_detail.parquet",
        )


if __name__ == "__main__":
    sample = pl.DataFrame(
        {
            "dispatching_base_num": ["B00001", "B00002"],
            "pickup_datetime": ["2023-01-01T10:00:00", "2023-01-01T11:00:00"],
            "dropoff_datetime": ["2023-01-01T10:30:00", "2023-01-01T11:45:00"],
            "pulocationid": [1, 2],
            "dolocationid": [3, 4],
            "sr_flag": [None, 1],
            "affiliated_base_number": ["B00001", "B00002"],
        }
    )
    result = FhvFareRevenueAnalysis().analyze(df=sample)
    print(f"Summary: {result.summary_data}")
