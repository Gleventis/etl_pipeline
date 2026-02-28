"""Tests for analyzer request, response, and internal models."""

import pytest
from pydantic import ValidationError

from src.server.models import AnalyzeRequest, AnalyzeResponse, StepResult, TaxiType


class TestTaxiType:
    """Tests for TaxiType enum."""

    def test_all_values(self):
        assert TaxiType.YELLOW == "yellow"
        assert TaxiType.GREEN == "green"
        assert TaxiType.FHV == "fhv"
        assert TaxiType.FHVHV == "fhvhv"

    def test_has_exactly_four_members(self):
        assert len(TaxiType) == 4


class TestAnalyzeRequest:
    """Tests for AnalyzeRequest model."""

    def test_valid_request(self):
        request = AnalyzeRequest(
            input_bucket="raw-data",
            input_object="yellow/2023/01/yellow_tripdata_2023-01.parquet",
            taxi_type=TaxiType.YELLOW,
            job_execution_id=42,
        )

        assert request.input_bucket == "raw-data"
        assert request.input_object == "yellow/2023/01/yellow_tripdata_2023-01.parquet"
        assert request.taxi_type == TaxiType.YELLOW
        assert request.job_execution_id == 42

    def test_frozen(self):
        request = AnalyzeRequest(
            input_bucket="raw-data",
            input_object="file.parquet",
            taxi_type=TaxiType.GREEN,
            job_execution_id=1,
        )

        with pytest.raises(ValidationError):
            request.input_bucket = "other"

    def test_rejects_empty_input_bucket(self):
        with pytest.raises(ValidationError):
            AnalyzeRequest(
                input_bucket="",
                input_object="file.parquet",
                taxi_type=TaxiType.YELLOW,
                job_execution_id=1,
            )

    def test_rejects_empty_input_object(self):
        with pytest.raises(ValidationError):
            AnalyzeRequest(
                input_bucket="raw-data",
                input_object="",
                taxi_type=TaxiType.YELLOW,
                job_execution_id=1,
            )

    def test_rejects_invalid_taxi_type(self):
        with pytest.raises(ValidationError):
            AnalyzeRequest(
                input_bucket="raw-data",
                input_object="file.parquet",
                taxi_type="invalid",
                job_execution_id=1,
            )

    def test_rejects_zero_job_execution_id(self):
        with pytest.raises(ValidationError):
            AnalyzeRequest(
                input_bucket="raw-data",
                input_object="file.parquet",
                taxi_type=TaxiType.YELLOW,
                job_execution_id=0,
            )

    def test_rejects_negative_job_execution_id(self):
        with pytest.raises(ValidationError):
            AnalyzeRequest(
                input_bucket="raw-data",
                input_object="file.parquet",
                taxi_type=TaxiType.YELLOW,
                job_execution_id=-1,
            )

    @pytest.mark.parametrize("taxi_type", list(TaxiType))
    def test_accepts_all_taxi_types(self, taxi_type: TaxiType):
        request = AnalyzeRequest(
            input_bucket="raw-data",
            input_object="file.parquet",
            taxi_type=taxi_type,
            job_execution_id=1,
        )

        assert request.taxi_type == taxi_type


class TestAnalyzeResponse:
    """Tests for AnalyzeResponse model."""

    def test_success_response(self):
        response = AnalyzeResponse(success=True)

        assert response.success is True
        assert response.error is None

    def test_failure_response(self):
        response = AnalyzeResponse(success=False, error="download failed")

        assert response.success is False
        assert response.error == "download failed"

    def test_frozen(self):
        response = AnalyzeResponse(success=True)

        with pytest.raises(ValidationError):
            response.success = False


class TestStepResult:
    """Tests for StepResult internal model."""

    def test_valid_step_result(self):
        result = StepResult(
            summary_data={"mean": 42.0, "count": 100},
            detail_bytes=b"parquet-content",
            detail_s3_key="descriptive-statistics-results/output.parquet",
        )

        assert result.summary_data == {"mean": 42.0, "count": 100}
        assert result.detail_bytes == b"parquet-content"
        assert result.detail_s3_key == "descriptive-statistics-results/output.parquet"

    def test_empty_summary_data(self):
        result = StepResult(
            summary_data={},
            detail_bytes=b"",
            detail_s3_key="key",
        )

        assert result.summary_data == {}
