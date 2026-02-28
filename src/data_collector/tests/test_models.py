"""Tests for data collector request/response models."""

import pytest

from src.server.models import (
    CollectRequest,
    CollectResponse,
    FileFailure,
    FileSuccess,
    MonthField,
    TaxiType,
    YearField,
)


class TestYearField:
    """Tests for YearField parsing and expansion."""

    def test_single_year(self):
        field = YearField.model_validate(2020)
        assert field.expand() == [2020]

    def test_year_range(self):
        field = YearField.model_validate({"from": 2020, "to": 2023})
        assert field.expand() == [2020, 2021, 2022, 2023]

    def test_year_range_same_value(self):
        field = YearField.model_validate({"from": 2020, "to": 2020})
        assert field.expand() == [2020]

    def test_year_range_invalid_order(self):
        with pytest.raises(ValueError, match="must be <="):
            YearField.model_validate({"from": 2023, "to": 2020})

    def test_year_invalid_input(self):
        with pytest.raises(ValueError):
            YearField.model_validate("bad")


class TestMonthField:
    """Tests for MonthField parsing and expansion."""

    def test_single_month(self):
        field = MonthField.model_validate(6)
        assert field.expand() == [6]

    def test_month_range(self):
        field = MonthField.model_validate({"from": 1, "to": 3})
        assert field.expand() == [1, 2, 3]

    def test_month_range_invalid_order(self):
        with pytest.raises(ValueError, match="must be <="):
            MonthField.model_validate({"from": 12, "to": 1})

    def test_month_out_of_bounds_single(self):
        with pytest.raises(ValueError):
            MonthField.model_validate(0)

    def test_month_out_of_bounds_single_high(self):
        with pytest.raises(ValueError):
            MonthField.model_validate(13)

    def test_month_out_of_bounds_range(self):
        with pytest.raises(ValueError, match="between 1 and 12"):
            MonthField.model_validate({"from": 0, "to": 12})


class TestCollectRequest:
    """Tests for CollectRequest model."""

    def test_full_request_with_ranges(self):
        request = CollectRequest.model_validate(
            {
                "year": {"from": 2020, "to": 2023},
                "month": {"from": 1, "to": 12},
                "taxi_type": "all",
            }
        )
        assert request.year.expand() == [2020, 2021, 2022, 2023]
        assert request.month.expand() == list(range(1, 13))
        assert request.taxi_type == TaxiType.ALL

    def test_single_values(self):
        request = CollectRequest.model_validate(
            {"year": 2023, "month": 6, "taxi_type": "yellow"}
        )
        assert request.year.expand() == [2023]
        assert request.month.expand() == [6]
        assert request.taxi_type == TaxiType.YELLOW

    def test_invalid_taxi_type(self):
        with pytest.raises(ValueError):
            CollectRequest.model_validate(
                {"year": 2023, "month": 1, "taxi_type": "invalid"}
            )

    @pytest.mark.parametrize("taxi_type", ["yellow", "green", "fhv", "fhvhv", "all"])
    def test_all_taxi_types_accepted(self, taxi_type: str):
        request = CollectRequest.model_validate(
            {"year": 2023, "month": 1, "taxi_type": taxi_type}
        )
        assert request.taxi_type == taxi_type


class TestCollectResponse:
    """Tests for CollectResponse model."""

    def test_empty_response(self):
        response = CollectResponse()
        assert response.successes == []
        assert response.failures == []

    def test_response_with_results(self):
        response = CollectResponse(
            successes=[
                FileSuccess(file_name="a.parquet", s3_key="yellow/2023/01/a.parquet")
            ],
            failures=[FileFailure(file_name="b.parquet", reason="HTTP 404")],
        )
        assert len(response.successes) == 1
        assert len(response.failures) == 1
        assert response.successes[0].file_name == "a.parquet"
        assert response.failures[0].reason == "HTTP 404"
