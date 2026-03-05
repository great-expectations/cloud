from __future__ import annotations

import datetime

import pytest

from great_expectations_cloud.agent.expect_ai.expectations import (
    ExpectColumnMaxToBeBetween,
    ExpectColumnMeanToBeBetween,
    ExpectColumnMedianToBeBetween,
    ExpectColumnMinToBeBetween,
    ExpectColumnStdevToBeBetween,
    ExpectColumnValuesToBeBetween,
    _parse_comparable_value,
)

pytestmark = pytest.mark.unit


class TestParseComparableValue:
    def test_float_passthrough(self):
        assert _parse_comparable_value(3.14) == 3.14

    def test_int_coerced_to_float(self):
        result = _parse_comparable_value(42)
        assert result == 42.0
        assert isinstance(result, float)

    def test_iso_date_string(self):
        result = _parse_comparable_value("2023-01-15")
        assert result == datetime.date(2023, 1, 15)
        assert isinstance(result, datetime.date)

    def test_iso_datetime_string(self):
        result = _parse_comparable_value("2023-01-15T10:30:00+00:00")
        assert result == datetime.datetime(2023, 1, 15, 10, 30, 0, tzinfo=datetime.UTC)

    def test_non_date_string_raises(self):
        with pytest.raises(ValueError, match="Could not parse value"):
            _parse_comparable_value("not-a-date")


class TestExpectColumnValuesToBeBetweenWithDates:
    def test_date_strings_produce_gx_expectation_with_date_objects(self):
        expectation = ExpectColumnValuesToBeBetween(
            column="order_date",
            min_value="2023-01-01",
            max_value="2024-12-31",
            mostly=1.0,
            description="Order dates should fall within expected range",
        )
        gx = expectation.get_gx_expectation()
        assert gx.min_value == datetime.date(2023, 1, 1)
        assert gx.max_value == datetime.date(2024, 12, 31)

    def test_float_values_still_work(self):
        expectation = ExpectColumnValuesToBeBetween(
            column="amount",
            min_value=0.0,
            max_value=10000.0,
            mostly=1.0,
            description="Amount within range",
        )
        gx = expectation.get_gx_expectation()
        assert gx.min_value == 0.0
        assert gx.max_value == 10000.0


class TestExpectColumnMinToBeBetweenWithDates:
    def test_date_strings_produce_gx_expectation_with_date_objects(self):
        expectation = ExpectColumnMinToBeBetween(
            column="created_date",
            min_value="2020-01-01",
            max_value="2020-06-30",
            description="Min date should be in first half of 2020",
        )
        gx = expectation.get_gx_expectation()
        assert gx.min_value == datetime.date(2020, 1, 1)
        assert gx.max_value == datetime.date(2020, 6, 30)

    def test_float_values_still_work(self):
        expectation = ExpectColumnMinToBeBetween(
            column="score",
            min_value=0.0,
            max_value=100.0,
            description="Min score within range",
        )
        gx = expectation.get_gx_expectation()
        assert gx.min_value == 0.0
        assert gx.max_value == 100.0


class TestExpectColumnMaxToBeBetweenWithDates:
    def test_date_strings_produce_gx_expectation_with_date_objects(self):
        expectation = ExpectColumnMaxToBeBetween(
            column="end_date",
            min_value="2025-01-01",
            max_value="2025-12-31",
            description="Max date should be in 2025",
        )
        gx = expectation.get_gx_expectation()
        assert gx.min_value == datetime.date(2025, 1, 1)
        assert gx.max_value == datetime.date(2025, 12, 31)


class TestExpectColumnMedianToBeBetweenWithDates:
    def test_date_strings_produce_gx_expectation_with_date_objects(self):
        expectation = ExpectColumnMedianToBeBetween(
            column="event_date",
            min_value="2023-06-01",
            max_value="2023-06-30",
            description="Median event date in June 2023",
        )
        gx = expectation.get_gx_expectation()
        assert gx.min_value == datetime.date(2023, 6, 1)
        assert gx.max_value == datetime.date(2023, 6, 30)


class TestExpectColumnMeanToBeBetweenWithDates:
    def test_date_strings_produce_gx_expectation_with_date_objects(self):
        expectation = ExpectColumnMeanToBeBetween(
            column="timestamp_col",
            min_value="2023-01-01",
            max_value="2023-12-31",
            description="Mean timestamp in 2023",
        )
        gx = expectation.get_gx_expectation()
        assert gx.min_value == datetime.date(2023, 1, 1)
        assert gx.max_value == datetime.date(2023, 12, 31)


class TestExpectColumnStdevToBeBetweenWithDates:
    def test_float_values_still_work(self):
        expectation = ExpectColumnStdevToBeBetween(
            column="amount",
            min_value=0.0,
            max_value=50.0,
            description="Stdev within range",
        )
        gx = expectation.get_gx_expectation()
        assert gx.min_value == 0.0
        assert gx.max_value == 50.0
