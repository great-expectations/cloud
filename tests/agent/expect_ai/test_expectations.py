from __future__ import annotations

import datetime

import pytest

from great_expectations_cloud.agent.expect_ai.expectations import (
    ExpectColumnMaxToBeBetween,
    ExpectColumnMeanToBeBetween,
    ExpectColumnMedianToBeBetween,
    ExpectColumnMinToBeBetween,
    ExpectColumnValuesToBeBetween,
)

pytestmark = pytest.mark.unit


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

    def test_invalid_string_raises(self):
        expectation = ExpectColumnValuesToBeBetween(
            column="col",
            min_value="not-a-date",
            max_value="also-not-a-date",
            mostly=1.0,
            description="test",
        )
        with pytest.raises(ValueError, match="Could not parse value"):
            expectation.get_gx_expectation()


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
