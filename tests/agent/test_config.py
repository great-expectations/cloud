from __future__ import annotations

import pytest
from pydantic.v1 import (
    BaseModel,
    ValidationError,
)

from great_expectations_cloud.agent.config import generate_config_validation_error_text


class LoggingUtilTestModel(BaseModel):
    string_one: str
    string_two: str


@pytest.fixture
def validation_error_missing_string_one():
    try:
        LoggingUtilTestModel(string_one=None, string_two="i am string two")  # type: ignore[arg-type]
    except ValidationError as validation_error:
        return validation_error


@pytest.fixture
def validation_error_missing_both_strings():
    try:
        LoggingUtilTestModel(string_one=None, string_two=None)  # type: ignore[arg-type]
    except ValidationError as validation_error:
        return validation_error


@pytest.fixture
def expected_error_text_missing_string_one():
    return "Missing or badly formed environment variable(s). Make sure to set the following environment variable(s): string_one"


@pytest.fixture
def expected_error_text_missing_both_strings():
    return "Missing or badly formed environment variable(s). Make sure to set the following environment variable(s): string_one, string_two"


def test_generate_config_validation_error_text_with_one_missing_attribute(
    validation_error_missing_string_one, expected_error_text_missing_string_one
):
    error_text = generate_config_validation_error_text(validation_error_missing_string_one)
    assert error_text == expected_error_text_missing_string_one


def test_generate_config_validation_error_text_with_two_missing_attributes(
    validation_error_missing_both_strings, expected_error_text_missing_both_strings
):
    error_text = generate_config_validation_error_text(validation_error_missing_both_strings)
    assert error_text == expected_error_text_missing_both_strings
