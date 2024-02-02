from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from great_expectations.compatibility import pydantic


def generate_validation_error_text(validation_error: pydantic.ValidationError) -> str:
    missing_variables = ", ".join(
        [validation_error["loc"][0] for validation_error in validation_error.errors()]
    )
    error_text = f"Missing or badly formed environment variable(s). Make sure to set the following environment variable(s): {missing_variables}"
    return error_text
