from __future__ import annotations

import pytest

from great_expectations_cloud.agent.exceptions import GXCoreError


class ErrorWithParams(GXCoreError):
    def __init__(self, message: str, error_code: str, param1: str, param2: str):
        super().__init__(message=message, error_code=error_code)
        self.param1 = param1
        self.param2 = param2


class ErrorWithParamsAndMore(GXCoreError):
    class_attr_1 = "class_attr_1"
    class_attr_2 = "class_attr_2"

    def __init__(self, message: str, error_code: str, param1: str, param2: str):
        super().__init__(message=message, error_code=error_code)
        self.param1 = param1
        self.param2 = param2

    @property
    def some_property(self) -> str:
        return "some_property"

    def some_method(self) -> str:
        return "some_method"

    @classmethod
    def some_class_method(cls) -> str:
        return "some_class_method"

    @staticmethod
    def some_static_method() -> str:
        return "some_static_method"


@pytest.mark.parametrize(
    "error, expected_params",
    [
        (GXCoreError(message="test error", error_code="test_code"), {}),
        (
            ErrorWithParams(
                message="test error", error_code="test_code", param1="param1", param2="param2"
            ),
            {"param1": "param1", "param2": "param2"},
        ),
        (
            ErrorWithParamsAndMore(
                message="test error", error_code="test_code", param1="param1", param2="param2"
            ),
            {
                "param1": "param1",
                "param2": "param2",
                # "class_attr_1": "class_attr_1", # Should not appear
                # "class_attr_2": "class_attr_2", # Should not appear
                # "some_property": "some_property", # Should not appear
                # "some_method": "some_method", # Should not appear
                # "some_class_method": "some_class_method", # Should not appear
                # "some_static_method": "some_static_method", # Should not appear
            },
        ),
    ],
)
def test_get_error_params(error: GXCoreError, expected_params: dict[str, str]):
    assert error.get_error_params() == expected_params
