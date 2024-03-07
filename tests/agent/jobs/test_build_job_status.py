from __future__ import annotations

import pytest

from great_expectations_cloud.agent.exceptions import GXCoreError
from great_expectations_cloud.agent.models import JobCompleted, build_failed_job_completed_status


class ErrorWithParams(GXCoreError):
    def __init__(self, message: str, error_code: str, param1: str, param2: str):
        super().__init__(message=message, error_code=error_code)
        self.param1 = param1
        self.param2 = param2


@pytest.mark.parametrize(
    "error, expected_status",
    [
        (
            GXCoreError(message="test error", error_code="test-error-code"),
            JobCompleted(
                success=False,
                error_stack_trace="test error",
                error_code="test-error-code",
                error_params={},
            ),
        ),
        (
            ErrorWithParams(
                message="test error",
                error_code="test-error-code",
                param1="test-param1",
                param2="test-param2",
            ),
            JobCompleted(
                success=False,
                error_stack_trace="test error",
                error_code="test-error-code",
                error_params={"param1": "test-param1", "param2": "test-param2"},
            ),
        ),
    ],
)
def test_build_failed_job_completed_status_gx_core_error(
    error: GXCoreError, expected_status: JobCompleted
):
    status = build_failed_job_completed_status(error)
    assert status == expected_status


def test_build_failed_job_completed_status_non_gx_core_error():
    error = RuntimeError("test error")
    status = build_failed_job_completed_status(error)
    assert status == JobCompleted(
        success=False,
        error_stack_trace="test error",
    )
