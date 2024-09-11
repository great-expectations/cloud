from __future__ import annotations

import enum
from typing import NoReturn


def raise_with_error_code(e: Exception, error_code: ErrorCode) -> NoReturn:
    """Raise a GXCoreError with the given error_code and the message from the given exception.

    Meant to standardize the way we raise GXCoreErrors with error codes.
    """
    raise GXCoreError(message=str(e), error_code=error_code) from e


class GXCoreError(Exception):
    """To bridge between 0.18.x and 1.0 we create the GXCoreError base class in the agent code
    and add additional properties if relevant when subclassing. This should be moved to the
    GX Core repo when error codes are added to the core codebase in v1."""

    def __init__(self, message: str, error_code: str) -> None:
        super().__init__(message)
        self.error_code = error_code

    def get_error_params(self) -> dict[str, str]:
        """Return a dictionary of error parameters to be included in the error response,
        not including the error message or error code."""
        return {k: str(v) for k, v in vars(self).items() if k not in ["message", "error_code"]}


class ErrorCode(str, enum.Enum):
    """Error codes for GX Core errors."""

    GENERIC_UNHANDLED_ERROR = "generic-unhandled-error"
    WRONG_USERNAME_OR_PASSWORD = "wrong-username-or-password"  # noqa: S105  # Not a hardcoded password


class GXAgentError(Exception): ...


class GXAgentConfigError(GXAgentError): ...


class GXAuthenticationError(GXAgentError): ...
