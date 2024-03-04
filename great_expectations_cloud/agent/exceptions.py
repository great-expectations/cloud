from __future__ import annotations


class GXCoreError(Exception):
    """To bridge between 0.18.x and 1.0 we create the GXCoreError base class in the agent code
    and add additional properties if relevant when subclassing. This should be moved to the
    GX Core repo when error codes are added to the core codebase in v1."""

    def __init__(self, message: str, error_code: str) -> None:
        super().__init__(message)
        self.error_code = error_code
