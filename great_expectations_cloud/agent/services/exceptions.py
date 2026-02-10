from __future__ import annotations


class MissingCloudConfigError(Exception):
    """Exception raised when CloudDataContext is missing ge_cloud_config."""

    def __init__(self) -> None:
        super().__init__("Missing ge_cloud_config")
