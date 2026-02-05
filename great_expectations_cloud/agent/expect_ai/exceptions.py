from __future__ import annotations


class AgentError(Exception):
    """Base exception for all Asset Review Agent errors."""


class InvalidDataSourceTypeError(AgentError, TypeError):
    """Raised when an invalid asset type is provided."""

    def __init__(self, data_source_type: type, valid_types: tuple[type, ...]) -> None:
        """Initialize the exception with the data source type and valid types.

        Args:
            data_source_type: The invalid data source type that was provided.
            valid_types: The valid data_source types that are supported.
        """
        super().__init__(
            f"Invalid data source type {data_source_type}, only {valid_types} are supported."
        )


class InvalidAssetTypeError(AgentError, TypeError):
    """Raised when an invalid asset type is provided."""

    def __init__(self, asset_type: type, valid_types: tuple[type, ...]) -> None:
        """Initialize the exception with the asset type and valid types.

        Args:
            asset_type: The invalid asset type that was provided.
            valid_types: The valid asset types that are supported.
        """
        super().__init__(f"Invalid asset type {asset_type}, only {valid_types} are supported.")


class NoBatchParametersError(AgentError):
    """Raised when no batch parameters are found."""

    def __init__(self) -> None:
        """Initialize the exception."""
        super().__init__("No available batch parameters found.")


class InvalidResponseTypeError(AgentError, TypeError):
    """Raised when an invalid response type is received."""

    def __init__(self, received_type: type, expected_type: type) -> None:
        """Initialize the exception with the received and expected types.

        Args:
            received_type: The type that was received.
            expected_type: The type that was expected.
        """
        super().__init__(f"Expected {expected_type.__name__}, got {received_type}")


class MissingDataQualityPlanError(AgentError):
    """Raised when the data quality plan is missing."""

    def __init__(self) -> None:
        """Initialize the exception."""
        super().__init__("Data quality plan is missing.")


class InvalidExpectationTypeError(AgentError, TypeError):
    """Raised when an invalid expectation type is received."""

    def __init__(self, received_type: type, expected_type: type) -> None:
        """Initialize the exception with the received and expected types.

        Args:
            received_type: The type that was received.
            expected_type: The type that was expected.
        """
        super().__init__(f"Checker expected {expected_type.__name__}, got {received_type}")


class InvalidBatchDefinitionError(AgentError, TypeError):
    """Raised when an invalid batch definition is received."""

    def __init__(self, received_type: type, expected_type: type) -> None:
        """Initialize the exception with the received and expected types.

        Args:
            received_type: The type that was received.
            expected_type: The type that was expected.
        """
        super().__init__(f"Expected {expected_type.__name__}, got {received_type}")
