"""Metric service protocol for expect_ai.

This module defines the interface for MetricService, which is implemented
in the gx-runner repository but needed by expect_ai for type checking.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from great_expectations.core.batch_definition import BatchDefinition, PartitionerT
    from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.metrics import Metric


class MetricNotComputableError(Exception):
    """Raised when a metric cannot be computed."""

    pass


class MetricService(Protocol):
    """Protocol defining the interface for MetricService used by expect_ai."""

    def get_data_source(
        self, data_source_name: str
    ) -> Datasource[DataAsset[Any, Any], ExecutionEngine[Any]]:
        """Get a data source by name.

        Args:
            data_source_name: Name of the data source to retrieve

        Returns:
            The requested datasource
        """
        ...

    def get_metric_value(
        self,
        metric: Metric[Any],
        batch_definition: BatchDefinition[PartitionerT],
        batch_parameters: dict[str, str | int] | None = None,
    ) -> Any:
        """Get the value of a metric.

        Args:
            metric: The metric to compute
            batch_definition: The batch definition
            batch_parameters: Optional batch parameters

        Returns:
            The computed metric value

        Raises:
            MetricNotComputableError: If the metric cannot be computed
        """
        ...

    def get_metric_value_or_error_text(
        self,
        metric: Metric[Any],
        batch_definition: BatchDefinition[PartitionerT],
        batch_parameters: dict[str, str | int] | None = None,
    ) -> Any:
        """Get the value of a metric or error text if it fails.

        Args:
            metric: The metric to compute
            batch_definition: The batch definition
            batch_parameters: Optional batch parameters

        Returns:
            The computed metric value or error message string
        """
        ...
