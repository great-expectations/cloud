from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from great_expectations.metrics.metric_results import (
    MetricErrorResult,
    MetricResult,
)

if TYPE_CHECKING:
    from great_expectations.core.batch_definition import PartitionerT
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import BatchDefinition, Datasource
    from great_expectations.datasource.fluent.interfaces import _DataAssetT, _ExecutionEngineT
    from great_expectations.metrics.metric import Metric

    from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import BatchParameters


MetricResultT = TypeVar("MetricResultT", bound=MetricResult[Any])


class MetricNotComputableError(Exception):
    def __init__(self, message: str):
        super().__init__(f"Could not compute metric: {message}")


class MetricService:
    def __init__(self, context: CloudDataContext):
        self._context = context

    def get_data_source(self, data_source_name: str) -> Datasource[_DataAssetT, _ExecutionEngineT]:
        return self._context.data_sources.get(data_source_name)

    def get_metric_result(
        self,
        batch_definition: BatchDefinition[PartitionerT],
        metric: Metric[MetricResultT],
        batch_parameters: BatchParameters | None,
    ) -> MetricResultT | MetricErrorResult:
        """Get a MetricResult for a batch.

        Returns the MetricResult associated with the Metric, or a MetricErrorResult if the Metric computation failed.
        """
        batch = batch_definition.get_batch(batch_parameters=batch_parameters)
        return batch.compute_metrics(metric)

    def get_metric_value(
        self,
        batch_definition: BatchDefinition[PartitionerT],
        metric: Metric[MetricResultT],
        batch_parameters: BatchParameters | None,
    ) -> Any:
        """Get the value of a metric for a batch.

        Raises MetricNotComputableError if the metric is not computable.
        """
        metric_result = self.get_metric_result(
            batch_definition=batch_definition,
            metric=metric,
            batch_parameters=batch_parameters,
        )
        if isinstance(metric_result, MetricErrorResult):
            raise MetricNotComputableError(metric_result.value.exception_message)
        return metric_result.value

    def get_metric_value_or_error_text(
        self,
        batch_definition: BatchDefinition[PartitionerT],
        metric: Metric[MetricResultT],
        batch_parameters: BatchParameters | None,
    ) -> Any:
        """Get the value of a metric for a batch or the error message if the metric is not computable."""
        try:
            return self.get_metric_value(
                batch_definition=batch_definition,
                metric=metric,
                batch_parameters=batch_parameters,
            )
        except MetricNotComputableError as e:
            return str(e)
