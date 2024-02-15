from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunColumnDescriptiveMetricsEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.experimental.metric_repository.batch_inspector import (
        BatchInspector,
    )
    from great_expectations.experimental.metric_repository.metric_repository import (
        MetricRepository,
    )
    from great_expectations.experimental.metric_repository.metrics import MetricRun


class ColumnDescriptiveMetricsAction(AgentAction[RunColumnDescriptiveMetricsEvent]):
    def __init__(
        self,
        context: CloudDataContext,
        metric_repository: MetricRepository,
        batch_inspector: BatchInspector,
    ):
        super().__init__(context=context)
        self._metric_repository = metric_repository
        self._batch_inspector = batch_inspector

    @override
    def run(self, event: RunColumnDescriptiveMetricsEvent, id: str) -> ActionResult:
        datasource = self._context.get_datasource(event.datasource_name)
        data_asset = datasource.get_asset(event.data_asset_name)
        data_asset.test_connection()  # raises `TestConnectionError` on failure

        batch_request = data_asset.build_batch_request()

        metric_run = self._batch_inspector.compute_metric_run(
            data_asset_id=data_asset.id, batch_request=batch_request
        )

        metric_run_id = self._metric_repository.add_metric_run(metric_run)

        # Note: This exception is raised after the metric run is added to the repository so that
        # the user can still access any computed metrics even if one of the metrics fails.
        self._raise_on_any_metric_exception(metric_run)

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(resource_id=str(metric_run_id), type="MetricRun"),
            ],
        )

    def _raise_on_any_metric_exception(self, metric_run: MetricRun) -> None:
        if any(metric.exception for metric in metric_run.metrics):
            raise RuntimeError("One or more metrics failed to compute.")
