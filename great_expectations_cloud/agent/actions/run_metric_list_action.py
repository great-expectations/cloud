from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.cloud_data_store import (
    CloudDataStore,
)
from great_expectations.experimental.metric_repository.metric_list_metric_retriever import (
    MetricListMetricRetriever,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunMetricsListEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.experimental.metric_repository.metrics import MetricRun


class MetricListAction(AgentAction[RunMetricsListEvent]):
    def __init__(  # noqa: PLR0913  # Refactor opportunity
        self,
        context: CloudDataContext,
        base_url: str,
        organization_id: UUID,
        auth_key: str,
        metric_repository: MetricRepository | None = None,
        batch_inspector: BatchInspector | None = None,
    ):
        super().__init__(
            context=context, base_url=base_url, organization_id=organization_id, auth_key=auth_key
        )
        self._metric_repository = metric_repository or MetricRepository(
            data_store=CloudDataStore(self._context)
        )
        self._batch_inspector = batch_inspector or BatchInspector(
            context, [MetricListMetricRetriever(self._context)]
        )

    @override
    def run(self, event: RunMetricsListEvent, id: str) -> ActionResult:
        datasource = self._context.data_sources.get(event.datasource_name)
        data_asset = datasource.get_asset(event.data_asset_name)
        data_asset.test_connection()  # raises `TestConnectionError` on failure

        batch_request = data_asset.build_batch_request()

        metric_run = self._batch_inspector.compute_metric_list_run(
            data_asset_id=data_asset.id,
            batch_request=batch_request,
            metric_list=list(event.metric_names),
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
            raise RuntimeError(  # noqa: TRY003 # one off error
                "One or more metrics failed to compute."
            )


register_event_action("1", RunMetricsListEvent, MetricListAction)
