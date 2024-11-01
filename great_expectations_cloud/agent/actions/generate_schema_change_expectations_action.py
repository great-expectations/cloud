from __future__ import annotations

import uuid
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
from great_expectations.experimental.metric_repository.metrics import MetricTypes
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    CreatedResource,
    GenerateSchemaChangeExpectationsEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class GenerateSchemaChangeExpectationsAction(AgentAction[GenerateSchemaChangeExpectationsEvent]):
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
    def run(self, event: GenerateSchemaChangeExpectationsEvent, id: str) -> ActionResult:
        metric_run_id = uuid.uuid4()

        for asset in event.data_assets:
            datasource = self._context.data_sources.get(event.datasource_name)
            data_asset = datasource.get_asset(asset)
            data_asset.test_connection()
            batch_request = data_asset.build_batch_request()

            self._batch_inspector.compute_metric_list_run(
                data_asset_id=data_asset.id,
                batch_request=batch_request,
                metric_list=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
            )
            # TODO - Update with full logic in ZELDA-1058

        # TODO - Update this after the full implementation in ZELDA-1058
        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(resource_id=str(metric_run_id), type="MetricRun"),
            ],
        )


register_event_action(
    "1", GenerateSchemaChangeExpectationsEvent, GenerateSchemaChangeExpectationsAction
)
