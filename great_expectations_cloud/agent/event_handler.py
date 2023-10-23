from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.cloud_data_store import (
    CloudDataStore,
)
from great_expectations.experimental.metric_repository.column_descriptive_metrics_metric_retriever import (  # noqa: E501
    ColumnDescriptiveMetricsMetricRetriever,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)

from great_expectations_cloud.agent.actions import (
    ColumnDescriptiveMetricsAction,
    ListTableNamesAction,
)
from great_expectations_cloud.agent.actions.data_assistants import (
    RunMissingnessDataAssistantAction,
    RunOnboardingDataAssistantAction,
)
from great_expectations_cloud.agent.actions.draft_datasource_config_action import (
    DraftDatasourceConfigAction,
)
from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    Event,
    ListTableNamesEvent,
    RunCheckpointEvent,
    RunColumnDescriptiveMetricsEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.experimental.metric_repository.metric_retriever import (
        MetricRetriever,
    )

    from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction


LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def get_event_action(self, event: Event) -> AgentAction:
        """Get the action that should be run for the given event."""
        if isinstance(event, RunOnboardingDataAssistantEvent):
            return RunOnboardingDataAssistantAction(context=self._context)

        if isinstance(event, RunMissingnessDataAssistantEvent):
            return RunMissingnessDataAssistantAction(context=self._context)

        if isinstance(event, ListTableNamesEvent):
            return ListTableNamesAction(context=self._context)

        if isinstance(event, RunCheckpointEvent):
            return RunCheckpointAction(context=self._context)

        if isinstance(event, RunColumnDescriptiveMetricsEvent):
            metric_retrievers: list[MetricRetriever] = [
                ColumnDescriptiveMetricsMetricRetriever(self._context)
            ]
            batch_inspector = BatchInspector(self._context, metric_retrievers)
            cloud_data_store = CloudDataStore(self._context)
            column_descriptive_metrics_repository = MetricRepository(data_store=cloud_data_store)
            return ColumnDescriptiveMetricsAction(
                context=self._context,
                batch_inspector=batch_inspector,
                metric_repository=column_descriptive_metrics_repository,
            )

        if isinstance(event, DraftDatasourceConfigEvent):
            return DraftDatasourceConfigAction(context=self._context)

        # shouldn't get here
        raise UnknownEventError("Unknown message received - cannot process.")

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""
        action = self.get_event_action(event=event)
        LOGGER.info(f"Handling event: {event.type} -> {action.__class__.__name__}")
        action_result = action.run(event=event, id=id)
        return action_result


class UnknownEventError(Exception):
    ...
