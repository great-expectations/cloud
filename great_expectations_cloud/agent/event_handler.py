from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Final

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
from great_expectations_cloud.agent.actions.unknown_event import UnknownEventAction
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

    from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction


LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def get_event_action(self, event: Event) -> AgentAction[Any]:
        EVENT_TO_ACTION_MAP = {
            RunOnboardingDataAssistantEvent: RunOnboardingDataAssistantAction,
            RunMissingnessDataAssistantEvent: RunMissingnessDataAssistantAction,
            ListTableNamesEvent: ListTableNamesAction,
            RunCheckpointEvent: RunCheckpointAction,
            RunColumnDescriptiveMetricsEvent: ColumnDescriptiveMetricsAction,
            DraftDatasourceConfigEvent: DraftDatasourceConfigAction,
        }

        if action := EVENT_TO_ACTION_MAP.get(event):
            return action(context=self._context)
        # Building an UnknownEventAction allows noop
        return UnknownEventAction(context=self._context)

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""
        action = self.get_event_action(event=event)
        # THis can be unknown event now!
        LOGGER.info(f"Handling event: {event.type} -> {action.__class__.__name__}")
        action_result = action.run(event=event, id=id)
        return action_result
