from __future__ import annotations

import logging
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Final

from great_expectations.compatibility import pydantic

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
from great_expectations_cloud.agent.actions.unknown import UnknownEventAction
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    Event,
    ListTableNamesEvent,
    RunCheckpointEvent,
    RunColumnDescriptiveMetricsEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
    UnknownEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

    from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction


LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    _EVENT_TO_ACTION_MAP: ClassVar[dict[type[Event], type[AgentAction[Any]]]] = {
        RunOnboardingDataAssistantEvent: RunOnboardingDataAssistantAction,
        RunMissingnessDataAssistantEvent: RunMissingnessDataAssistantAction,
        ListTableNamesEvent: ListTableNamesAction,
        RunCheckpointEvent: RunCheckpointAction,
        RunColumnDescriptiveMetricsEvent: ColumnDescriptiveMetricsAction,
        DraftDatasourceConfigEvent: DraftDatasourceConfigAction,
    }

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def get_event_action(self, event: Event) -> AgentAction[Any]:
        """Get the action that should be run for the given event."""

        event_class = event.__class__
        if action := self._EVENT_TO_ACTION_MAP.get(event_class):
            return action(context=self._context)
        # Building an UnknownEventAction allows noop
        return UnknownEventAction(context=self._context)

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""
        action = self.get_event_action(event=event)
        LOGGER.info(f"Handling event: {event.type} -> {action.__class__.__name__}")
        action_result = action.run(event=event, id=id)
        return action_result

    @classmethod
    def parse_event_from(cls, msg_body: bytes) -> Event:
        try:
            event: Event = pydantic.parse_raw_as(Event, msg_body)
            return event
        except (pydantic.ValidationError, JSONDecodeError):
            LOGGER.error("Unable to parse event type", extra={"msg_body": f"{msg_body}"})
            return UnknownEvent()


class UnknownEventError(Exception):
    ...
