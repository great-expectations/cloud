from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Final

import great_expectations as gx
from packaging.version import LegacyVersion, Version
from packaging.version import parse as parse_version

from great_expectations_cloud.agent.actions import (
    ColumnDescriptiveMetricsAction,
    ListTableNamesAction,
    RunCheckpointAction,
    RunMissingnessDataAssistantAction,
    RunOnboardingDataAssistantAction,
)
from great_expectations_cloud.agent.actions.draft_datasource_config_action import (
    DraftDatasourceConfigAction,
)
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    ListTableNamesEvent,
    RunCheckpointEvent,
    RunColumnDescriptiveMetricsEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

    from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction
    from great_expectations_cloud.agent.models import Event

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


# Map of event types to actions, specific to the GX Core major version.
# Built via registering using register_event_action(). For example:
# _EVENT_ACTION_MAP = {
#     "0": {
#         "EventName": AgentActionClass,
#         "RunCheckpointEvent": RunCheckpointAction,
#     },
#     "1": {
#         # No events implemented yet for GX Core v1
#         # Suggested naming convention, add version suffix "V1" e.g.: RunCheckpointEventV1
#     },
# }
_EVENT_ACTION_MAP: dict[str, dict[str, type[AgentAction[Any]]]] = defaultdict(dict)


def register_event_action(
    version: str, event_type: type[Event], action_class: type[AgentAction[Any]]
) -> None:
    """Register an event type to an action class."""
    if version in _EVENT_ACTION_MAP and event_type.__name__ in _EVENT_ACTION_MAP[version]:
        raise ValueError(
            f"Event type {event_type.__name__} already registered for version {version}."
        )
    event_type_str = event_type.__name__
    _EVENT_ACTION_MAP[version][event_type_str] = action_class
    LOGGER.debug(
        f"Registered event action: {event_type_str} -> {action_class.__name__} (version {version})"
    )


register_event_action("0", RunCheckpointEvent, RunCheckpointAction)
register_event_action("0", DraftDatasourceConfigEvent, DraftDatasourceConfigAction)
register_event_action("0", RunColumnDescriptiveMetricsEvent, ColumnDescriptiveMetricsAction)
register_event_action("0", ListTableNamesEvent, ListTableNamesAction)
register_event_action("0", RunMissingnessDataAssistantEvent, RunMissingnessDataAssistantAction)
register_event_action("0", RunOnboardingDataAssistantEvent, RunOnboardingDataAssistantAction)


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def get_event_action(self, event: Event) -> AgentAction[Any]:
        """Get the action that should be run for the given event."""
        print("_EVENT_ACTION_MAP:", _EVENT_ACTION_MAP)
        action_map = _EVENT_ACTION_MAP.get(_GX_MAJOR_VERSION)
        if action_map is None:
            raise NoVersionImplementationError(
                f"No event action map implemented for GX Core major version {_GX_MAJOR_VERSION}"
            )
        action_class = action_map.get(_get_event_name(event))
        if action_class is None:
            raise UnknownEventError(
                f'Unknown message received: "{_get_event_name(event)}" - cannot process.'
            )
        return action_class(context=self._context)

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""
        action = self.get_event_action(event=event)
        LOGGER.info(f"Handling event: {event.type} -> {action.__class__.__name__}")
        action_result = action.run(event=event, id=id)
        return action_result


class UnknownEventError(Exception):
    ...


class NoVersionImplementationError(Exception):
    ...


class InvalidVersionError(Exception):
    ...


def _get_major_version(version: str) -> str:
    """Get major version as a string. For example, "0.18.0" -> "0"."""
    parsed: Version | LegacyVersion = parse_version(version)
    if not isinstance(parsed, Version):
        raise InvalidVersionError(f"Invalid version: {version}")
    return str(parsed.major)


_GX_MAJOR_VERSION = _get_major_version(gx.__version__)


def _get_event_name(event: Event) -> str:
    try:
        return str(event.__name__)
    except AttributeError:
        return str(event.__class__.__name__)
