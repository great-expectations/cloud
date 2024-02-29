from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Final

import great_expectations as gx
from packaging.version import Version
from packaging.version import parse as parse_version

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

    from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction
    from great_expectations_cloud.agent.models import Event


LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


# Map of event types to actions, specific to the GX Core major version.
# Built via registering using register_event_action(). For example:
# _EVENT_ACTION_MAP = {
#     "0": {
#         RunOnboardingDataAssistantEvent.__name__: RunOnboardingDataAssistantAction,
#         RunMissingnessDataAssistantEvent.__name__: RunMissingnessDataAssistantAction,
#         ListTableNamesEvent.__name__: ListTableNamesAction,
#         RunCheckpointEvent.__name__: RunCheckpointAction,
#         RunColumnDescriptiveMetricsEvent.__name__: ColumnDescriptiveMetricsAction,
#         DraftDatasourceConfigEvent.__name__: DraftDatasourceConfigAction,
#     },
#     "1": {
#         # No events implemented yet for GX Core v1
#         # Suggested naming convention, add version suffix "V1" e.g.: RunCheckpointEventV1
#     },
# }
_EVENT_ACTION_MAP: dict[str, dict[str, type[AgentAction]]] = {}


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def get_event_action(self, event: Event) -> AgentAction[Any]:
        """Get the action that should be run for the given event."""
        action_map = _EVENT_ACTION_MAP.get(_GX_MAJOR_VERSION)
        if action_map is None:
            raise NoVersionImplementationError(
                f"No event action map implemented for GX Core major version {_GX_MAJOR_VERSION}"
            )
        action_class = action_map.get(event.__class__.__name__)
        if action_class is None:
            raise UnknownEventError("Unknown message received - cannot process.")
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


def _get_major_version(version: str) -> str:
    """Get major version as a string. For example, "0.18.0" -> "0"."""
    parsed: Version = parse_version(version)
    return str(parsed.major)


_GX_MAJOR_VERSION = _get_major_version(gx.__version__)


def register_event_action(
    version: str, event_type: type[Event], action_class: type[AgentAction[Any]]
) -> None:
    """Register an event type to an action class."""
    version_in_map = (
        _EVENT_ACTION_MAP.get(version) is not None
    )  # check for None explicitly since version can be empty
    if version_in_map and (event_type in _EVENT_ACTION_MAP[version]):
        raise ValueError(f"Event type {event_type} already registered for version {version}.")

    if version not in _EVENT_ACTION_MAP:
        _EVENT_ACTION_MAP[version] = {}
    event_type_str = event_type.__name__
    _EVENT_ACTION_MAP[version][event_type_str] = action_class
