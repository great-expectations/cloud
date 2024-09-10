from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Final
from uuid import UUID

import great_expectations as gx
from packaging.version import Version
from packaging.version import parse as parse_version
from pydantic import v1 as pydantic_v1

from great_expectations_cloud.agent.actions.unknown import UnknownEventAction
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.models import (
    Event,
    UnknownEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

    from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction


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
        raise EventAlreadyRegisteredError(event_type_name=event_type.__name__, version=version)
    event_type_str = event_type.__name__
    _EVENT_ACTION_MAP[version][event_type_str] = action_class
    LOGGER.debug(
        f"Registered event action: {event_type_str} -> {action_class.__name__} (version {version})"
    )


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def get_event_action(
        self, event: Event, base_url: str, auth_key: str, organization_id: UUID
    ) -> AgentAction[Any]:
        """Get the action that should be run for the given event."""

        if not self._check_event_organization_id(event, organization_id):
            # Making message more generic
            raise GXAgentError("Unable to process job. Invalid input.")  # noqa: TRY003

        action_map = _EVENT_ACTION_MAP.get(_GX_MAJOR_VERSION)
        if action_map is None:
            raise NoVersionImplementationError(version=_GX_MAJOR_VERSION)
        action_class = action_map.get(_get_event_name(event))
        if action_class is None:
            action_class = UnknownEventAction
        return action_class(
            context=self._context,
            base_url=base_url,
            organization_id=organization_id,
            auth_key=auth_key,
        )

    def handle_event(  # Refactor opportunity
        self, event: Event, id: str, base_url: str, auth_key: str, organization_id: UUID
    ) -> ActionResult:
        start_time = datetime.now(tz=timezone.utc)
        """Transform an Event into an ActionResult."""
        action = self.get_event_action(
            event=event, base_url=base_url, auth_key=auth_key, organization_id=organization_id
        )
        LOGGER.info(f"Handling event: {event.type} -> {action.__class__.__name__}")
        action_result = action.run(event=event, id=id)
        end_time = datetime.now(tz=timezone.utc)
        action_result.job_duration = end_time - start_time
        return action_result

    @classmethod
    def parse_event_from_dict(cls, msg_body: dict[str, Any]) -> Event:
        try:
            event: Event = pydantic_v1.parse_obj_as(Event, msg_body)  # type: ignore[arg-type] # FIXME
        except pydantic_v1.ValidationError:
            # Log as dict
            LOGGER.exception("Unable to parse event type", extra={"msg_body": msg_body})
            return UnknownEvent()

        return event

    @staticmethod
    def _check_event_organization_id(event: Event, organization_id: UUID) -> bool:
        """Check if the organization_id in the event matches the given organization_id.

        This prevents processing events that are not intended for the current organization, and potentially
        leaking sensitive information across organizations.
        """
        if hasattr(event, "organization_id") and event.organization_id != organization_id:
            return False
        return True


class EventError(Exception): ...


class UnknownEventError(EventError):
    def __init__(self, event_name: str):
        super().__init__(f'Unknown message received: "{event_name}" - cannot process.')


class NoVersionImplementationError(EventError):
    def __init__(self, version: str | Version):
        super().__init__(f"No event action map implemented for GX Core major version {version}.")


class InvalidVersionError(EventError):
    def __init__(self, version: str | Version):
        super().__init__(f"Invalid version: {version}")


class EventAlreadyRegisteredError(EventError):
    def __init__(self, event_type_name: str, version: str | Version):
        super().__init__(f"Event type {event_type_name} already registered for version {version}.")


def _get_major_version(version: str) -> str:
    """Get major version as a string from version as a string. For example, "0.18.0" -> "0"."""
    parsed: Version = parse_version(version)
    if not isinstance(parsed, Version):
        raise InvalidVersionError(version)
    return str(parsed.major)


version = gx.__version__  # type: ignore[attr-defined] # TODO: fix this
_GX_MAJOR_VERSION = _get_major_version(str(version))


def _get_event_name(event: Event) -> str:
    try:
        return str(event.__name__)  # type: ignore[union-attr] # FIXME
    except AttributeError:
        return event.__class__.__name__
