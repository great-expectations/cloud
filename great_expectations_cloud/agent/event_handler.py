from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

    from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction
    from great_expectations_cloud.agent.models import (
        Event,
    )


LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def get_event_action(self, event: Event) -> AgentAction[Any]:
        return event.action(context=self._context)

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""
        action = event.action(context=self._context)
        # THis can be unknown event now!
        LOGGER.info(f"Handling event: {event.type} -> {action.__class__.__name__}")
        action_result = action.run(event=event, id=id)
        return action_result
