from __future__ import annotations

import logging
from typing import Final

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.agent_warnings import warn_unknown_event
from great_expectations_cloud.agent.models import (
    UnknownEvent,
)

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class UnknownEventAction(AgentAction[UnknownEvent]):
    @override
    def run(self, event: UnknownEvent, id: str) -> ActionResult:
        warn_unknown_event(event.type)
        # noop
        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[],
        )
