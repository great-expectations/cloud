from __future__ import annotations

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.models import (
    UnknownEvent,
)


class UnknownEventAction(AgentAction[UnknownEvent]):
    @override
    def run(self, event: UnknownEvent, id: str) -> ActionResult:
        # noop
        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[],
        )
