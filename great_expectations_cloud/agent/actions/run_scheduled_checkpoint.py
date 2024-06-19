from __future__ import annotations

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.actions.run_checkpoint import run_checkpoint
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    RunScheduledCheckpointEvent,
)


class RunScheduledCheckpointAction(AgentAction[RunScheduledCheckpointEvent]):
    # TODO: New actions need to be created that are compatible with GX v1 and registered for v1.
    #  This action is registered for v0, see register_event_action()

    @override
    def run(self, event: RunScheduledCheckpointEvent, id: str) -> ActionResult:
        return run_checkpoint(self._context, event, id)


register_event_action("0", RunScheduledCheckpointEvent, RunScheduledCheckpointAction)
