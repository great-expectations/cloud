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
    @override
    def run(self, event: RunScheduledCheckpointEvent, id: str) -> ActionResult:
        return run_checkpoint(self._context, event, id)


register_event_action("1", RunScheduledCheckpointEvent, RunScheduledCheckpointAction)
