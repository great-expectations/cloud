from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.actions.run_checkpoint import run_checkpoint
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import RunWindowCheckpointEvent

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class RunWindowCheckpointAction(AgentAction[RunWindowCheckpointEvent]):
    @override
    def run(self, event: RunWindowCheckpointEvent, id: str) -> ActionResult:
        return run_window_checkpoint(self._context, event, id)


register_event_action("1", RunWindowCheckpointEvent, RunWindowCheckpointAction)


def run_window_checkpoint(
    context: CloudDataContext,
    event: RunWindowCheckpointEvent,
    id: str,
) -> ActionResult:
    return run_checkpoint(context, event, id)
