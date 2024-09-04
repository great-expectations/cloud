from __future__ import annotations

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.actions.run_checkpoint import run_checkpoint
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    RunWindowCheckpointEvent,
)


class RunWindowCheckpointAction(AgentAction[RunWindowCheckpointEvent]):
    # TODO: New actions need to be created that are compatible with GX v1 and registered for v1.
    #  This action is registered for v0, see register_event_action()

    @override
    def run(self, event: RunWindowCheckpointEvent, id: str) -> ActionResult:
        # TODO: https://greatexpectations.atlassian.net/browse/ZELDA-922
        #  This currently only runs a normal checkpoint. Logic for window checkpoints needs to be added (e.g. call the backend to get the params and then construct the evaluation_parameters before passing them into context.run_checkpoint()) One way we can do this via a param in `run_checkpoint()` that takes a function to build the evaluation_parameters, defaulting to a noop for the other checkpoint action types.
        return run_checkpoint(self._context, event, id)


register_event_action("0", RunWindowCheckpointEvent, RunWindowCheckpointAction)
