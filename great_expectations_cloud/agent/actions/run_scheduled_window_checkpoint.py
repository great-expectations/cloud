from __future__ import annotations

from urllib.parse import urljoin

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.actions.run_window_checkpoint import run_window_checkpoint
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    RunScheduledWindowCheckpointEvent,
)


class RunScheduledWindowCheckpointAction(AgentAction[RunScheduledWindowCheckpointEvent]):
    @override
    def run(self, event: RunScheduledWindowCheckpointEvent, id: str) -> ActionResult:
        expectation_parameters_url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._organization_id}/checkpoints/{event.checkpoint_id}/expectation-parameters",
        )
        return run_window_checkpoint(
            context=self._context,
            event=event,
            id=id,
            auth_key=self._auth_key,
            url=expectation_parameters_url,
        )


register_event_action("1", RunScheduledWindowCheckpointEvent, RunScheduledWindowCheckpointAction)
