from __future__ import annotations

import logging
import socket
from typing import Final

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.actions.run_checkpoint import run_checkpoint
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import RunScheduledCheckpointEvent

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class RunScheduledCheckpointAction(AgentAction[RunScheduledCheckpointEvent]):
    @override
    def run(self, event: RunScheduledCheckpointEvent, id: str) -> ActionResult:
        hostname = socket.gethostname()
        log_extra = {
            "correlation_id": id,
            "checkpoint_id": str(event.checkpoint_id),
            "schedule_id": str(event.schedule_id),
            "hostname": hostname,
        }
        LOGGER.debug(
            "Proceeding to run checkpoint",
            extra={**log_extra, "has_expectation_parameters": False},
        )
        return run_checkpoint(self._context, event, id, expectation_parameters=None)


register_event_action("1", RunScheduledCheckpointEvent, RunScheduledCheckpointAction)
