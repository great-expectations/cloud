from __future__ import annotations

import logging
import warnings
from typing import Final

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.models import (
    UnknownEvent,
)

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class UnknownEventAction(AgentAction[UnknownEvent]):
    @override
    def run(self, event: UnknownEvent, id: str) -> ActionResult:
        warnings.warn(
            "The version of the GX Agent you are using does not support this job ({event.type}). Please upgrade to latest.",
            stacklevel=2,
        )
        # noop
        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[],
        )
