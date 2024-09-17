from __future__ import annotations

import logging
from typing import Final

from great_expectations_cloud.agent import GXAgent
from great_expectations_cloud.agent.agent import GXAgentConfigError

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


def run_agent() -> None:
    """Run an instance of the GX Agent."""
    try:
        agent = GXAgent()
        agent.run()
    except GXAgentConfigError as error:
        # catch error to avoid stacktrace printout
        LOGGER.error(error)  # noqa: TRY400 # intentionally avoiding logging stacktrace


def get_version() -> str:
    return GXAgent.get_current_gx_agent_version()
