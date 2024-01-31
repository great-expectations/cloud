from __future__ import annotations

from great_expectations_cloud.agent import GXAgent
from great_expectations_cloud.agent.agent import GXAgentConfigError


def run_agent() -> None:
    """Run an instance of the GX Agent."""
    try:
        agent = GXAgent()
        agent.run()
    except GXAgentConfigError as error:
        print(error)
