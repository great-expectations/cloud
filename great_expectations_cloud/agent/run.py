from __future__ import annotations

from great_expectations_cloud.agent import GXAgent


def run_agent() -> None:
    """Run an instance of the GXAgent."""
    agent = GXAgent()
    agent.run()

def get_version() -> str:
    return GXAgent.get_current_gx_agent_version()
