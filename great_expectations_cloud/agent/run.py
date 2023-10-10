from great_expectations_cloud.agent import GXAgent


def run_agent() -> None:
    """Run an instance of the GXAgent."""
    agent = GXAgent()
    agent.run()
