from __future__ import annotations

from great_expectations_cloud.agent import run_agent


def test_run_calls_gx_agent(mocker):
    agent = mocker.patch("great_expectations_cloud.agent.run.GXAgent")
    run_agent()
    agent.assert_called_with()
    agent().run.assert_called_with()
