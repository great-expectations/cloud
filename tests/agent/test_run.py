from __future__ import annotations

import logging

from great_expectations_cloud.agent import run_agent


def test_run_calls_gx_agent(mocker):
    agent = mocker.patch("great_expectations_cloud.agent.run.GXAgent")
    run_agent(logging.getLogger(__name__))
    agent.assert_called_with()
    agent().run.assert_called_with()
