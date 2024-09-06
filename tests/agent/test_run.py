from __future__ import annotations

from great_expectations_cloud.agent.run import run_agent


def test_run_inits_gx_agent(mocker):
    agent = mocker.patch("great_expectations_cloud.agent.run.GXAgent")
    asyncio = mocker.patch("great_expectations_cloud.agent.run.asyncio")
    run_agent()
    agent.assert_called_once_with(config=mocker.ANY)  # gets passed the config
    asyncio.run.assert_called_once_with(mocker.ANY)  # gets passed the app.run() call
