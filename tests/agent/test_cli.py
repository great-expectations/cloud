from __future__ import annotations

import os
import pathlib
import subprocess
from typing import TYPE_CHECKING, Generator

import pytest

from great_expectations_cloud.agent.cli import load_dotenv, main

if TYPE_CHECKING:
    from pytest_mock import MockerFixture, MockType


@pytest.fixture
def mock_agent_run(mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch) -> MockType:
    run_patch = mocker.patch("great_expectations_cloud.agent.run_agent")
    return run_patch


@pytest.fixture
def clean_gx_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """
    Cleanup the GX_CLOUD environment variables before and after test run.

    GX_CLOUD_ACCESS_TOKEN
    GX_CLOUD_ORGANIZATION_ID
    GX_BASE_URL
    """
    env_vars = ["GX_CLOUD_ACCESS_TOKEN", "GX_CLOUD_ORGANIZATION_ID", "GX_BASE_URL"]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)
    yield None
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)


@pytest.mark.parametrize(
    "args",
    [
        ("--help",),
        ("--version",),
        ("--env-file", "example.env"),
        ("--log-level", "DEBUG"),
        ("--json-log",),
    ],
    ids=lambda x: " ".join(x),
)
def test_main(monkeypatch: pytest.MonkeyPatch, mock_agent_run: MockType, args: tuple[str, ...]):
    """Ensure that the main function runs without error."""
    monkeypatch.delenv("GX_CLOUD_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("GX_CLOUD_ORGANIZATION_ID", raising=False)

    monkeypatch.setattr("sys.argv", ["gx-agent", *args])

    try:
        main()
    except SystemExit as exit:
        # as long as the exit code is 0, we are good
        assert exit.code == 0


@pytest.mark.parametrize(
    "cmd",
    [
        "--help",
        "-h",
        "--version",
    ],
)
def test_command_retuns_zero_exit_code(cmd: str):
    cli_cmds = ["gx-agent", cmd]
    print(f"Testing command:\n  {' '.join(cli_cmds)}\n")
    cmplt_process = subprocess.run(cli_cmds, check=False, timeout=6.0)  # noqa: S603 # trusted input
    print(cmplt_process.stdout)
    assert cmplt_process.returncode == 0


def test_custom_log_tags_failure():
    cli_cmds = ["gx-agent", "--custom-log-tags", "{'badJSON"]
    print(f"Testing command:\n  {' '.join(cli_cmds)}\n")
    cmplt_process = subprocess.run(cli_cmds, check=False, timeout=6.0)  # noqa: S603 # trusted input
    print(cmplt_process.stdout)
    assert cmplt_process.returncode != 0


def test_load_dotenv(clean_gx_env: None):
    env_file = pathlib.Path("example.env")
    loaded_env_vars = load_dotenv(env_file)
    assert loaded_env_vars == {"GX_CLOUD_ACCESS_TOKEN", "GX_CLOUD_ORGANIZATION_ID"}

    assert os.environ["GX_CLOUD_ACCESS_TOKEN"] == "<YOUR_ACCESS_TOKEN>"
    assert os.environ["GX_CLOUD_ORGANIZATION_ID"] == "<YOUR_ORGANIZATION_ID>"


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
