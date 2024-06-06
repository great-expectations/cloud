from __future__ import annotations

import os
import pathlib
import subprocess

import pytest

from great_expectations_cloud.agent.cli import load_dotenv


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


def test_load_dotenv(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("GX_CLOUD_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("GX_CLOUD_ORGANIZATION_ID", raising=False)

    env_file = pathlib.Path("example.env")
    loaded_env_vars = load_dotenv(env_file)
    assert loaded_env_vars == {"GX_CLOUD_ACCESS_TOKEN", "GX_CLOUD_ORGANIZATION_ID"}

    assert os.environ["GX_CLOUD_ACCESS_TOKEN"] == "<YOUR_ACCESS_TOKEN>"
    assert os.environ["GX_CLOUD_ORGANIZATION_ID"] == "<YOUR_ORGANIZATION_ID>"


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
