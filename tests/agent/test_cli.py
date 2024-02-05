from __future__ import annotations

import subprocess

import pytest


@pytest.mark.parametrize(
    "cmd",
    [
        "--help",
        "-h",
    ],
)
def test_command_retuns_zero_exit_code(cmd: str):
    cli_cmds = ["gx-agent", cmd]
    print(f"Testing command:\n  {' '.join(cli_cmds)}\n")
    cmplt_process = subprocess.run(cli_cmds, check=False, timeout=6.0)  # noqa: S603 # trusted input
    print(cmplt_process.stdout)
    assert cmplt_process.returncode == 0


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
