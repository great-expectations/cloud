#!/usr/bin/env python
"""Debug-enabled entrypoint for GX Agent. Starts debugpy then runs gx-agent."""

import debugpy
from pydantic.v1 import BaseSettings


class _DebugSettings(BaseSettings):
    debugpy_wait_for_client: bool = False


PORT = 6103
debugpy.listen(("0.0.0.0", PORT))  # noqa: S104
print(f"Debugpy listening on port {PORT}.")

if _DebugSettings().debugpy_wait_for_client:
    debugpy.wait_for_client()
    print("Debugger attached.")

from great_expectations_cloud.agent.cli import main  # noqa: E402

main()
