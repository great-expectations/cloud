#!/usr/bin/env python
"""Debug-enabled entrypoint for GX Agent. Starts debugpy then runs gx-agent."""

import os

import debugpy

PORT = 6124
debugpy.listen(("0.0.0.0", PORT))
print(f"Debugpy listening on port {PORT}.")

if os.environ.get("DEBUGPY_WAIT_FOR_CLIENT", "false").lower() == "true":
    debugpy.wait_for_client()
    print("Debugger attached.")

from great_expectations_cloud.agent.cli import main  # noqa: E402

main()
