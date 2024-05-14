"""
Verify that both versions are installed

pip install --pre great_expectations
pip install /path/to/local/great_expectations

pip list | grep great
"""
from __future__ import annotations

import great_expectations as gx

print(f"Standard version={gx.__version__}")
ctx1 = gx.get_context(mode="ephemeral")
print(type(ctx1))

print("\n=====================================\n")
import great_expectations_v0 as gx_v0

print(f"V0 version={gx_v0.__version__}")
ctx2 = gx_v0.get_context(mode="ephemeral")
print(type(ctx2))
