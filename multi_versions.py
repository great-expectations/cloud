from __future__ import annotations

import great_expectations as gx

ctx1 = gx.get_context(mode="ephemeral")
print("Standard", gx.__version__)
print(type(ctx1))

print("\n=====================================\n")
import great_expectations_v0 as gx_v0

ctx2 = gx_v0.get_context(mode="ephemeral")
print("V0", gx_v0.__version__ )
print(type(ctx2))
