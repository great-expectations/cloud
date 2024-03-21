from __future__ import annotations

import warnings


class GXAgentUserWarning(UserWarning): ...


def warn_unknown_event(event_type: str) -> None:
    warnings.warn(
        message=f"The version of the GX Agent you are using does not support this job ({event_type}). Please upgrade to latest.",
        category=GXAgentUserWarning,
        stacklevel=2,
    )
