from __future__ import annotations

import warnings

import pytest


class GXAgentUserWarning(UserWarning):
    ...


def warn_unknown_event(event_type: str) -> None:
    with pytest.warns(GXAgentUserWarning):
        warnings.warn(
            message=f"The version of the GX Agent you are using does not support this job ({event_type}). Please upgrade to latest.",
            category=GXAgentUserWarning,
            stacklevel=2,
        )
