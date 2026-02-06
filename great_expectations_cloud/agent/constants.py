from __future__ import annotations

from enum import StrEnum
from typing import Final


class HeaderName(StrEnum):
    USER_AGENT = "User-Agent"
    AGENT_JOB_ID = "Agent-Job-Id"


USER_AGENT_HEADER: Final = "gx-agent"

__all__ = ["USER_AGENT_HEADER"]
