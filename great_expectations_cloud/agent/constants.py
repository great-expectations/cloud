from __future__ import annotations

import enum
from typing import Final


class HeaderName(str, enum.Enum):
    USER_AGENT = "User-Agent"
    AGENT_JOB_ID = "Agent-Job-Id"


USER_AGENT_HEADER: Final = "gx-agent"

__all__ = ["USER_AGENT_HEADER"]


JOB_TIMEOUT_IN_SECONDS: Final = 60 * 60 * 1  # 1 hour
