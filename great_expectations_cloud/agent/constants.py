from __future__ import annotations

import enum
from typing import Final


class HeaderName(str, enum.Enum):
    USER_AGENT = "User-Agent"
    CORRELATION_ID = "Correlation-ID"


USER_AGENT_HEADER: Final = "gx-agent"

__all__ = ["USER_AGENT_HEADER"]
