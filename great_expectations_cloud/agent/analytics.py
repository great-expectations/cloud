from __future__ import annotations

from enum import Enum


class RejectionReason(Enum):
    """Enum representing reasons for expectation rejection."""

    INVALID_SQL = "invalid_sql"
    INVALID_PYDANTIC_CONSTRUCTION = "invalid_pydantic_construction"
    INVALID_CONSTRUCTION = "invalid_construction"
    OTHER = "other"


class AgentAnalytics:
    """Base analytics class."""

    def emit_expectation_validated(self, expectation_type: str) -> None:
        pass  # No-op in public agent

    def emit_expectation_rejected(self, expectation_type: str, reason: RejectionReason) -> None:
        pass  # No-op in public agent
