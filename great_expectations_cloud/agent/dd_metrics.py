from __future__ import annotations

from enum import Enum


class RejectionReason(Enum):
    """Enum representing reasons for expectation rejection."""

    INVALID_SQL = "invalid_sql"
    INVALID_PYDANTIC_CONSTRUCTION = "invalid_pydantic_construction"
    INVALID_CONSTRUCTION = "invalid_construction"
    OTHER = "other"


class ExpectAIAnalytics:
    """Base analytics class. No-op in the open-source agent.

    Downstream consumers (e.g. gx-runner) should subclass this and
    override methods to emit real analytics.
    """

    def emit_expectation_validated(self, expectation_type: str) -> None:
        pass  # No-op in public agent

    def emit_expectation_rejected(self, expectation_type: str, reason: RejectionReason) -> None:
        pass  # No-op in public agent
