from __future__ import annotations

from enum import Enum


class RejectionReason(Enum):
    """Enum representing reasons for expectation rejection."""

    INVALID_SQL = "invalid_sql"
    INVALID_PYDANTIC_CONSTRUCTION = "invalid_pydantic_construction"
    INVALID_CONSTRUCTION = "invalid_construction"
    OTHER = "other"


class ExpectAIMetrics:
    @staticmethod
    def emit_expectation_validated(expectation_type: str) -> None:
        pass  # No-op in public agent

    @staticmethod
    def emit_expectation_rejected(expectation_type: str, reason: RejectionReason) -> None:
        pass  # No-op in public agent
