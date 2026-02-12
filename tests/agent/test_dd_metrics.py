from __future__ import annotations

import unittest

import pytest

from great_expectations_cloud.agent.dd_metrics import ExpectAIMetrics, RejectionReason


@pytest.mark.unit
class TestExpectAIMetrics(unittest.TestCase):
    """Test the stubbed ExpectAIMetrics implementation.

    Since dd_metrics is stubbed out in the open-source agent,
    these tests simply verify the methods can be called without errors.
    """

    def test_emit_expectation_validated(self):
        # Arrange
        metrics = ExpectAIMetrics()
        expectation_type = "TestExpectationType"

        # Act - should not raise an exception
        metrics.emit_expectation_validated(expectation_type)

        # Assert - method completes without error
        # No assertion needed since we're just testing it doesn't raise

    def test_emit_expectation_rejected(self):
        # Arrange
        metrics = ExpectAIMetrics()
        expectation_type = "TestExpectationType"
        reason = RejectionReason.INVALID_SQL

        # Act - should not raise an exception
        metrics.emit_expectation_rejected(expectation_type, reason)

        # Assert - method completes without error
        # No assertion needed since we're just testing it doesn't raise

    def test_all_rejection_reasons_callable(self):
        """Test that all rejection reasons can be used with emit_expectation_rejected."""
        metrics = ExpectAIMetrics()
        expectation_type = "TestExpectationType"

        # Act - should not raise an exception for any rejection reason
        for reason in RejectionReason:
            metrics.emit_expectation_rejected(expectation_type, reason)

        # Assert - method completes without error for all reasons
        # No assertion needed since we're just testing it doesn't raise


if __name__ == "__main__":
    unittest.main()
