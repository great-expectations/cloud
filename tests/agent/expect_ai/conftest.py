from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from great_expectations.data_context import CloudDataContext


@pytest.fixture
def mock_context():
    """Mock CloudDataContext using create_autospec for stricter mocking."""
    return create_autospec(CloudDataContext)
