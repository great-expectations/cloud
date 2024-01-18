from __future__ import annotations

import logging

import pytest
from great_expectations import __version__ as gx_version
from packaging.version import Version


@pytest.fixture
def mock_gx_version_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mock the response from pypi.org for the great_expectations package"""

    def _mock_get_lastest_version_from_pypi(self) -> Version:
        return Version(gx_version)

    monkeypatch.setattr(
        "great_expectations.data_context._version_checker._VersionChecker._get_latest_version_from_pypi",
        _mock_get_lastest_version_from_pypi,
        raising=True,
    )


@pytest.fixture
def gx_logging_info(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Set up logging for the gx package"""
    with caplog.at_level(logging.INFO, logger="great_expectations"):
        yield caplog
