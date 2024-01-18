from __future__ import annotations

import logging
from collections import deque
from typing import Any

import pytest
from great_expectations import __version__ as gx_version
from packaging.version import Version
from typing_extensions import override

from great_expectations_cloud.agent.message_service.subscriber import OnMessageCallback, Subscriber
from great_expectations_cloud.agent.models import Event

LOGGER = logging.getLogger(__name__)


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


class FakeSubscriber(Subscriber):
    test_queue: deque[Event]

    def __init__(self, client: Any, test_events: list[Event] | None = None):
        self.client = client
        self.test_queue = deque()
        if test_events:
            self.test_queue.extend(test_events)

    @override
    def consume(self, queue: str, on_message: OnMessageCallback) -> None:
        print(on_message)
        while self.test_queue:
            # TODO: parse and handle the event
            msg = self.test_queue.pop()
            LOGGER.info(f"FakeSubscriber.consume() received {msg}")

    @override
    def close(self) -> None:
        LOGGER.info(f"{self.__class__.__name__}.close() called")


@pytest.fixture
def fake_subscriber(mocker) -> FakeSubscriber:
    """Patch the agent.Subscriber constuctor to return a FakeSubscriber that pulls from a `.test_queue` in-memory list."""
    subscriber = FakeSubscriber(client=object())
    mocker.patch("great_expectations_cloud.agent.agent.Subscriber", return_value=subscriber)
    return subscriber
