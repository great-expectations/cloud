from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any, NamedTuple

import pytest
from great_expectations import __version__ as gx_version
from packaging.version import Version
from typing_extensions import override

from great_expectations_cloud.agent.message_service.subscriber import (
    EventContext,
    OnMessageCallback,
    Subscriber,
)
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


class FakeMessagePayload(NamedTuple):
    """
    Fake message payload for testing purposes
    The real payload is a JSON string which must be parsed into an Event
    """

    event: Event
    correlation_id: str


class FakeSubscriber(Subscriber):
    test_queue: deque[FakeMessagePayload | tuple[Event, str]]

    def __init__(
        self, client: Any, test_events: list[FakeMessagePayload | tuple[Event, str]] | None = None
    ):
        self.client = client
        self.test_queue = deque()
        if test_events:
            self.test_queue.extend(test_events)

    @override
    def consume(self, queue: str, on_message: OnMessageCallback) -> None:
        LOGGER.info(f"{on_message=}")
        while self.test_queue:
            # TODO: correleation_id needs to be part of the queue message
            event, correlation_id = self.test_queue.pop()
            LOGGER.info(f"FakeSubscriber.consume() received -> {event!r}")
            event_context = EventContext(
                event=event,
                correlation_id=correlation_id,
                processed_successfully=lambda: None,
                processed_with_failures=lambda: None,
                redeliver_message=lambda: None,
            )
            on_message(event_context)
            # allow time for thread to process the event
            time.sleep(0.2)

    @override
    def close(self) -> None:
        LOGGER.info(f"{self.__class__.__name__}.close() called")


@pytest.fixture
def fake_subscriber(mocker) -> FakeSubscriber:
    """Patch the agent.Subscriber constuctor to return a FakeSubscriber that pulls from a `.test_queue` in-memory list."""
    subscriber = FakeSubscriber(client=object())
    mocker.patch("great_expectations_cloud.agent.agent.Subscriber", return_value=subscriber)
    return subscriber
