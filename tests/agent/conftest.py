from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any, Iterable, NamedTuple

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
    """
    Fake Subscriber that pulls from a `.test_queue` in-memory deque (double-ended queue).

    The deque is populated with FakeMessagePayloads or tuples of (Event, correlation_id) rather than JSON strings/bytes.
    The real Subscriber pulls from a RabbitMQ queue and receives JSON strings/bytes which must be parsed into an Event.
    """

    test_queue: deque[FakeMessagePayload | tuple[Event, str]]

    def __init__(
        self,
        client: Any,
        test_events: Iterable[FakeMessagePayload | tuple[Event, str]] | None = None,
    ):
        self.client = client
        self.test_queue = deque()
        if test_events:
            self.test_queue.extend(test_events)

    @override
    def consume(self, queue: str, on_message: OnMessageCallback) -> None:
        LOGGER.info(f"{self.__class__.__name__}.consume() called")
        while self.test_queue:
            event, correlation_id = self.test_queue.pop()
            LOGGER.info(f"FakeSubscriber.consume() received -> {event!r}")
            event_context = EventContext(
                event=event,
                correlation_id=correlation_id,
                processed_successfully=lambda: None,
                processed_with_failures=lambda: None,
                redeliver_message=lambda: None,  # type: ignore[arg-type,return-value] # should be Coroutine
            )
            on_message(event_context)
            # allow time for thread to process the event
            # TODO: better solution for this might be to make the FakeSubscriber not run in a separate thread at all
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
