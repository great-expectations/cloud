from __future__ import annotations

import logging
import time
import uuid
from collections import deque
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, TypedDict

import pytest
from great_expectations import (
    __version__ as gx_version,
)
from great_expectations.data_context import CloudDataContext
from packaging.version import Version
from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction
from great_expectations_cloud.agent.event_handler import (
    register_event_action,
)
from great_expectations_cloud.agent.message_service.subscriber import (
    EventContext,
    OnMessageCallback,
    Subscriber,
)
from great_expectations_cloud.agent.models import Event, EventBase

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

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


@pytest.fixture
def mock_context(mocker: MockerFixture):
    """Returns a `MagicMock` of a `CloudDataContext` for testing purposes."""
    return mocker.MagicMock(autospec=CloudDataContext)


class FakeMessagePayload(NamedTuple):
    """
    Fake message payload for testing purposes
    The real payload is a JSON string which must be parsed into an Event
    """

    event: Event | DummyEvent
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
                event=event,  # type: ignore[arg-type] # In tests, could be a DummyEvent
                correlation_id=correlation_id,
                processed_successfully=lambda: None,
                processed_with_failures=lambda: None,
                redeliver_message=lambda: None,  # type: ignore[arg-type,return-value] # should be Coroutine
            )
            on_message(event_context)
            # allow time for thread to process the event
            # TODO: better solution for this might be to make the FakeSubscriber not run in a separate thread at all
            time.sleep(0.4)

    @override
    def close(self) -> None:
        LOGGER.info(f"{self.__class__.__name__}.close() called")


@pytest.fixture
def fake_subscriber(mocker) -> FakeSubscriber:
    """Patch the agent.Subscriber constuctor to return a FakeSubscriber that pulls from a `.test_queue` in-memory list."""
    subscriber = FakeSubscriber(client=object())
    mocker.patch("great_expectations_cloud.agent.agent.Subscriber", return_value=subscriber)
    return subscriber


class DataContextConfigTD(TypedDict):
    anonymous_usage_statistics: dict[str, Any]
    checkpoint_store_name: str
    datasources: dict[str, dict[str, Any]]
    stores: dict[str, dict[str, Any]]


@pytest.fixture
def data_context_config() -> DataContextConfigTD:
    """
    Return a minimal DataContext config for testing.
    This what GET /organizations/{id}/data-contexts/{id} should return.

    See also:
    https://github.com/great-expectations/great_expectations/blob/develop/tests/datasource/fluent/_fake_cloud_api.py
    """
    return {
        "anonymous_usage_statistics": {
            "data_context_id": str(uuid.uuid4()),
            "enabled": False,
        },
        "checkpoint_store_name": "default_checkpoint_store",
        "datasources": {},
        "stores": {
            "default_evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "default_expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                    "ge_cloud_credentials": {
                        "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                        "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                    },
                    "ge_cloud_resource_type": "expectation_suite",
                    "suppress_store_backend_id": True,
                },
            },
            "default_checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                    "ge_cloud_credentials": {
                        "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                        "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                    },
                    "ge_cloud_resource_type": "checkpoint",
                    "suppress_store_backend_id": True,
                },
            },
            "default_validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                    "ge_cloud_credentials": {
                        "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                        "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                    },
                    "ge_cloud_resource_type": "validation_result",
                    "suppress_store_backend_id": True,
                },
            },
        },
    }


class DummyEvent(EventBase):
    type: Literal["event_name.received"] = "event_name.received"


class DummyAction(AgentAction[Any]):
    # Dummy event is used for testing only
    @override
    def run(self, event: Event, id: str) -> ActionResult:
        return ActionResult(id=id, type="DummyAction", created_resources=[])


register_event_action("1", DummyEvent, DummyAction)  # type: ignore[arg-type]
