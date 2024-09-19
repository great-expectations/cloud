from __future__ import annotations

import random
import string
import uuid
from time import sleep
from typing import TYPE_CHECKING, Callable, Literal
from unittest.mock import call

import pytest
import responses
from great_expectations.exceptions import exceptions as gx_exception
from pika.exceptions import AuthenticationError, ProbableAuthenticationError
from pydantic.v1 import (
    ValidationError,
)
from tenacity import RetryError

from great_expectations_cloud.agent import GXAgent
from great_expectations_cloud.agent.actions.agent_action import ActionResult
from great_expectations_cloud.agent.agent import GXAgentConfig, Payload
from great_expectations_cloud.agent.constants import USER_AGENT_HEADER, HeaderName
from great_expectations_cloud.agent.exceptions import GXAgentConfigError
from great_expectations_cloud.agent.message_service.asyncio_rabbit_mq_client import (
    ClientError,
)
from great_expectations_cloud.agent.message_service.subscriber import (
    EventContext,
    SubscriberError,
)
from great_expectations_cloud.agent.models import (
    JobCompleted,
    JobStarted,
    RunOnboardingDataAssistantEvent,
    RunScheduledCheckpointEvent,
)
from tests.agent.conftest import DummyEvent, FakeMessagePayload, FakeSubscriber

if TYPE_CHECKING:
    from tests.agent.conftest import DataContextConfigTD


# TODO: This should be marked as unit tests after fixing the tests to mock outgoing calls
pytestmark = pytest.mark.integration


@pytest.fixture
def set_required_env_vars(monkeypatch, random_uuid, random_string, local_mercury):
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", random_uuid)
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", random_string)
    monkeypatch.setenv("GX_CLOUD_BASE_URL", local_mercury)


@pytest.fixture
def gx_agent_config(
    set_required_env_vars, queue, connection_string, random_uuid, random_string, local_mercury
) -> GXAgentConfig:
    config = GXAgentConfig(
        queue=queue,
        connection_string=connection_string,
        gx_cloud_access_token=random_string,
        gx_cloud_organization_id=random_uuid,
        gx_cloud_base_url=local_mercury,
    )
    return config


@pytest.fixture
def gx_agent_config_missing_token(
    set_required_env_vars,
    queue,
    connection_string,
    random_uuid,
    random_string,
    local_mercury,
    monkeypatch,
) -> GXAgentConfig:
    monkeypatch.delenv("GX_CLOUD_ACCESS_TOKEN")
    config = GXAgentConfig(  # type: ignore[call-arg]
        queue=queue,
        connection_string=connection_string,
        gx_cloud_organization_id=random_uuid,
        token=random_string,
        gx_cloud_base_url=local_mercury,
    )
    return config


@pytest.fixture
def gx_agent_config_missing_org_id(
    set_required_env_vars,
    queue,
    connection_string,
    random_uuid,
    random_string,
    local_mercury,
    monkeypatch,
) -> GXAgentConfig:
    monkeypatch.delenv("GX_CLOUD_ORGANIZATION_ID")
    config = GXAgentConfig(  # type: ignore[call-arg]
        queue=queue,
        connection_string=connection_string,
        gx_cloud_access_token=random_string,
        org_id=random_uuid,
        gx_cloud_base_url=local_mercury,
    )
    return config


@pytest.fixture
def local_mercury():
    return "http://localhost:5000/"


@pytest.fixture
def random_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def random_string() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=20))


@pytest.fixture
def get_context(mocker):
    get_context = mocker.patch("great_expectations_cloud.agent.agent.get_context")
    return get_context


@pytest.fixture
def client(mocker):
    """Patch for agent.RabbitMQClient"""
    client = mocker.patch("great_expectations_cloud.agent.agent.AsyncRabbitMQClient")
    return client


@pytest.fixture
def subscriber(mocker):
    """Patch for agent.Subscriber"""
    subscriber = mocker.patch("great_expectations_cloud.agent.agent.Subscriber")
    return subscriber


@pytest.fixture
def event_handler(mocker):
    event_handler = mocker.patch("great_expectations_cloud.agent.agent.EventHandler")
    return event_handler


@pytest.fixture
def queue() -> str:
    return "3ee9791c-4ea6-479d-9b05-98217e70d341"


@pytest.fixture
def connection_string() -> str:
    return "amqps://user:pass@great_expectations.io:5671"


@pytest.fixture(autouse=True)
def create_session(mocker, queue, connection_string):
    """Patch for great_expectations.core.http.create_session"""
    create_session = mocker.patch("great_expectations_cloud.agent.agent.create_session")
    create_session().post().json.return_value = {
        "queue": queue,
        "connection_string": connection_string,
    }
    create_session().post().ok = True
    return create_session


@pytest.fixture(autouse=True)
def requests_post(mocker, queue, connection_string):
    """Patch for requests.Session.post"""
    requests_post = mocker.patch("requests.Session.post")
    requests_post().json.return_value = {
        "queue": queue,
        "connection_string": connection_string,
    }
    requests_post().ok = True
    return requests_post


def test_gx_agent_gets_env_vars_on_init(get_context, gx_agent_config, requests_post):
    agent = GXAgent()
    assert agent._config == gx_agent_config


def test_gx_agent_invalid_token(monkeypatch, set_required_env_vars: None):
    # There is no validation for the token aside from presence, so we set to empty to raise an error.
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "")
    with pytest.raises(gx_exception.GXCloudConfigurationError):
        GXAgent()


def test_gx_agent_initializes_cloud_context(get_context, gx_agent_config):
    GXAgent()
    get_context.assert_called_with(cloud_mode=True)


def test_gx_agent_run_starts_subscriber(get_context, subscriber, client, gx_agent_config):
    """Expect GXAgent.run to invoke the Subscriber class with the correct arguments."""
    agent = GXAgent()
    agent.run()

    subscriber.assert_called_with(client=client())


def test_gx_agent_run_invokes_consume(get_context, subscriber, client, gx_agent_config):
    """Expect GXAgent.run to invoke subscriber.consume with the correct arguments."""
    agent = GXAgent()
    agent.run()

    subscriber().consume.assert_called_with(
        queue=gx_agent_config.queue,
        on_message=agent._handle_event_as_thread_enter,
    )


def test_gx_agent_run_closes_subscriber(get_context, subscriber, client, gx_agent_config):
    """Expect GXAgent.run to invoke subscriber.close."""
    agent = GXAgent()
    agent.run()

    subscriber().close.assert_called_with()


def test_gx_agent_run_handles_client_error_on_init(
    get_context, subscriber, client, gx_agent_config
):
    client.side_effect = ClientError
    agent = GXAgent()
    agent.run()


def test_gx_agent_run_handles_subscriber_error_on_init(
    get_context, subscriber, client, gx_agent_config
):
    subscriber.side_effect = SubscriberError
    agent = GXAgent()
    agent.run()


def test_gx_agent_run_handles_subscriber_error_on_consume(
    get_context, subscriber, client, gx_agent_config
):
    subscriber.consume.side_effect = SubscriberError
    agent = GXAgent()
    agent.run()


def test_gx_agent_run_handles_client_authentication_error_on_init(
    get_context, subscriber, client, gx_agent_config
):
    with pytest.raises((AuthenticationError, RetryError)):
        client.side_effect = AuthenticationError
        agent = GXAgent()
        agent.run()


def test_gx_agent_run_handles_client_probable_authentication_error_on_init(
    get_context, subscriber, client, gx_agent_config
):
    with pytest.raises((ProbableAuthenticationError, RetryError)):
        client.side_effect = ProbableAuthenticationError
        agent = GXAgent()
        agent.run()


def test_gx_agent_run_handles_subscriber_error_on_close(
    get_context, subscriber, client, gx_agent_config
):
    subscriber.close.side_effect = SubscriberError
    agent = GXAgent()
    agent.run()


def test_gx_agent_updates_cloud_on_job_status(
    subscriber, create_session, get_context, client, gx_agent_config, event_handler
):
    correlation_id = "4ae63677-4dd5-4fb0-b511-870e7a286e77"
    url = (
        f"{gx_agent_config.gx_cloud_base_url}/organizations/"
        f"{gx_agent_config.gx_cloud_organization_id}/agent-jobs/{correlation_id}"
    )
    job_started_data = JobStarted().json()
    job_completed = JobCompleted(success=True, created_resources=[], processed_by="agent")
    job_completed_data = job_completed.json()

    async def redeliver_message():
        return None

    event = RunOnboardingDataAssistantEvent(
        datasource_name="test-ds", data_asset_name="test-da", organization_id=uuid.uuid4()
    )

    end_test = False

    def signal_subtask_finished():
        nonlocal end_test
        end_test = True

    event_context = EventContext(
        event=event,
        correlation_id=correlation_id,
        processed_successfully=signal_subtask_finished,
        processed_with_failures=signal_subtask_finished,
        redeliver_message=redeliver_message,
    )
    event_handler.return_value.handle_event.return_value = ActionResult(
        id=correlation_id, type=event.type, created_resources=[]
    )

    def consume(queue: str, on_message: Callable[[EventContext], None]):
        """util to allow testing agent behavior without a subscriber.

        Replicates behavior of Subscriber.consume by invoking the on_message
        parameter with an event_context.
        """
        nonlocal event_context
        on_message(event_context)

        # we need the main thread to remain alive until event handler has finished
        nonlocal end_test
        while end_test is False:
            sleep(0)  # defer control

    subscriber().consume = consume

    agent = GXAgent()
    agent.run()

    # sessions created with context managers now, so we need to
    # test the runtime calls rather than the return value calls.
    # the calls also appear to store in any order, hence the any_order=True
    create_session().__enter__().patch.assert_has_calls(
        any_order=True,
        calls=[
            call(url, data=job_started_data),
            call(url, data=job_completed_data),
        ],
    )


def test_gx_agent_sends_request_to_create_scheduled_job(
    subscriber, create_session, get_context, client, gx_agent_config, event_handler
):
    """What does this test and why?

    Scheduled jobs are created in mercury by sending a POST request to the agent-jobs endpoint.
    This is because the scheduler + lambda create the event in the queue, and the agent consumes it. The agent then
    sends a request to the agent-jobs endpoint to create the job in mercury. Non-scheduled events are created in mercury
    first, and then an event is created in the queue.
    This test ensures that the agent sends the correct request to the correct endpoint.
    """
    correlation_id = "4ae63677-4dd5-4fb0-b511-870e7a286e77"
    post_url = (
        f"{gx_agent_config.gx_cloud_base_url}/organizations/"
        f"{gx_agent_config.gx_cloud_organization_id}/agent-jobs"
    )

    checkpoint_id = uuid.uuid4()
    schedule_id = uuid.uuid4()
    event = RunScheduledCheckpointEvent(
        checkpoint_id=checkpoint_id,
        datasource_names_to_asset_names={},
        splitter_options=None,
        schedule_id=schedule_id,
        organization_id=uuid.uuid4(),
    )
    payload = Payload(
        data={
            "correlation_id": correlation_id,
            "event": event.dict(),
        }
    )
    data = payload.json()

    async def redeliver_message():
        return None

    end_test = False

    def signal_subtask_finished():
        nonlocal end_test
        end_test = True

    event_context = EventContext(
        event=event,
        correlation_id=correlation_id,
        processed_successfully=signal_subtask_finished,
        processed_with_failures=signal_subtask_finished,
        redeliver_message=redeliver_message,
    )
    event_handler.return_value.handle_event.return_value = ActionResult(
        id=correlation_id, type=event.type, created_resources=[]
    )

    def consume(queue: str, on_message: Callable[[EventContext], None]):
        """util to allow testing agent behavior without a subscriber.

        Replicates behavior of Subscriber.consume by invoking the on_message
        parameter with an event_context.
        """
        nonlocal event_context
        on_message(event_context)

        # we need the main thread to remain alive until event handler has finished
        nonlocal end_test
        while end_test is False:
            sleep(0)  # defer control

    subscriber().consume = consume

    agent = GXAgent()
    agent.run()

    # sessions created with context managers now, so we need to
    # test the runtime calls rather than the return value calls
    return create_session().__enter__().post.assert_any_call(post_url, data=data)


def test_invalid_env_variables_missing_token(set_required_env_vars, monkeypatch):
    monkeypatch.delenv("GX_CLOUD_ACCESS_TOKEN")
    with pytest.raises(GXAgentConfigError):
        GXAgent()


def test_invalid_env_variables_missing_org_id(set_required_env_vars, monkeypatch):
    monkeypatch.delenv("GX_CLOUD_ORGANIZATION_ID")
    with pytest.raises(GXAgentConfigError):
        GXAgent()


def test_invalid_config_agent_missing_token(
    connection_string: str, queue: str, random_uuid: str, local_mercury: str
):
    with pytest.raises(ValidationError) as exc_info:
        GXAgentConfig(  # type: ignore[call-arg]
            queue=queue,
            connection_string=connection_string,  # type: ignore[arg-type]
            gx_cloud_organization_id=random_uuid,
            gx_cloud_base_url=local_mercury,  # type: ignore[arg-type]
        )
    error_locs = [error["loc"] for error in exc_info.value.errors()]
    assert "gx_cloud_access_token" in error_locs[0]


def test_invalid_config_agent_missing_org_id(
    connection_string: str, queue: str, local_mercury: str, random_string: str
):
    with pytest.raises(ValidationError) as exc_info:
        GXAgentConfig(  # type: ignore[call-arg]
            queue=queue,
            connection_string=connection_string,  # type: ignore[arg-type]
            gx_cloud_access_token=random_string,
            gx_cloud_base_url=local_mercury,  # type: ignore[arg-type]
        )
    error_locs = [error["loc"] for error in exc_info.value.errors()]
    assert "gx_cloud_organization_id" in error_locs[0]


def test_custom_user_agent(
    mock_gx_version_check: None,
    set_required_env_vars: None,
    gx_agent_config: GXAgentConfig,
    data_context_config: DataContextConfigTD,
):
    """Ensure custom User-Agent header is set on GX Cloud api calls."""
    base_url = gx_agent_config.gx_cloud_base_url
    org_id = gx_agent_config.gx_cloud_organization_id
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{base_url}api/v1/organizations/{org_id}/data-context-configuration",
            json=data_context_config,
            # match will fail if the User-Agent header is not set
            match=[
                responses.matchers.header_matcher(
                    {
                        HeaderName.USER_AGENT: f"{USER_AGENT_HEADER}/{GXAgent.get_current_gx_agent_version()}"
                    }
                )
            ],
        )
        rsps.add(
            responses.GET,
            f"{base_url}organizations/{org_id}/accounts/me",
            json={"user_id": str(uuid.uuid4())},
            # match will fail if the User-Agent header is not set
            match=[
                responses.matchers.header_matcher(
                    {
                        HeaderName.USER_AGENT: f"{USER_AGENT_HEADER}/{GXAgent.get_current_gx_agent_version()}"
                    }
                )
            ],
        )
        GXAgent()


@pytest.fixture
def ds_config_factory() -> Callable[[str], dict[Literal["name", "type", "connection_string"], str]]:
    """
    Return a factory that takes a `name` and creates valid datasource config dicts.
    The datasource will always be an in-memory sqlite datasource that will pass `.test_connection()`
    But will fail if trying to add a TableAsset because no tables exist for it.
    """

    def _factory(name: str = "test-ds") -> dict[Literal["name", "type", "connection_string"], str]:
        return {
            "name": name,
            "type": "sqlite",
            "connection_string": "sqlite:///",
        }

    return _factory


def test_correlation_id_header(
    set_required_env_vars: None,
    mock_gx_version_check: None,
    data_context_config: DataContextConfigTD,
    ds_config_factory: Callable[[str], dict[Literal["name", "type", "connection_string"], str]],
    gx_agent_config: GXAgentConfig,
    fake_subscriber: FakeSubscriber,
    random_uuid: str,
):
    """Ensure agent-job-id/correlation-id header is set on GX Cloud api calls and updated for every new job."""
    agent_correlation_ids: list[str] = [str(uuid.uuid4()) for _ in range(3)]
    datasource_config_id_1 = uuid.UUID("00000000-0000-0000-0000-000000000001")
    datasource_config_id_2 = uuid.UUID("00000000-0000-0000-0000-000000000002")
    checkpoint_id = uuid.UUID("00000000-0000-0000-0000-000000000003")
    # seed the fake queue with an event that will be consumed by the agent
    fake_subscriber.test_queue.extendleft(
        [
            (
                DraftDatasourceConfigEvent(
                    config_id=datasource_config_id_1,
                    organization_id=random_uuid,  # type: ignore[arg-type] # str coerced to UUID
                ),
                agent_correlation_ids[0],
            ),
            (
                DraftDatasourceConfigEvent(
                    config_id=datasource_config_id_2,
                    organization_id=random_uuid,  # type: ignore[arg-type] # str coerced to UUID
                ),
                agent_correlation_ids[1],
            ),
            (
                RunCheckpointEvent(
                    checkpoint_id=checkpoint_id,
                    datasource_names_to_asset_names={},
                    organization_id=random_uuid,  # type: ignore[arg-type] # str coerced to UUID
                ),
                agent_correlation_ids[2],
            ),
        ]
    )
    base_url = gx_agent_config.gx_cloud_base_url
    org_id = gx_agent_config.gx_cloud_organization_id
    with responses.RequestsMock() as rsps:
        (
            rsps.add(
                responses.GET,
                f"{base_url}organizations/{org_id}/accounts/me",
                json={"user_id": str(uuid.uuid4())},
            ),
        )
        rsps.add(
            responses.GET,
            f"{base_url}api/v1/organizations/{org_id}/data-context-configuration",
            json=data_context_config,
        )
        agent = GXAgent()
        agent.run()
