from __future__ import annotations

import logging
import uuid
from time import sleep
from typing import TYPE_CHECKING, Callable, Literal
from unittest.mock import call

import pytest
import responses
from pika.exceptions import AuthenticationError, ProbableAuthenticationError
from tenacity import RetryError

from great_expectations_cloud.agent import GXAgent
from great_expectations_cloud.agent.actions.agent_action import ActionResult
from great_expectations_cloud.agent.agent import GXAgentConfig, GXAgentConfigError
from great_expectations_cloud.agent.constants import USER_AGENT_HEADER, HeaderName
from great_expectations_cloud.agent.message_service.asyncio_rabbit_mq_client import (
    ClientError,
)
from great_expectations_cloud.agent.message_service.subscriber import (
    EventContext,
    SubscriberError,
)
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    JobCompleted,
    JobStarted,
    RunCheckpointEvent,
    RunOnboardingDataAssistantEvent,
)
from tests.agent.conftest import FakeSubscriber

if TYPE_CHECKING:
    from tests.agent.conftest import DataContextConfigTD


@pytest.fixture
def set_required_env_vars(monkeypatch, org_id, token):
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", org_id)
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", token)


@pytest.fixture
def set_required_env_vars_missing_token(monkeypatch, org_id):
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", org_id)


@pytest.fixture
def set_required_env_vars_missing_org_id(monkeypatch, token):
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", token)


@pytest.fixture
def gx_agent_config(
    set_required_env_vars, queue, connection_string, org_id, token
) -> GXAgentConfig:
    config = GXAgentConfig(
        queue=queue,
        connection_string=connection_string,
        gx_cloud_access_token=token,
        gx_cloud_organization_id=org_id,
    )
    return config


@pytest.fixture
def gx_agent_config_missing_token(
    set_required_env_vars_missing_token, queue, connection_string, org_id, token
) -> GXAgentConfig:
    config = GXAgentConfig(
        queue=queue,
        connection_string=connection_string,
        gx_cloud_access_token=token,
        gx_cloud_organization_id=org_id,
    )
    return config


@pytest.fixture
def gx_agent_config_missing_org_id(
    set_required_env_vars_missing_org_id, queue, connection_string, org_id, token
) -> GXAgentConfig:
    config = GXAgentConfig(
        queue=queue,
        connection_string=connection_string,
        gx_cloud_access_token=token,
        gx_cloud_organization_id=org_id,
    )
    return config


@pytest.fixture
def org_id():
    return "4ea2985c-4fb7-4c53-9f8e-07b7e0506c3e"


@pytest.fixture
def token():
    return "MTg0NDkyYmYtNTBiOS00ZDc1LTk3MmMtYjQ0M2NhZDA2NjJk"


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
def queue():
    return "3ee9791c-4ea6-479d-9b05-98217e70d341"


@pytest.fixture
def connection_string():
    return "amqps://user:pass@great_expectations.io:5671"


@pytest.fixture
def logger():
    return logging.getLogger(__name__)


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


def test_gx_agent_gets_env_vars_on_init(get_context, gx_agent_config, logger):
    agent = GXAgent(logger)
    assert agent._config == gx_agent_config


def test_gx_agent_initializes_cloud_context(get_context, gx_agent_config, logger):
    GXAgent(logger)
    get_context.assert_called_with(cloud_mode=True)


def test_gx_agent_run_starts_subscriber(get_context, subscriber, client, gx_agent_config, logger):
    """Expect GXAgent.run to invoke the Subscriber class with the correct arguments."""
    agent = GXAgent(logger)
    agent.run()

    subscriber.assert_called_with(client=client())


def test_gx_agent_run_invokes_consume(get_context, subscriber, client, gx_agent_config, logger):
    """Expect GXAgent.run to invoke subscriber.consume with the correct arguments."""
    agent = GXAgent(logger)
    agent.run()

    subscriber().consume.assert_called_with(
        queue=gx_agent_config.queue,
        on_message=agent._handle_event_as_thread_enter,
    )


def test_gx_agent_run_closes_subscriber(get_context, subscriber, client, gx_agent_config, logger):
    """Expect GXAgent.run to invoke subscriber.close."""
    agent = GXAgent(logger)
    agent.run()

    subscriber().close.assert_called_with()


def test_gx_agent_run_handles_client_error_on_init(
    get_context, subscriber, client, gx_agent_config, logger
):
    client.side_effect = ClientError
    agent = GXAgent(logger)
    agent.run()


def test_gx_agent_run_handles_subscriber_error_on_init(
    get_context, subscriber, client, gx_agent_config, logger
):
    subscriber.side_effect = SubscriberError
    agent = GXAgent(logger)
    agent.run()


def test_gx_agent_run_handles_subscriber_error_on_consume(
    get_context, subscriber, client, gx_agent_config, logger
):
    subscriber.consume.side_effect = SubscriberError
    agent = GXAgent(logger)
    agent.run()


def test_gx_agent_run_handles_client_authentication_error_on_init(
    get_context, subscriber, client, gx_agent_config, logger
):
    with pytest.raises((AuthenticationError, RetryError)):
        client.side_effect = AuthenticationError
        agent = GXAgent(logger)
        agent.run()


def test_gx_agent_run_handles_client_probable_authentication_error_on_init(
    get_context, subscriber, client, gx_agent_config, logger
):
    with pytest.raises((ProbableAuthenticationError, RetryError)):
        client.side_effect = ProbableAuthenticationError
        agent = GXAgent(logger)
        agent.run()


def test_gx_agent_run_handles_subscriber_error_on_close(
    get_context, subscriber, client, gx_agent_config, logger
):
    subscriber.close.side_effect = SubscriberError
    agent = GXAgent(logger)
    agent.run()


def test_gx_agent_updates_cloud_on_job_status(
    subscriber, create_session, get_context, client, gx_agent_config, event_handler, logger
):
    correlation_id = "4ae63677-4dd5-4fb0-b511-870e7a286e77"
    url = (
        f"{gx_agent_config.gx_cloud_base_url}/organizations/"
        f"{gx_agent_config.gx_cloud_organization_id}/agent-jobs/{correlation_id}"
    )
    job_started_data = JobStarted().json()
    job_completed = JobCompleted(success=True, created_resources=[])
    job_completed_data = job_completed.json()

    async def redeliver_message():
        return None

    event = RunOnboardingDataAssistantEvent(datasource_name="test-ds", data_asset_name="test-da")

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

    agent = GXAgent(logger)
    agent.run()

    create_session.return_value.patch.assert_has_calls(
        calls=[
            call(url, data=job_started_data),
            call(url, data=job_completed_data),
        ],
    )


def test_invalid_config_agent_missing_token(gx_agent_config_missing_token, logger):
    with pytest.raises(GXAgentConfigError):
        GXAgent(logger)


def test_invalid_config_agent_missing_org_id(gx_agent_config_missing_org_id, logger):
    with pytest.raises(GXAgentConfigError):
        GXAgent(logger)


def test_custom_user_agent(
    mock_gx_version_check: None, set_required_env_vars: None, gx_agent_config: GXAgentConfig, logger
):
    """Ensure custom User-Agent header is set on GX Cloud api calls."""
    base_url = gx_agent_config.gx_cloud_base_url
    org_id = gx_agent_config.gx_cloud_organization_id
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{base_url}/organizations/{org_id}/data-context-configuration",
            json={
                "anonymous_usage_statistics": {
                    "data_context_id": str(uuid.uuid4()),
                    "enabled": False,
                },
                "datasources": {},
                "stores": {},
            },
            # match will fail if the User-Agent header is not set
            match=[
                responses.matchers.header_matcher(
                    {
                        HeaderName.USER_AGENT: f"{USER_AGENT_HEADER}/{GXAgent.get_current_gx_agent_version()}"
                    }
                )
            ],
        )
        GXAgent(logger)


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
    mock_gx_version_check: None,
    set_required_env_vars: None,
    data_context_config: DataContextConfigTD,
    ds_config_factory: Callable[[str], dict[Literal["name", "type", "connection_string"], str]],
    gx_agent_config: GXAgentConfig,
    fake_subscriber: FakeSubscriber,
    logger,
):
    """Ensure agent-job-id/correlation-id header is set on GX Cloud api calls and updated for every new job."""
    agent_job_ids: list[str] = [str(uuid.uuid4()) for _ in range(3)]
    datasource_config_id_1 = uuid.UUID("00000000-0000-0000-0000-000000000001")
    datasource_config_id_2 = uuid.UUID("00000000-0000-0000-0000-000000000002")
    checkpoint_id = uuid.UUID("00000000-0000-0000-0000-000000000003")
    # seed the fake queue with an event that will be consumed by the agent
    fake_subscriber.test_queue.extendleft(
        [
            (DraftDatasourceConfigEvent(config_id=datasource_config_id_1), agent_job_ids[0]),
            (DraftDatasourceConfigEvent(config_id=datasource_config_id_2), agent_job_ids[1]),
            (
                RunCheckpointEvent(checkpoint_id=checkpoint_id, datasource_names_to_asset_names={}),
                agent_job_ids[2],
            ),
        ]
    )

    base_url = gx_agent_config.gx_cloud_base_url
    org_id = gx_agent_config.gx_cloud_organization_id
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{base_url}/organizations/{org_id}/data-context-configuration",
            json=data_context_config,
        )
        rsps.add(
            responses.GET,
            f"{base_url}/organizations/{org_id}/datasources/drafts/{datasource_config_id_1}",
            json={"data": {"attributes": {"draft_config": ds_config_factory("test-ds-1")}}},
            # match will fail if correlation-id header is not set
            match=[responses.matchers.header_matcher({HeaderName.AGENT_JOB_ID: agent_job_ids[0]})],
        )
        rsps.add(
            responses.GET,
            f"{base_url}/organizations/{org_id}/datasources/drafts/{datasource_config_id_2}",
            json={"data": {"attributes": {"draft_config": ds_config_factory("test-ds-2")}}},
            # match will fail if correlation-id header is not set
            match=[responses.matchers.header_matcher({HeaderName.AGENT_JOB_ID: agent_job_ids[1]})],
        )
        rsps.add(
            responses.GET,
            f"{base_url}organizations/{org_id}/checkpoints/{checkpoint_id}",
            json={"data": {}},
            # match will fail if correlation-id header is not set
            match=[responses.matchers.header_matcher({HeaderName.AGENT_JOB_ID: agent_job_ids[2]})],
        )
        agent = GXAgent(logger)
        agent.run()
