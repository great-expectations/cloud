from __future__ import annotations

import os
import uuid
from time import sleep
from typing import Callable
from unittest.mock import call

import pytest
import responses

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
    RunOnboardingDataAssistantEvent,
)
from tests.agent.conftest import FakeSubscriber


@pytest.fixture
def set_required_env_vars(monkeypatch, org_id, token):
    env_vars = {
        "GX_CLOUD_ORGANIZATION_ID": org_id,
        "GX_CLOUD_ACCESS_TOKEN": token,
    }
    monkeypatch.setattr(os, "environ", env_vars)


@pytest.fixture
def set_required_env_vars_missing_token(monkeypatch, org_id):
    env_vars = {
        "GX_CLOUD_ORGANIZATION_ID": org_id,
        "GX_CLOUD_ACCESS_TOKEN": None,
    }
    monkeypatch.setattr(os, "environ", env_vars)


@pytest.fixture
def set_required_env_vars_missing_org_id(monkeypatch, token):
    env_vars = {
        "GX_CLOUD_ORGANIZATION_ID": None,
        "GX_CLOUD_ACCESS_TOKEN": token,
    }
    monkeypatch.setattr(os, "environ", env_vars)


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


def test_gx_agent_gets_env_vars_on_init(get_context, gx_agent_config):
    agent = GXAgent()
    assert agent._config == gx_agent_config


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

    agent = GXAgent()
    agent.run()

    create_session.return_value.patch.assert_has_calls(
        calls=[
            call(url, data=job_started_data),
            call(url, data=job_completed_data),
        ],
    )


def test_invalid_config_agent_missing_token(gx_agent_config_missing_token):
    with pytest.raises(GXAgentConfigError):
        GXAgent()


def test_invalid_config_agent_missing_org_id(gx_agent_config_missing_org_id):
    with pytest.raises(GXAgentConfigError):
        GXAgent()


def test_custom_user_agent(
    mock_gx_version_check: None,
    set_required_env_vars: None,
    gx_agent_config: GXAgentConfig,
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
                        HeaderName.USER_AGENT: f"{USER_AGENT_HEADER}/{GXAgent._get_current_gx_agent_version()}"
                    }
                )
            ],
        )
        GXAgent()


def test_correlation_id_header(
    mock_gx_version_check: None,
    set_required_env_vars: None,
    gx_agent_config: GXAgentConfig,
    fake_subscriber: FakeSubscriber,
):
    """Ensure agent-job-id/correlation-id header is set on GX Cloud api calls and updated for every new job."""
    agent_job_id_1 = uuid.uuid4()
    datasource_config_id_1 = uuid.UUID("00000000-0000-0000-0000-000000000001")
    agent_job_id_2 = uuid.uuid4()
    datasource_config_id_2 = uuid.UUID("00000000-0000-0000-0000-000000000002")
    # seed the fake queue with an event that will be consumed by the agent
    fake_subscriber.test_queue.extend(
        [
            (DraftDatasourceConfigEvent(config_id=datasource_config_id_1), str(agent_job_id_1)),
            (DraftDatasourceConfigEvent(config_id=datasource_config_id_2), str(agent_job_id_2)),
        ]
    )

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
        )
        rsps.add(
            responses.GET,
            f"{base_url}/organizations/{org_id}/datasources/drafts/{datasource_config_id_1}",
            json={},
            # match will fail if correlation-id header is not set
            match=[
                responses.matchers.header_matcher({HeaderName.AGENT_JOB_ID: str(agent_job_id_1)})
            ],
        )
        rsps.add(
            responses.GET,
            f"{base_url}/organizations/{org_id}/datasources/drafts/{datasource_config_id_2}",
            json={},
            # match will fail if correlation-id header is not set
            match=[
                responses.matchers.header_matcher({HeaderName.AGENT_JOB_ID: str(agent_job_id_2)})
            ],
        )
        agent = GXAgent()
        agent.run()
