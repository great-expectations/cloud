from __future__ import annotations

import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset

from great_expectations_cloud.agent.actions.generate_sql_expectation import (
    GenerateSqlExpectationAction,
)
from great_expectations_cloud.agent.event_handler import EventHandler
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.models import (
    DomainContext,
    GenerateSqlExpectationEvent,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.fixture
def org_id():
    return UUID("81f4e105-e37d-4168-85a0-2526943f9956")


@pytest.fixture
def workspace_id():
    return UUID("91f4e105-e37d-4168-85a0-2526943f9956")


@pytest.fixture
def mock_context(mocker: MockerFixture):
    return mocker.Mock(spec=CloudDataContext)


@pytest.fixture
def domain_context(org_id: UUID, workspace_id: UUID):
    return DomainContext(organization_id=org_id, workspace_id=workspace_id)


@pytest.fixture
def generate_sql_expectation_event(org_id: UUID, workspace_id: UUID):
    return GenerateSqlExpectationEvent(
        organization_id=org_id,
        workspace_id=workspace_id,
        expectation_prompt_id=UUID("00000000-0000-0000-0000-000000000099"),
    )


@pytest.fixture
def action(mock_context, domain_context):
    return GenerateSqlExpectationAction(
        context=mock_context,
        base_url="https://test-base-url",
        domain_context=domain_context,
        auth_key="test-token",
    )


def test_generate_sql_expectation_action_raises_error_when_openai_not_configured(
    monkeypatch, action, generate_sql_expectation_event
):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    # Also need to set required env vars so GxAgentEnvVars can be instantiated
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", str(uuid.uuid4()))
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "test-token")

    with pytest.raises(GXAgentError) as exc_info:
        action.run(event=generate_sql_expectation_event, id="test-correlation-id")

    assert "OpenAI credentials not configured" in str(exc_info.value)
    assert "OPENAI_API_KEY" in str(exc_info.value)
    assert "ExpectAI" in str(exc_info.value)


def test_generate_sql_expectation_action_has_openai_credentials_when_set(
    monkeypatch, action, generate_sql_expectation_event, mocker: MockerFixture
):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-key")
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", str(uuid.uuid4()))
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "test-token")

    # Mock prompt metadata API response
    mock_get_response = mocker.Mock()
    mock_get_response.status_code = HTTPStatus.OK
    mock_get_response.json.return_value = {
        "user_prompt": "Every customer must have an email address",
        "data_source_name": "test_datasource",
        "asset_name": "test_asset",
        "batch_definition_name": "test_batch",
    }

    mock_datasource = mocker.Mock(spec=SQLDatasource)
    mock_asset = mocker.Mock(spec=TableAsset)
    mock_asset.id = UUID("00000000-0000-0000-0000-000000000001")
    mock_datasource.get_asset.return_value = mock_asset
    action._context.data_sources.get.return_value = mock_datasource

    # Mock the SQL agent and expectation creation
    mock_expectation = mocker.Mock()
    mock_expectation.configuration.to_json_dict.return_value = {
        "type": "expect_unexpected_rows_query_to_return_non_empty_set",
        "unexpected_rows_query": "SELECT * FROM {batch} WHERE email IS NULL",
        "description": "Expect all customers to have an email address",
    }

    mock_sql_agent = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_sql_expectation.SqlExpectationAgent"
    )
    mock_sql_agent_instance = mocker.Mock()
    mock_sql_agent_instance.arun = mocker.AsyncMock(return_value=mock_expectation)
    mock_sql_agent.return_value = mock_sql_agent_instance

    # Mock the API calls (GET for metadata, POST for draft config)
    mock_post_response = mocker.Mock()
    mock_post_response.status_code = HTTPStatus.CREATED
    mock_post_response.json.return_value = {
        "data": [{"id": str(UUID("00000000-0000-0000-0000-000000000002"))}]
    }

    mock_session = mocker.MagicMock()
    mock_session.__enter__.return_value.get.return_value = mock_get_response
    mock_session.__enter__.return_value.post.return_value = mock_post_response
    mock_session.__exit__.return_value = None
    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_sql_expectation.create_session",
        return_value=mock_session,
    )

    result = action.run(event=generate_sql_expectation_event, id="test-correlation-id")

    assert result.id == "test-correlation-id"
    assert result.type == generate_sql_expectation_event.type
    assert len(result.created_resources) == 1
    assert result.created_resources[0].type == "ExpectationDraftConfig"


def test_generate_sql_expectation_action_registered(mock_context, org_id: UUID, workspace_id: UUID):
    handler = EventHandler(context=mock_context)
    event = GenerateSqlExpectationEvent(
        organization_id=org_id,
        workspace_id=workspace_id,
        expectation_prompt_id=UUID("00000000-0000-0000-0000-000000000099"),
    )
    domain_context = DomainContext(organization_id=org_id, workspace_id=workspace_id)

    action = handler.get_event_action(
        event=event,
        base_url="https://test-base-url",
        auth_key="test-token",
        domain_context=domain_context,
    )

    assert isinstance(action, GenerateSqlExpectationAction)
