from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from great_expectations.expectations import UnexpectedRowsExpectation

from great_expectations_cloud.agent.actions.generate_sql_expectation import (
    GenerateSqlExpectationAction,
)
from great_expectations_cloud.agent.event_handler import EventHandler
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.models import (
    CreatedResource,
    DomainContext,
    GenerateSqlExpectationEvent,
)
from great_expectations_cloud.agent.services.expectation_draft_config_service import (
    CreatedResourceTypes,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from tests.agent.conftest import MockCreateSessionType


@pytest.fixture(scope="module")
def expectation_prompt_id() -> UUID:
    return UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


@pytest.mark.unit
def test_generate_sql_expectation_action_success(
    base_url: str,
    auth_key: str,
    organization_id: UUID,
    workspace_id: UUID,
    expectation_prompt_id: UUID,
    mock_context,
    mocker: MockerFixture,
    mock_create_session: MockCreateSessionType,
):

    expected_metadata = {
        "id": str(expectation_prompt_id),
        "user_prompt": "test prompt",
        "data_source_name": "test_datasource",
        "asset_name": "test_asset",
        "batch_definition_name": "test_batch_def",
    }
    _session = mock_create_session(
        "great_expectations_cloud.agent.actions.generate_sql_expectation",
        "get",
        HTTPStatus.OK,
        expected_metadata,
    )

    mock_expectation = mocker.MagicMock(spec=UnexpectedRowsExpectation)
    mock_expectation.configuration.to_json_dict.return_value = {
        "type": "unexpected_rows_expectation",
        "unexpected_rows_query": "SELECT * FROM {batch} WHERE condition = false",
        "description": "Test expectation",
    }

    mock_arun = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_sql_expectation.SqlExpectationAgent.arun"
    )
    mock_arun.return_value = mock_expectation

    expected_created_resource = CreatedResource(
        resource_id="test-expectation-id", type=CreatedResourceTypes.EXPECTATION_DRAFT_CONFIG
    )
    mocker.patch.object(
        GenerateSqlExpectationAction,
        "_create_expectation_draft_config",
        return_value=expected_created_resource,
    )

    generate_sql_event = GenerateSqlExpectationEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        expectation_prompt_id=expectation_prompt_id,
    )

    generate_sql_action = GenerateSqlExpectationAction(
        context=mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    result = generate_sql_action.run(
        event=generate_sql_event,
        id="test-id",
    )

    assert result.type == generate_sql_event.type
    assert result.id == "test-id"
    assert len(result.created_resources) == 1
    assert result.created_resources[0].resource_id == "test-expectation-id"
    assert result.created_resources[0].type == CreatedResourceTypes.EXPECTATION_DRAFT_CONFIG

    expected_url = (
        f"{base_url}/api/v1/organizations/{organization_id}/workspaces/{workspace_id}/expectations/"
        f"prompt-metadata/{expectation_prompt_id}"
    )
    _session.get.assert_called_once_with(url=expected_url)


@pytest.mark.unit
def test_generate_sql_expectation_action_api_failure(
    base_url: str,
    auth_key: str,
    organization_id: UUID,
    workspace_id: UUID,
    expectation_prompt_id: UUID,
    mock_context,
    mocker: MockerFixture,
    mock_create_session: MockCreateSessionType,
):

    _session = mock_create_session(
        "great_expectations_cloud.agent.actions.generate_sql_expectation",
        "get",
        HTTPStatus.NOT_FOUND,
        {"error": "Config not found"},
    )

    generate_sql_event = GenerateSqlExpectationEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        expectation_prompt_id=expectation_prompt_id,
    )

    generate_sql_action = GenerateSqlExpectationAction(
        context=mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    with pytest.raises(GXAgentError) as exc_info:
        generate_sql_action.run(
            event=generate_sql_event,
            id="test-id",
        )

    assert "Failed to retrieve prompt metadata" in str(exc_info.value)
    assert str(expectation_prompt_id) in str(exc_info.value)
    assert "404" in str(exc_info.value)

    expected_url = (
        f"{base_url}/api/v1/organizations/{organization_id}/workspaces/{workspace_id}/expectations/"
        f"prompt-metadata/{expectation_prompt_id}"
    )
    _session.get.assert_called_once_with(url=expected_url)


@pytest.mark.unit
def test_generate_sql_expectation_action_registered(
    mock_context, organization_id: UUID, workspace_id: UUID
):
    handler = EventHandler(context=mock_context)
    event = GenerateSqlExpectationEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        expectation_prompt_id=UUID("00000000-0000-0000-0000-000000000099"),
    )
    domain_context = DomainContext(organization_id=organization_id, workspace_id=workspace_id)

    action = handler.get_event_action(
        event=event,
        base_url="https://test-base-url",
        auth_key="test-token",
        domain_context=domain_context,
    )

    assert isinstance(action, GenerateSqlExpectationAction)


@pytest.mark.unit
def test_generate_sql_expectation_action_missing_openai_credentials(
    base_url: str,
    auth_key: str,
    organization_id: UUID,
    workspace_id: UUID,
    expectation_prompt_id: UUID,
    mock_context,
    mocker: MockerFixture,
):
    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_sql_expectation.ensure_openai_credentials",
        side_effect=ValueError(
            "OpenAI credentials are not set. Please set the OPENAI_API_KEY environment variable to enable ExpectAI."
        ),
    )

    generate_sql_event = GenerateSqlExpectationEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        expectation_prompt_id=expectation_prompt_id,
    )

    generate_sql_action = GenerateSqlExpectationAction(
        context=mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    with pytest.raises(ValueError, match="OpenAI credentials are not set"):
        generate_sql_action.run(
            event=generate_sql_event,
            id="test-id",
        )
