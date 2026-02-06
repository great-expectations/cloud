from __future__ import annotations

import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest
import responses
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations_cloud.agent.models import EXPECTATION_DRAFT_CONFIG
from great_expectations_cloud.agent.services.expectation_draft_config_service import (
    ExpectationDraftConfigError,
    ExpectationDraftConfigService,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext

pytestmark = pytest.mark.unit


@responses.activate
def test_create_expectation_draft_configs_success(mock_context: CloudDataContext):
    """Test successful creation of expectation draft configs."""
    # ARRANGE
    org_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    asset_id = uuid.uuid4()
    base_url = "https://test-base-url"
    auth_key = "test-auth-key"
    event_id = "test-event-id"

    # Mock the data source and asset
    mock_data_source = mock_context.data_sources.get.return_value
    mock_asset = mock_data_source.get_asset.return_value
    mock_asset.id = asset_id

    # Create test expectations
    expectation = ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["active", "inactive"],
    )

    # Mock API response
    draft_id = str(uuid.uuid4())
    api_response = {"data": [{"id": draft_id}]}

    responses.post(
        f"{base_url}/api/v1/organizations/{org_id}/workspaces/{workspace_id}/expectation-draft-configs",
        json=api_response,
        status=HTTPStatus.CREATED,
    )

    service = ExpectationDraftConfigService(
        context=mock_context,
        base_url=base_url,
        auth_key=auth_key,
        organization_id=org_id,
        workspace_id=workspace_id,
    )

    # ACT
    result = service.create_expectation_draft_configs(
        data_source_name="test_datasource",
        data_asset_name="test_asset",
        expectations=[expectation],
        event_id=event_id,
    )

    # ASSERT
    assert len(result) == 1
    assert result[0].resource_id == draft_id
    assert result[0].type == EXPECTATION_DRAFT_CONFIG

    # Verify the request was made with correct headers
    assert len(responses.calls) == 1
    assert responses.calls[0].request.headers["Agent-Job-Id"] == event_id


@responses.activate
def test_create_expectation_draft_configs_multiple(mock_context: CloudDataContext):
    """Test creation of multiple expectation draft configs."""
    # ARRANGE
    org_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    asset_id = uuid.uuid4()
    base_url = "https://test-base-url"
    auth_key = "test-auth-key"

    mock_data_source = mock_context.data_sources.get.return_value
    mock_asset = mock_data_source.get_asset.return_value
    mock_asset.id = asset_id

    expectations = [
        ExpectColumnValuesToBeInSet(column="status", value_set=["active", "inactive"]),
        ExpectColumnValuesToBeInSet(column="category", value_set=["A", "B", "C"]),
    ]

    draft_id_1 = str(uuid.uuid4())
    draft_id_2 = str(uuid.uuid4())
    api_response = {
        "data": [
            {"id": draft_id_1},
            {"id": draft_id_2},
        ]
    }

    responses.post(
        f"{base_url}/api/v1/organizations/{org_id}/workspaces/{workspace_id}/expectation-draft-configs",
        json=api_response,
        status=HTTPStatus.CREATED,
    )

    service = ExpectationDraftConfigService(
        context=mock_context,
        base_url=base_url,
        auth_key=auth_key,
        organization_id=org_id,
        workspace_id=workspace_id,
    )

    # ACT
    result = service.create_expectation_draft_configs(
        data_source_name="test_datasource",
        data_asset_name="test_asset",
        expectations=expectations,
        event_id="test-event-id",
    )

    # ASSERT
    assert len(result) == 2
    assert result[0].resource_id == draft_id_1
    assert result[1].resource_id == draft_id_2
    assert all(r.type == EXPECTATION_DRAFT_CONFIG for r in result)


@responses.activate
def test_create_expectation_draft_configs_api_failure(mock_context: CloudDataContext):
    """Test that API failure raises ExpectationDraftConfigError."""
    # ARRANGE
    org_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    asset_id = uuid.uuid4()
    base_url = "https://test-base-url"
    auth_key = "test-auth-key"

    mock_data_source = mock_context.data_sources.get.return_value
    mock_asset = mock_data_source.get_asset.return_value
    mock_asset.id = asset_id

    expectation = ExpectColumnValuesToBeInSet(column="status", value_set=["active"])

    responses.post(
        f"{base_url}/api/v1/organizations/{org_id}/workspaces/{workspace_id}/expectation-draft-configs",
        json={"error": "Bad request"},
        status=HTTPStatus.BAD_REQUEST,
    )

    service = ExpectationDraftConfigService(
        context=mock_context,
        base_url=base_url,
        auth_key=auth_key,
        organization_id=org_id,
        workspace_id=workspace_id,
    )

    # ACT & ASSERT
    with pytest.raises(ExpectationDraftConfigError):
        service.create_expectation_draft_configs(
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            expectations=[expectation],
            event_id="test-event-id",
        )


@responses.activate
def test_create_single_expectation_draft_config(mock_context: CloudDataContext):
    """Test creation of a single expectation draft config."""
    # ARRANGE
    org_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    asset_id = uuid.uuid4()
    base_url = "https://test-base-url"
    auth_key = "test-auth-key"

    mock_data_source = mock_context.data_sources.get.return_value
    mock_asset = mock_data_source.get_asset.return_value
    mock_asset.id = asset_id

    expectation = ExpectColumnValuesToBeInSet(column="status", value_set=["active"])

    draft_id = str(uuid.uuid4())
    api_response = {"data": [{"id": draft_id}]}

    responses.post(
        f"{base_url}/api/v1/organizations/{org_id}/workspaces/{workspace_id}/expectation-draft-configs",
        json=api_response,
        status=HTTPStatus.CREATED,
    )

    service = ExpectationDraftConfigService(
        context=mock_context,
        base_url=base_url,
        auth_key=auth_key,
        organization_id=org_id,
        workspace_id=workspace_id,
    )

    # ACT
    result = service.create_single_expectation_draft_config(
        data_source_name="test_datasource",
        data_asset_name="test_asset",
        expectation=expectation,
        event_id="test-event-id",
    )

    # ASSERT
    assert result.resource_id == draft_id
    assert result.type == EXPECTATION_DRAFT_CONFIG
