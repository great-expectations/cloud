from __future__ import annotations

import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest
import responses

from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    ExistingExpectationContext,
)
from great_expectations_cloud.agent.services.expectation_service import (
    ExpectationService,
    ListExpectationsError,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext

pytestmark = pytest.mark.unit


@responses.activate
def test_get_existing_expectations_by_data_asset_success(mock_context: CloudDataContext):
    """Test successful retrieval of existing expectations."""
    # ARRANGE
    org_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    asset_id = uuid.uuid4()
    base_url = "https://test-base-url"
    auth_key = "test-auth-key"

    # Mock the data source and asset
    mock_data_source = mock_context.data_sources.get.return_value
    mock_asset = mock_data_source.get_asset.return_value
    mock_asset.id = asset_id

    # Mock API response
    api_response = {
        "data": [
            {
                "config": {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "status"},
                    "description": "Status must be active or inactive",
                }
            },
            {
                "config": {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {},
                    "description": "Table should have reasonable row count",
                }
            },
        ]
    }

    responses.get(
        f"{base_url}/api/v1/organizations/{org_id}/workspaces/{workspace_id}/expectations?data_asset_id={asset_id}",
        json=api_response,
        status=HTTPStatus.OK,
    )

    service = ExpectationService(context=mock_context, base_url=base_url, auth_key=auth_key)

    # ACT
    result = service.get_existing_expectations_by_data_asset(
        data_source_name="test_datasource",
        data_asset_name="test_asset",
        organization_id=org_id,
        workspace_id=workspace_id,
    )

    # ASSERT
    assert len(result) == 2
    assert isinstance(result[0], ExistingExpectationContext)
    assert result[0].domain == "status"
    assert result[0].expectation_type == "expect_column_values_to_be_in_set"
    assert result[0].description == "Status must be active or inactive"

    assert result[1].domain == "table"
    assert result[1].expectation_type == "expect_table_row_count_to_be_between"
    assert result[1].description == "Table should have reasonable row count"


@responses.activate
def test_get_existing_expectations_empty_list(mock_context: CloudDataContext):
    """Test handling of empty expectations list."""
    # ARRANGE
    org_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    asset_id = uuid.uuid4()
    base_url = "https://test-base-url"
    auth_key = "test-auth-key"

    mock_data_source = mock_context.data_sources.get.return_value
    mock_asset = mock_data_source.get_asset.return_value
    mock_asset.id = asset_id

    responses.get(
        f"{base_url}/api/v1/organizations/{org_id}/workspaces/{workspace_id}/expectations?data_asset_id={asset_id}",
        json={"data": None},
        status=HTTPStatus.OK,
    )

    service = ExpectationService(context=mock_context, base_url=base_url, auth_key=auth_key)

    # ACT
    result = service.get_existing_expectations_by_data_asset(
        data_source_name="test_datasource",
        data_asset_name="test_asset",
        organization_id=org_id,
        workspace_id=workspace_id,
    )

    # ASSERT
    assert result == []


@responses.activate
def test_get_existing_expectations_api_failure(mock_context: CloudDataContext):
    """Test that API failure raises ListExpectationsError."""
    # ARRANGE
    org_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    asset_id = uuid.uuid4()
    base_url = "https://test-base-url"
    auth_key = "test-auth-key"

    mock_data_source = mock_context.data_sources.get.return_value
    mock_asset = mock_data_source.get_asset.return_value
    mock_asset.id = asset_id

    responses.get(
        f"{base_url}/api/v1/organizations/{org_id}/workspaces/{workspace_id}/expectations?data_asset_id={asset_id}",
        json={"error": "Not found"},
        status=HTTPStatus.NOT_FOUND,
    )

    service = ExpectationService(context=mock_context, base_url=base_url, auth_key=auth_key)

    # ACT & ASSERT
    with pytest.raises(ListExpectationsError):
        service.get_existing_expectations_by_data_asset(
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            organization_id=org_id,
            workspace_id=workspace_id,
        )
