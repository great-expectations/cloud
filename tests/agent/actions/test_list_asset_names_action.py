from __future__ import annotations

import re
import uuid
from typing import TYPE_CHECKING

import pytest
import responses
from great_expectations.datasource.fluent import (
    PandasDatasource,
    SQLDatasource,
)
from great_expectations.exceptions import StoreBackendError

from great_expectations_cloud.agent.actions import (
    ListAssetNamesAction,
)
from great_expectations_cloud.agent.actions.utils import get_asset_names
from great_expectations_cloud.agent.models import ListAssetNamesEvent

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


@pytest.fixture
def dummy_base_url() -> str:
    return "https://test-base-url"


@pytest.fixture
def dummy_org_id() -> str:
    return "94af8c91-6e56-4f2a-9b1f-04868321c5f5"


@pytest.fixture
def dummy_access_token() -> str:
    return (
        "5a43e329ddd64ca286bf58574d17f9e1.V1"
        ".myBie29LH0m_5CTw0RCeAtBXWb5V519lAeIq1rF4WWN4WZrJGMe0GAMcnuuwYkveR0ptvUFoeeK2zCt6NJMiSg"
    )


@pytest.fixture
def set_required_env_vars(monkeypatch, dummy_org_id, dummy_base_url, dummy_access_token):
    env_vars = {
        "GX_CLOUD_ORGANIZATION_ID": dummy_org_id,
        "GX_CLOUD_ACCESS_TOKEN": dummy_access_token,
        "GX_CLOUD_BASE_URL": dummy_base_url,
    }
    for key, val in env_vars.items():
        monkeypatch.setenv(name=key, value=val)


@pytest.fixture
def event():
    return ListAssetNamesEvent(
        type="list_table_names_request.received",
        datasource_name="test-datasource",
        organization_id=uuid.uuid4(),
    )


def test_list_table_names_event_raises_for_non_sql_datasource(
    mock_context, event, mocker: MockerFixture
):
    action = ListAssetNamesAction(
        context=mock_context,
        base_url="https://api.greatexpectations.io/",
        organization_id=uuid.uuid4(),
        auth_key="",
    )
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    mock_context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    mock_context.get_checkpoint.side_effect = StoreBackendError("test-message")
    datasource = mocker.Mock(spec=PandasDatasource)
    mock_context.data_sources.get.return_value = datasource

    with pytest.raises(TypeError, match=r"This operation requires a SQL Data Source but got"):
        action.run(event=event, id=id)


@responses.activate
def test_run_list_table_names_action_returns_action_result(
    mock_context, event, dummy_base_url, dummy_org_id, set_required_env_vars, mocker: MockerFixture
):
    action = ListAssetNamesAction(
        context=mock_context,
        base_url=dummy_base_url,
        organization_id=uuid.UUID(dummy_org_id),
        auth_key="",
    )
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    _get_asset_names_spy = mocker.patch(
        "great_expectations_cloud.agent.actions.list_asset_names.get_asset_names",
        wraps=get_asset_names,
    )

    datasource = mocker.Mock(spec=SQLDatasource)
    datasource_id = str(uuid.uuid4())
    datasource.id = datasource_id
    mock_context.data_sources.get.return_value = datasource

    responses.put(
        re.compile(rf"{dummy_base_url}/api/v1/organizations/{dummy_org_id}/table-names/.*"),
        status=200,
    )

    asset_names = ["table_1", "table_2", "table_3", "view_1", "view_2", "view_3"]
    _get_asset_names_spy.return_value = asset_names

    action_result = action.run(event=event, id=id)

    _get_asset_names_spy.assert_called_with(datasource)
    assert action_result.type == event.type
    assert action_result.id == id
    assert action_result.created_resources == []
