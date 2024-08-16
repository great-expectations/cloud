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
from sqlalchemy.engine import Inspector

from great_expectations_cloud.agent.actions import (
    ListTableNamesAction,
)
from great_expectations_cloud.agent.models import ListTableNamesEvent

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
    return ListTableNamesEvent(
        type="list_table_names_request.received",
        datasource_name="test-datasource",
        organization_id=uuid.uuid4(),
    )


def test_list_table_names_event_raises_for_non_sql_datasource(
    mock_context, event, mocker: MockerFixture
):
    action = ListTableNamesAction(
        context=mock_context,
        base_url="https://api.greatexpectations.io/",
        organization_id=uuid.uuid4(),
        auth_key="",
    )
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    mock_context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    mock_context.get_checkpoint.side_effect = StoreBackendError("test-message")
    datasource = mocker.Mock(spec=PandasDatasource)
    mock_context.get_datasource.return_value = datasource

    with pytest.raises(TypeError, match=r"This operation requires a SQL Data Source but got"):
        action.run(event=event, id=id)


@responses.activate
def test_run_list_table_names_action_returns_action_result(
    mock_context, event, dummy_base_url, dummy_org_id, set_required_env_vars, mocker: MockerFixture
):
    action = ListTableNamesAction(
        context=mock_context,
        base_url=dummy_base_url,
        organization_id=uuid.UUID(dummy_org_id),
        auth_key="",
    )
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    mock_inspect = mocker.patch("great_expectations_cloud.agent.actions.list_table_names.inspect")
    datasource = mocker.Mock(spec=SQLDatasource)
    datasource_id = str(uuid.uuid4())
    datasource.id = datasource_id
    mock_context.get_datasource.return_value = datasource

    responses.patch(
        re.compile(rf"{dummy_base_url}/organizations/{dummy_org_id}/datasources/.*"),
        status=204,
    )

    table_names = ["table_1", "table_2", "table_3"]
    inspector = mocker.Mock(spec=Inspector)
    inspector.get_table_names.return_value = table_names

    mock_inspect.return_value = inspector

    action_result = action.run(event=event, id=id)

    assert action_result.type == event.type
    assert action_result.id == id
    assert action_result.created_resources == []
