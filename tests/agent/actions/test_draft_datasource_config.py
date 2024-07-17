from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import pytest
import responses
from great_expectations.datasource.fluent import SnowflakeDatasource, SQLDatasource
from great_expectations.datasource.fluent.interfaces import TestConnectionError

from great_expectations_cloud.agent.actions.draft_datasource_config_action import (
    DraftDatasourceConfigAction,
)
from great_expectations_cloud.agent.config import GxAgentEnvVars
from great_expectations_cloud.agent.exceptions import ErrorCode, GXCoreError
from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.fixture
def org_id():
    return "81f4e105-e37d-4168-85a0-2526943f9956"


@pytest.fixture
def token():
    return "MTg0NDkyYmYtNTBiOS00ZDc1LTk3MmMtYjQ0M2NhZDA2NjJk"


@pytest.fixture
def set_required_env_vars(monkeypatch, org_id, token) -> None:
    env_vars = {
        "GX_CLOUD_ORGANIZATION_ID": org_id,
        "GX_CLOUD_ACCESS_TOKEN": token,
    }
    for key, val in env_vars.items():
        monkeypatch.setenv(name=key, value=val)


def build_get_draft_config_payload(
    config: dict[str, Any], id: UUID
) -> dict[Literal["data"], dict[str, str | UUID | dict[str, Any]]]:
    return {
        "data": {
            "type": "draft_config",
            "id": str(id),
            "attributes": {"draft_config": config},
        }
    }


@responses.activate
def test_test_draft_datasource_config_success_non_sql_ds(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    datasource_config = {"type": "pandas", "name": "test-1-2-3"}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    env_vars = GxAgentEnvVars()
    action = DraftDatasourceConfigAction(
        context=mock_context,
        base_url=" https://api.greatexpectations.io/",
        auth_key="",
        organization_id=org_id,
    )

    _get_table_names_spy = mocker.spy(action, "_get_table_names")
    _update_table_names_list_spy = mocker.spy(action, "_update_table_names_list")

    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id, organization_id=uuid.uuid4())
    expected_url: str = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )

    responses.get(
        url=expected_url,
        json=build_get_draft_config_payload(config=datasource_config, id=config_id),
    )

    action_result = action.run(event=event, id=str(job_id))

    assert action_result.id == str(job_id)
    assert action_result.type == event.type
    assert action_result.created_resources == []

    # session.get.assert_called_with(expected_url)

    _get_table_names_spy.assert_not_called()
    _update_table_names_list_spy.assert_not_called()


@responses.activate
def test_test_draft_datasource_config_success_sql_ds(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    """
    Test that the action successfully tests a SQL datasource, introspects table names, and updates the table names list
    in the draft config.
    """
    ds_type = "snowflake"
    datasource_cls = mocker.Mock(
        spec=SnowflakeDatasource, return_value=mocker.Mock(spec=SnowflakeDatasource)
    )
    mock_context.sources.type_lookup = {ds_type: datasource_cls}

    datasource_config = {
        "name": "test_snowflake_ds",
        "connection_string": "snowflake://test_co:F3LLo1wxyP3HkbGfwVmS@oca29081.us-east-1/demo_db/restaurants?warehouse=compute_wh&role=opendata",
        "assets": [],
        "create_temp_table": False,
        "kwargs": {},
        "type": ds_type,
    }
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")

    # mock the sqlalchemy inspector, which is used to get table names
    inspect = mocker.patch(
        "great_expectations_cloud.agent.actions.draft_datasource_config_action.inspect"
    )
    table_names = ["table_1", "table_2", "table_3"]
    mock_inspector = inspect.return_value
    mock_inspector.get_table_names.return_value = table_names

    env_vars = GxAgentEnvVars()
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    action = DraftDatasourceConfigAction(
        context=mock_context,
        base_url=" https://api.greatexpectations.io/",
        auth_key="",
        organization_id=org_id,
    )

    # add spies to the action methods
    _get_table_names_spy = mocker.spy(action, "_get_table_names")
    _update_table_names_list_spy = mocker.spy(action, "_update_table_names_list")

    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id, organization_id=uuid.uuid4())
    expected_url: str = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )

    responses.get(
        url=expected_url,
        json=build_get_draft_config_payload(config=datasource_config, id=config_id),
    )
    # match will fail if patch not called with correct json data
    responses.patch(
        url=expected_url,
        status=204,
        match=[responses.matchers.json_params_matcher({"table_names": table_names})],
    )

    action_result = action.run(event=event, id=str(job_id))

    assert action_result.id == str(job_id)
    assert action_result.type == event.type
    assert action_result.created_resources == []

    # assert that the action properly calls helper methods to get table names and update the draft config
    _get_table_names_spy.assert_called_with(datasource=datasource_cls(**datasource_config))
    _update_table_names_list_spy.assert_called_with(config_id=config_id, table_names=table_names)


@responses.activate
def test_test_draft_datasource_config_sql_ds_raises_on_patch_failure(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    """
    Test that the action successfully tests a SQL datasource, introspects table names, and updates the table names list
    in the draft config.
    """
    ds_type = "snowflake"
    datasource_cls = mocker.Mock(
        spec=SnowflakeDatasource, return_value=mocker.Mock(spec=SnowflakeDatasource)
    )
    mock_context.sources.type_lookup = {ds_type: datasource_cls}

    datasource_config = {
        "name": "test_snowflake_ds",
        "connection_string": "snowflake://test_co:F3LLo1wxyP3HkbGfwVmS@oca29081.us-east-1/demo_db/restaurants?warehouse=compute_wh&role=opendata",
        "assets": [],
        "create_temp_table": False,
        "kwargs": {},
        "type": ds_type,
    }
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")

    # mock the sqlalchemy inspector, which is used to get table names
    inspect = mocker.patch(
        "great_expectations_cloud.agent.actions.draft_datasource_config_action.inspect"
    )
    table_names = ["table_1", "table_2", "table_3"]
    mock_inspector = inspect.return_value
    mock_inspector.get_table_names.return_value = table_names

    env_vars = GxAgentEnvVars()
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    action = DraftDatasourceConfigAction(
        context=mock_context,
        base_url=" https://api.greatexpectations.io/",
        auth_key="",
        organization_id=org_id,
    )

    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id, organization_id=uuid.uuid4())
    expected_url: str = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )

    responses.get(
        url=expected_url,
        json=build_get_draft_config_payload(config=datasource_config, id=config_id),
    )
    # match will fail if patch not called with correct json data
    responses.patch(
        url=expected_url,
        status=404,
        match=[responses.matchers.json_params_matcher({"table_names": table_names})],
    )

    with pytest.raises(RuntimeError, match="Unable to update table_names for Draft Config with ID"):
        action.run(event=event, id=str(job_id))


@responses.activate
def test_test_draft_datasource_config_failure(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    ds_type = "sql"
    datasource_config = {"type": ds_type, "name": "test-1-2-3"}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")

    env_vars = GxAgentEnvVars()
    base_url = " https://api.greatexpectations.io/"
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    action = DraftDatasourceConfigAction(
        context=mock_context,
        base_url=base_url,
        auth_key="",
        organization_id=org_id,
    )
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id, organization_id=uuid.uuid4())
    expected_url = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )
    datasource_cls = mocker.Mock(autospec=SQLDatasource)
    mock_context.sources.type_lookup = {ds_type: datasource_cls}
    datasource_cls.return_value.test_connection.side_effect = TestConnectionError

    responses.get(
        url=expected_url,
        json=build_get_draft_config_payload(config=datasource_config, id=config_id),
    )

    with pytest.raises(GXCoreError):
        action.run(event=event, id=str(job_id))


@responses.activate
def test_test_draft_datasource_config_raises_for_non_fds(mock_context, set_required_env_vars: None):
    datasource_config = {"name": "test-1-2-3", "connection_string": ""}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")

    env_vars = GxAgentEnvVars()
    base_url = " https://api.greatexpectations.io/"
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    action = DraftDatasourceConfigAction(
        context=mock_context,
        base_url=base_url,
        auth_key="",
        organization_id=org_id,
    )
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id, organization_id=uuid.uuid4())
    expected_url = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )
    responses.get(
        url=expected_url,
        json=build_get_draft_config_payload(config=datasource_config, id=config_id),
    )
    with pytest.raises(TypeError, match="fluent-style Data Source"):
        action.run(event=event, id=str(job_id))


@pytest.mark.parametrize(
    "error_message, expected_error_code",
    [
        (
            """Attempt to connect to datasource failed with the following error message: (snowflake.connector.errors.DatabaseError) 250001 (08001): None: Failed to connect to DB: <DB Name> Incorrect username or password was specified.\n(Background on this error at: https://sqlalche.me/e/14/4xp6)""",
            ErrorCode.WRONG_USERNAME_OR_PASSWORD,
        ),
        (
            """Unrecognized error.""",
            ErrorCode.GENERIC_UNHANDLED_ERROR,
        ),
    ],
)
def test_draft_datasource_config_failure_raises_correct_gx_core_error(
    mock_context, mocker: MockerFixture, error_message: str, expected_error_code: str
):
    base_url = " https://api.greatexpectations.io/"
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    action = DraftDatasourceConfigAction(
        context=mock_context,
        base_url=base_url,
        auth_key="",
        organization_id=org_id,
    )
    mock_check_draft_datasource_config = mocker.patch(
        f"{DraftDatasourceConfigAction.__module__}.{DraftDatasourceConfigAction.__name__}.check_draft_datasource_config"
    )
    mock_check_draft_datasource_config.side_effect = TestConnectionError(error_message)

    event = DraftDatasourceConfigEvent(config_id=uuid.uuid4(), organization_id=uuid.uuid4())
    with pytest.raises(GXCoreError) as e:
        action.run(event=event, id=str(uuid.uuid4()))

    assert e.value.error_code == expected_error_code
    assert e.value.get_error_params() == {}


@responses.activate
def test_test_draft_datasource_config_raises_for_unknown_type(
    mock_context, set_required_env_vars: None
):
    datasource_config = {"type": "not a datasource", "name": "test-1-2-3"}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")

    env_vars = GxAgentEnvVars()
    base_url = " https://api.greatexpectations.io/"
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    action = DraftDatasourceConfigAction(
        context=mock_context,
        base_url=base_url,
        auth_key="",
        organization_id=org_id,
    )
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id, organization_id=uuid.uuid4())
    expected_url = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )

    mock_context.sources.type_lookup = {}

    responses.get(
        url=expected_url,
        json=build_get_draft_config_payload(config=datasource_config, id=config_id),
    )

    with pytest.raises(TypeError, match="unknown Data Source type"):
        action.run(event=event, id=str(job_id))


@responses.activate
def test_test_draft_datasource_config_raises_for_cloud_backend_error(
    mock_context, set_required_env_vars: None
):
    datasource_config = {"type": "not a datasource", "name": "test-1-2-3"}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")

    env_vars = GxAgentEnvVars()
    base_url = " https://api.greatexpectations.io/"
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    action = DraftDatasourceConfigAction(
        context=mock_context,
        base_url=base_url,
        auth_key="",
        organization_id=org_id,
    )
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id, organization_id=uuid.uuid4())
    expected_url = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )

    responses.get(
        url=expected_url,
        json=build_get_draft_config_payload(config=datasource_config, id=config_id),
        status=404,
    )

    with pytest.raises(RuntimeError, match="error while connecting to GX Cloud"):
        action.run(event=event, id=str(job_id))
