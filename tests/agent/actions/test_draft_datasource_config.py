from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import pytest
from great_expectations.datasource.fluent import SQLDatasource
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


def build_payload(
    config: dict[str, Any], id: UUID
) -> dict[Literal["data"], dict[str, str | UUID | dict[str, Any]]]:
    return {
        "data": {
            "type": "draft_config",
            "id": id,
            "attributes": {"draft_config": config},
        }
    }


def test_test_draft_datasource_config_success(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    datasource_config = {"type": "pandas", "name": "test-1-2-3"}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    create_session = mocker.patch(
        "great_expectations_cloud.agent.actions.draft_datasource_config_action.create_session"
    )
    session = create_session.return_value
    response = session.get.return_value
    response.ok = True
    response.json.return_value = build_payload(config=datasource_config, id=config_id)
    env_vars = GxAgentEnvVars()
    action = DraftDatasourceConfigAction(context=mock_context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id)
    expected_url: str = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )

    action_result = action.run(event=event, id=str(job_id))

    assert action_result.id == str(job_id)
    assert action_result.type == event.type
    assert action_result.created_resources == []

    session.get.assert_called_with(expected_url)


def test_test_draft_datasource_config_failure(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    ds_type = "sql"
    datasource_config = {"type": ds_type, "name": "test-1-2-3"}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    create_session = mocker.patch(
        "great_expectations_cloud.agent.actions.draft_datasource_config_action.create_session"
    )
    session = create_session.return_value
    response = session.get.return_value
    response.ok = True
    response.json.return_value = build_payload(config=datasource_config, id=config_id)
    env_vars = GxAgentEnvVars()
    action = DraftDatasourceConfigAction(context=mock_context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id)
    expected_url = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )
    datasource_cls = mocker.Mock(autospec=SQLDatasource)
    mock_context.sources.type_lookup = {ds_type: datasource_cls}
    datasource_cls.return_value.test_connection.side_effect = TestConnectionError

    with pytest.raises(GXCoreError):
        action.run(event=event, id=str(job_id))

    session.get.assert_called_with(expected_url)


def test_test_draft_datasource_config_raises_for_non_fds(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    datasource_config = {"name": "test-1-2-3", "connection_string": ""}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    create_session = mocker.patch(
        "great_expectations_cloud.agent.actions.draft_datasource_config_action.create_session"
    )
    session = create_session.return_value
    response = session.get.return_value
    response.ok = True
    response.json.return_value = build_payload(config=datasource_config, id=config_id)
    env_vars = GxAgentEnvVars()
    action = DraftDatasourceConfigAction(context=mock_context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id)
    expected_url = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )
    with pytest.raises(ValueError, match="fluent-style Data Source"):
        action.run(event=event, id=str(job_id))

    session.get.assert_called_with(expected_url)


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
    action = DraftDatasourceConfigAction(context=mock_context)
    mock_check_draft_datasource_config = mocker.patch(
        f"{DraftDatasourceConfigAction.__module__}.{DraftDatasourceConfigAction.__name__}.check_draft_datasource_config"
    )
    mock_check_draft_datasource_config.side_effect = TestConnectionError(error_message)

    event = DraftDatasourceConfigEvent(config_id=uuid.uuid4())
    with pytest.raises(GXCoreError) as e:
        action.run(event=event, id=str(uuid.uuid4()))

    assert e.value.error_code == expected_error_code
    assert e.value.get_error_params() == {}


def test_test_draft_datasource_config_raises_for_unknown_type(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    datasource_config = {"type": "not a datasource", "name": "test-1-2-3"}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    create_session = mocker.patch(
        "great_expectations_cloud.agent.actions.draft_datasource_config_action.create_session"
    )
    session = create_session.return_value
    response = session.get.return_value
    response.ok = True
    response.json.return_value = build_payload(config=datasource_config, id=config_id)
    env_vars = GxAgentEnvVars()
    action = DraftDatasourceConfigAction(context=mock_context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id)
    expected_url = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )

    mock_context.sources.type_lookup = {}

    with pytest.raises(ValueError, match="unknown Data Source type"):
        action.run(event=event, id=str(job_id))

    session.get.assert_called_with(expected_url)


def test_test_draft_datasource_config_raises_for_cloud_backend_error(
    mock_context, mocker: MockerFixture, set_required_env_vars: None
):
    datasource_config = {"type": "not a datasource", "name": "test-1-2-3"}
    config_id = UUID("df02b47c-e1b8-48a8-9aaa-b6ed9c49ffa5")
    create_session = mocker.patch(
        "great_expectations_cloud.agent.actions.draft_datasource_config_action.create_session"
    )
    session = create_session.return_value
    response = session.get.return_value
    response.ok = False
    response.json.return_value = build_payload(config=datasource_config, id=config_id)
    env_vars = GxAgentEnvVars()
    action = DraftDatasourceConfigAction(context=mock_context)
    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    event = DraftDatasourceConfigEvent(config_id=config_id)
    expected_url = (
        f"{env_vars.gx_cloud_base_url}/organizations/{env_vars.gx_cloud_organization_id}"
        f"/datasources/drafts/{config_id}"
    )

    with pytest.raises(RuntimeError, match="error while connecting to GX Cloud"):
        action.run(event=event, id=str(job_id))

    session.get.assert_called_with(expected_url)
