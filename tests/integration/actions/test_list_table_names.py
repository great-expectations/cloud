from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from great_expectations_cloud.agent.actions import ListTableNamesAction
from great_expectations_cloud.agent.models import ListTableNamesEvent

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.integration


def test_running_list_table_names_action(
    context: CloudDataContext,
    cloud_base_url: str,
    org_id_env_var: str,
    token_env_var: str,
    mocker: MockerFixture,
):
    # Arrange
    action = ListTableNamesAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=UUID(org_id_env_var),
        auth_key=token_env_var,
    )

    list_table_names_event = ListTableNamesEvent(
        type="list_table_names_request.received",
        datasource_name="local_mercury_db",
        organization_id=UUID(org_id_env_var),
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    datasource_id_for_connect_successfully = (
        "2ccfea7f-3f91-47f2-804e-2106aa07ef24"  # local_mercury_db
    )
    expected_table_names = [
        "alembic_version",
        "organization_api_tokens",
        "asset_refs",
        "alert_emails",
        "asset_alert_emails",
        "checkpoints",
        "data_context_variables",
        "datasources",
        "expectation_suites",
        "organizations",
        "organizations_auth0_orgs",
        "sso_organization_email_domains",
        "api_tokens",
        "users",
        "agent_job_created_resources",
        "agent_job_source_resources",
        "draft_configs",
        "metric_runs",
        "metrics",
        "agent_jobs",
        "expectations",
        "expectation_changes",
        "system_users",
        "batch_definitions",
        "checkpoint_job_schedules",
        "expectation_validation_results",
        "suite_validation_results",
        "user_api_tokens",
        "auth0_users",
        "organization_users",
        "user_asset_alerts",
        "validations",
    ]

    # add spy to the action method
    _add_or_update_table_names_list = mocker.spy(action, "_add_or_update_table_names_list")

    # Act
    result = action.run(event=list_table_names_event, id=event_id)

    # Assert
    # Check that the action was successful e.g. that we can connect to the datasource and list table names
    assert result
    assert result.id == event_id
    assert result.type == list_table_names_event.type
    assert result.created_resources == []

    _add_or_update_table_names_list.assert_called_once()
    call_args = _add_or_update_table_names_list.call_args
    assert call_args.kwargs["datasource_id"] == datasource_id_for_connect_successfully
    assert set(call_args.kwargs["table_names"]) == set(expected_table_names)


def test_running_list_table_names_action_fails_for_unreachable_datasource(
    context: CloudDataContext, cloud_base_url: str, org_id_env_var: str, token_env_var: str
):
    # Arrange
    action = ListTableNamesAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=UUID(org_id_env_var),
        auth_key=token_env_var,
    )
    list_table_names_event = ListTableNamesEvent(
        type="list_table_names_request.received",
        organization_id=UUID(org_id_env_var),
        datasource_name="local_mercury_db_bad_password",
    )
    event_id = "64842838-c7bf-4038-8b27-c7a32eba4b7b"

    # Act & Assert
    # Check that the action was unsuccessful and an error was raised.
    with pytest.raises(KeyError):
        action.run(event=list_table_names_event, id=event_id)
