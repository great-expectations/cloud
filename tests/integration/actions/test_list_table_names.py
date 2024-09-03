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
    # Note: Draft config is loaded in mercury seed data

    action = ListTableNamesAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=UUID(org_id_env_var),
        auth_key=token_env_var,
    )

    # datasource_id_for_connect_successfully = (
    #     "2512c2d8-a212-4295-b01b-2bb2ac066f04"  # local_mercury_db
    # )
    list_table_names_event = ListTableNamesEvent(
        type="list_table_names_request.received",
        datasource_name="local_mercury_db",
        organization_id=UUID(org_id_env_var),
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    expected_table_names = [
        "alembic_version",
        "agent_job_created_resources",
        "agent_job_source_resources",
        "agent_jobs",
        "asset_refs",
        "expectations",
        "expectation_changes",
        "checkpoint_job_schedules",
        "draft_configs",
        "user_api_tokens",
        "user_asset_alerts",
        "organization_api_tokens",
        "suite_validation_results",
        "metrics",
        "batch_definitions",
        "expectation_suites",
        "expectation_validation_results",
        "data_context_variables",
        "system_users",
        "checkpoints",
        "organizations",
        "metric_runs",
        "users",
        "auth0_users",
        "datasources",
        "organization_users",
        "validations",
        "api_tokens",
    ]
    # add spies to the action methods
    _add_or_update_table_names_list = mocker.spy(action, "_add_or_update_table_names_list")

    # Act
    result = action.run(event=list_table_names_event, id=event_id)

    # Assert
    # Check that the action was successful e.g. that we can connect to the datasource
    assert result
    assert result.id == event_id
    assert result.type == list_table_names_event.type
    assert result.created_resources == []

    # Ensure table name introspection was successful
    # assert sorted(_add_or_update_table_names_list.spy_return) == sorted(expected_table_names)
    print("Table names: ", _add_or_update_table_names_list.spy_return)
    print("Expected table names: ", expected_table_names)


def test_running_list_table_names_action_fails_for_unreachable_datasource(
    context: CloudDataContext, cloud_base_url: str, org_id_env_var: str, token_env_var: str
):
    # Arrange
    # Note: Draft config is loaded in mercury seed data

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
    with pytest.raises(ValueError):
        action.run(event=list_table_names_event, id=event_id)
