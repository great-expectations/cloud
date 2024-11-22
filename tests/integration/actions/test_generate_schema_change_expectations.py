from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest

from great_expectations_cloud.agent.actions.generate_schema_change_expectations_action import (
    GenerateSchemaChangeExpectationsAction,
)
from great_expectations_cloud.agent.models import (
    GenerateSchemaChangeExpectationsEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import PostgresDatasource
    from great_expectations.datasource.fluent.sql_datasource import TableAsset

pytestmark = pytest.mark.integration


@pytest.fixture
def user_api_token_headers_org_admin_sc_org():
    api_token = os.environ.get("GX_CLOUD_ACCESS_TOKEN")
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/vnd.api+json",
    }


@pytest.fixture
def org_id_env_var_local():
    return "0ccac18e-7631-4bdd-8a42-3c35cce574c6"


@pytest.fixture(scope="module")
def cloud_base_url_local() -> str:
    return "http://localhost:7000"


@pytest.fixture
def token_env_var_local():
    return os.environ.get("GX_CLOUD_ACCESS_TOKEN")


@pytest.fixture
def local_mercury_db_datasource(
    context: CloudDataContext,
) -> PostgresDatasource:
    datasource_name = "local_mercury_db"
    datasource = context.data_sources.get(datasource_name)
    yield datasource


@pytest.fixture
def local_mercury_db_organizations_table_asset(
    local_mercury_db_datasource: PostgresDatasource,
) -> TableAsset:
    data_asset_name = "local-mercury-db-organizations-table"
    data_asset = local_mercury_db_datasource.get_asset(name=data_asset_name)
    yield data_asset


@pytest.mark.skip(reason="This test will be updated in a follow-up PR")
def test_running_schema_change_expectation_action(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    local_mercury_db_datasource: PostgresDatasource,
    local_mercury_db_organizations_table_asset: TableAsset,
    org_id_env_var_local: str,
    cloud_base_url_local: str,
    token_env_var_local: str,
):
    generate_schema_change_expectations_event = GenerateSchemaChangeExpectationsEvent(
        type="generate_schema_change_expectations_request.received",
        datasource_name="local_mercury_db",
        data_assets=["checkpoints"],
        organization_id=org_id_env_var_local,
    )

    action = GenerateSchemaChangeExpectationsAction(
        context=context,
        base_url=cloud_base_url_local,
        organization_id=uuid.UUID(org_id_env_var_local),
        auth_key=token_env_var_local,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    action_result = action.run(event=generate_schema_change_expectations_event, id=event_id)

    # Assert
    # Check that the action was successful e.g. that we can create metrics_list_request action
    assert action_result.type == generate_schema_change_expectations_event.type
    assert action_result.id == event_id
