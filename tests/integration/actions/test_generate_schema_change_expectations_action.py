from __future__ import annotations

import json
import os
import uuid
from typing import TYPE_CHECKING

import pytest
import requests

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
    # This should be pointing to the local GX cloud access token
    api_token = os.environ.get("GX_CLOUD_ACCESS_TOKEN")
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/vnd.api+json",
    }


@pytest.fixture
def query():
    return """
        queryAsset($assetId: UUID!) {
            dataAsset(id: $assetId) {
                id
                name
                datasourceId
                aggregateSuccessStatus
                lastValidatedAt
                latestMetricRun {
                    dataAssetId
                    lastFetched
                    rowCount
                    metrics {
                        columnName
                        columnDataType
                    }
                }
            }
        }
    """


@pytest.fixture
def graphql_test_client(
    org_id_env_var,
):
    mercury_api_host = "localhost"
    # TODO - check if this needs to be changed to 7000
    mercury_api_port = 5000

    class GraphQlTestClient:
        client = requests.Session()

        def execute(self, query, variables=None, headers=None, status_code=None):
            headers = {**(headers or {}), "Content-Type": "application/json"}

            payload_dict = {"query": query}
            if variables is not None:
                payload_dict["variables"] = variables

            payload = json.dumps(payload_dict)

            res = self.client.post(
                f"http://{mercury_api_host}:{mercury_api_port}/organizations"
                f"/{org_id_env_var}/graphql",
                data=payload,
                headers=headers,
            )

            if status_code is not None:
                assert res.status_code == status_code

            return res.json()

    return GraphQlTestClient()


@pytest.fixture
def local_mercury_db_datasource(
    context: CloudDataContext,
) -> PostgresDatasource:
    datasource_name = "local_mercury_db"
    datasource = context.data_sources.get(name=datasource_name)
    yield datasource


@pytest.fixture
def local_mercury_db_organizations_table_asset(
    local_mercury_db_datasource: PostgresDatasource,
) -> TableAsset:
    data_asset_name = "local-mercury-db-organizations-table"
    data_asset = local_mercury_db_datasource.get_asset(name=data_asset_name)
    yield data_asset


@pytest.fixture
def local_mercury_db_expectation_suite_id():
    # There is no ExpectationSuite that is associated with the local_mercury_db data asset
    # after creating it in the UI, this is the UUID that is associated with it.
    # TODO - update the seed data so that an ExpectationSuite can be added as part of the db-init process
    return uuid.UUID("35c7a8ec-71fb-4b5b-90e7-7c23b83eeef0")


def test_generate_schema_change_expectations_action(
    context: CloudDataContext,
    graphql_test_client,
    user_api_token_headers_org_admin_sc_org,
    query: str,
    local_mercury_db_datasource: PostgresDatasource,
    local_mercury_db_organizations_table_asset: TableAsset,
    local_mercury_db_expectation_suite_id: uuid.UUID,
    org_id_env_var: str,
    cloud_base_url: str,
    token_env_var: str,
):
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    # Mercury
    action = GenerateSchemaChangeExpectationsAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=uuid.UUID(org_id_env_var),
        auth_key=token_env_var,
    )

    generate_schema_change_expectation_event = GenerateSchemaChangeExpectationsEvent(
        type="generate_schema_change_expectations_request.received",
        datasource_name=local_mercury_db_datasource.name,
        data_assets=[local_mercury_db_organizations_table_asset.name],
        organization_id=uuid.UUID(org_id_env_var),
        expectation_suite_id=local_mercury_db_expectation_suite_id,
        create_expectations=True,
    )

    action_result = action.run(event=generate_schema_change_expectation_event, id=event_id)
    assert action_result.id == event_id

    # Assert
    # Check that the action was successful e.g. that we can create metrics_list_request action
    assert action_result.type == generate_schema_change_expectation_event.type
    assert action_result.id == event_id

    # Check that metrics_list_request was successful by querying DB.
    result = graphql_test_client.execute(
        query,
        headers=user_api_token_headers_org_admin_sc_org,
        variables={
            "assetId": f"{local_mercury_db_organizations_table_asset.id}",
        },
    )

    print(result)
