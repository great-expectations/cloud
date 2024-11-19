from __future__ import annotations

import json
import os
import uuid
from typing import TYPE_CHECKING

import pytest
import requests
from great_expectations.experimental.metric_repository.metrics import MetricTypes

from great_expectations_cloud.agent.actions import MetricListAction
from great_expectations_cloud.agent.models import (
    RunMetricsListEvent,
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
def query():
    # only the table metrics
    return """
        query Asset($assetId: UUID!) {
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
    mercury_api_port = 5000

    class GraphQlTestClient:
        client = requests.Session()

        def execute(self, query, variables=None, headers=None, status_code=None):
            headers = {**(headers or {}), "Content-Type": "application/json"}

            payload_dict = {"query": query}
            if variables is not None:
                payload_dict["variables"] = variables

            payload = json.dumps(payload_dict)
            graphql_endpoint = f"http://{mercury_api_host}:{mercury_api_port}/organizations/{org_id_env_var}/graphql"
            res = self.client.post(
                url=graphql_endpoint,
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
    datasource = context.data_sources.get(datasource_name)
    yield datasource


@pytest.fixture
def local_mercury_db_organizations_table_asset(
    local_mercury_db_datasource: PostgresDatasource,
) -> TableAsset:
    data_asset_name = "local-mercury-db-organizations-table"
    data_asset = local_mercury_db_datasource.get_asset(name=data_asset_name)
    yield data_asset


def test_running_metric_list_action(
    context: CloudDataContext,
    graphql_test_client,
    user_api_token_headers_org_admin_sc_org,
    query: str,
    local_mercury_db_datasource: PostgresDatasource,
    local_mercury_db_organizations_table_asset: TableAsset,
    org_id_env_var: str,
    token_env_var: str,
):
    cloud_base_url = "http://localhost:7000"

    # MetricListEvent with only the Table Metrics requested
    metrics_list_event = RunMetricsListEvent(
        type="metrics_list_request.received",
        datasource_name=local_mercury_db_datasource.name,
        data_asset_name=local_mercury_db_organizations_table_asset.name,
        metric_names=[
            MetricTypes.TABLE_COLUMN_TYPES,
            MetricTypes.TABLE_COLUMNS,
            MetricTypes.TABLE_ROW_COUNT,
        ],
        organization_id=uuid.UUID(org_id_env_var),
    )

    action = MetricListAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=uuid.UUID(org_id_env_var),
        auth_key=token_env_var,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    # Act
    action.run(
        event=metrics_list_event,
        id="test-id",
    )
    action_result = action.run(event=metrics_list_event, id=event_id)

    # Assert
    # Check that the action was successful e.g. that we can create metrics_list_request action
    assert action_result.type == metrics_list_event.type
    assert action_result.id == event_id

    # Check that metrics_list_request was successful by querying DB.
    result = graphql_test_client.execute(
        query,
        headers=user_api_token_headers_org_admin_sc_org,
        variables={
            "assetId": f"{local_mercury_db_organizations_table_asset.id}",
        },
    )

    assert result["data"]["dataAsset"]["id"] == f"{local_mercury_db_organizations_table_asset.id}"
    assert result["data"]["dataAsset"]["name"] == local_mercury_db_organizations_table_asset.name
    assert result["data"]["dataAsset"]["datasourceId"] == f"{local_mercury_db_datasource.id}"
    # assert rowCount is computed and Metrics is non-empty
    assert result["data"]["dataAsset"]["latestMetricRun"]["metrics"]
    assert result["data"]["dataAsset"]["latestMetricRun"]["rowCount"] != 0
