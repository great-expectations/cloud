from __future__ import annotations

import json

import pytest
import requests
from great_expectations.experimental.metric_repository.metrics import MetricTypes

from great_expectations_cloud.agent.actions import MetricListAction
from great_expectations_cloud.agent.models import (
    RunMetricsListEvent,
)


@pytest.fixture
def user_api_token_headers_org_admin_sc_org():
    # api_token: str = "d47995c6872540429c98f94d6182b5c4.V1.6CmUs5x39flxrKpltY9PYRYiN8DUjNy8vZSaB70nztPpWMg-80LeyjSl-yQvDnxnoNwJeXlBc-fB3HhosVPa7A"
    api_token: str = "808d6f9e99c94caaaf70baac1a4581ee.V1.kPujCIKZZDLdhw8xUuTb8ZX7oIRhIQY0MgrxRS8sPlX6Dp8121jSTcMlmPwhIMgciNIqkXrTMkgZ5invhUfuFg"
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/vnd.api+json",
    }


@pytest.fixture
def query():
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
                        valueRangeMin
                        valueRangeMax
                        valueRangeMinUnion {
                            __typename
                            ...MetricValueStringTypeFragment
                            ...MetricValueFloatTypeFragment
                        }
                        valueRangeMaxUnion {
                            __typename
                            ...MetricValueStringTypeFragment
                            ...MetricValueFloatTypeFragment
                        }
                        mean
                        median
                        nullCount
                    }
                }
            }
        }

        fragment MetricValueStringTypeFragment on MetricValueStringType {
            __typename
            stringValue
        }

        fragment MetricValueFloatTypeFragment on MetricValueFloatType {
            __typename
            floatValue
        }
    """


@pytest.fixture
def graphql_test_client(
    org_id,
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

            res = self.client.post(
                f"http://{mercury_api_host}:{mercury_api_port}/organizations" f"/{org_id}/graphql",
                data=payload,
                headers=headers,
            )

            if status_code is not None:
                assert res.status_code == status_code

            return res.json()

    return GraphQlTestClient()


@pytest.fixture
def metric_list_event(datasource, data_asset):
    return RunMetricsListEvent(
        type="metrics_list_request.received",
        datasource_name=datasource.name,
        data_asset_name=data_asset.name,
        metric_names=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
    )


def test_running_metric_list_action(
    context,
    metric_list_event,
    datasource,
    data_asset,
    batch_request,
    cloud_base_url,
    org_id,
    token,
    graphql_test_client,
    user_api_token_headers_org_admin_sc_org,
    query,
):
    action = MetricListAction(context=context)
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    action.run(
        event=RunMetricsListEvent(
            type="metrics_list_request.received",
            datasource_name=datasource.name,
            data_asset_name=data_asset.name,
            metric_names=[
                MetricTypes.TABLE_COLUMN_TYPES,
                MetricTypes.TABLE_COLUMNS,
                MetricTypes.TABLE_ROW_COUNT,
                MetricTypes.COLUMN_NULL_COUNT,
                MetricTypes.COLUMN_MAX,
                MetricTypes.COLUMN_MIN,
                MetricTypes.COLUMN_MEAN,
                MetricTypes.COLUMN_MEDIAN,
            ],
        ),
        id="test-id",
    )
    action_result = action.run(event=metric_list_event, id=event_id, batch_request=batch_request)
    assert action_result.type == metric_list_event.type
    assert action_result.id == event_id
    print(user_api_token_headers_org_admin_sc_org)
    result = graphql_test_client.execute(
        query,
        headers=user_api_token_headers_org_admin_sc_org,
        variables={
            "assetId": data_asset.name,
        },
    )
    print(result)

    raise Exception(result)  # noqa: TRY002
    # metric_run_event_id = action_result.created_resources[0].resource_id
    # resource_url = f"{cloud_base_url}/organizations/" f"{org_id}/metric-runs"
    # print(resource_url)
    # session = create_session(access_token=token)
    # response = session.get(resource_url)
    # print(response)
    # print("hi hihi")
    # #data = response.json()
    # # print(data)
