from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

import pytest
from great_expectations.core import ExpectationConfiguration
from great_expectations.experimental.metric_repository.metrics import MetricTypes

from great_expectations_cloud.agent.actions import MetricListAction
from great_expectations_cloud.agent.models import (
    RunMetricsListEvent,
)

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
    get_missing_expectation_suite_error_type: type[Exception],
) -> Iterator[ExpectationSuite]:
    expectation_suite_name = f"{data_asset.datasource.name} | {data_asset.name}"
    expectation_suite = context.add_expectation_suite(
        expectation_suite_name=expectation_suite_name,
    )
    expectation_suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "string",
                "mostly": 1,
            },
        )
    )
    _ = context.add_or_update_expectation_suite(expectation_suite=expectation_suite)
    expectation_suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
    assert (
        len(expectation_suite.expectations) == 1
    ), "Expectation Suite was not updated in the previous method call."
    yield expectation_suite
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
    with pytest.raises(get_missing_expectation_suite_error_type):
        context.get_expectation_suite(expectation_suite_name=expectation_suite_name)


@pytest.fixture
def metric_list_event(datasource, data_asset):
    return RunMetricsListEvent(
        type="metrics_list_request.received",
        datasource_name=datasource.name,
        data_asset_name=data_asset.name,
        metric_names=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
    )


def test_running_metric_list_action(
    context, metric_list_event, datasource, data_asset, batch_request, cloud_base_url, org_id, token
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
            ],
        ),
        id="test-id",
    )
    action_result = action.run(event=metric_list_event, id=event_id, batch_request=batch_request)
    assert action_result.type == metric_list_event.type
    assert action_result.id == event_id
    # metric_run_event_id = action_result.created_resources[0].resource_id
    # resource_url = f"{cloud_base_url}/organizations/" f"{org_id}/metric-runs/{metric_run_event_id}"
    # session = create_session(access_token=token)
    # response = session.get(resource_url)
    # data = response.json()
    # print(data)
