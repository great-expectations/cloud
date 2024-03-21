from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    MetricException,
    MetricRun,
    MetricTypes,
)

from great_expectations_cloud.agent.actions import MetricListAction
from great_expectations_cloud.agent.models import CreatedResource, RunMetricsEvent

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


def test_run_metrics_list_computes_metric_run(
    mock_context: CloudDataContext, mocker: MockerFixture
):
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = MetricListAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    action._raise_on_any_metric_exception = mocker.Mock()  # mock so that we don't raise

    action.run(
        event=RunMetricsEvent(
            type="metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
            metric_list=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
        ),
        id="test-id",
    )
    mock_batch_inspector.compute_metric_list_run.assert_called_once()


def test_run_metrics_list_computes_metric_run_missing_batch_inspector(
    mock_context, mocker: MockerFixture
):
    """
    batch_inspector is passed in or generated by the action
    """
    # Patching here bc when the batch_inspector is not passed in, it is generated by the action
    mocker.patch("great_expectations_cloud.agent.actions.run_metrics_list_action.BatchInspector")

    mock_metric_repository = mocker.Mock(spec=MetricRepository)

    action = MetricListAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=None,
    )

    action._raise_on_any_metric_exception = mocker.Mock()  # mock so that we don't raise

    action.run(
        event=RunMetricsEvent(
            type="metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
            metric_list=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
        ),
        id="test-id",
    )

    # Asserting against mocked class proved challenging
    action._batch_inspector.compute_metric_list_run.assert_called_once()


def test_run_metrics_list_creates_metric_run(mock_context, mocker: MockerFixture):
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    mock_metric_run = mocker.Mock(spec=MetricRun)
    mock_batch_inspector.compute_metric_list_run.return_value = mock_metric_run

    action = MetricListAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    action._raise_on_any_metric_exception = mocker.Mock()  # mock so that we don't raise

    action.run(
        event=RunMetricsEvent(
            type="metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
            metric_list=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
        ),
        id="test-id",
    )

    mock_metric_repository.add_metric_run.assert_called_once_with(mock_metric_run)


def test_run_metrics_list_returns_action_result(mock_context, mocker: MockerFixture):
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    metric_run_id = uuid.uuid4()
    mock_metric_repository.add_metric_run.return_value = metric_run_id

    action = MetricListAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )
    action._raise_on_any_metric_exception = mocker.Mock()  # mock so that we don't raise

    action_result = action.run(
        event=RunMetricsEvent(
            type="metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
            metric_list=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
        ),
        id="test-id",
    )

    assert action_result.type == "metrics_request.received"
    assert action_result.id == "test-id"
    assert action_result.created_resources == [
        CreatedResource(resource_id=str(metric_run_id), type="MetricRun"),
    ]


def test_run_column_descriptive_metrics_raises_on_test_connection_to_data_asset_failure(
    mock_context, mocker: MockerFixture
):
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    mock_datasource = mocker.Mock()
    mock_context.get_datasource.return_value = mock_datasource
    mock_data_asset = mocker.Mock()
    mock_datasource.get_asset.return_value = mock_data_asset
    mock_data_asset.test_connection.side_effect = TestConnectionError()

    action = MetricListAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    with pytest.raises(TestConnectionError):
        action.run(
            event=RunMetricsEvent(
                type="metrics_request.received",
                datasource_name="test-datasource",
                data_asset_name="test-data-asset",
                metric_list=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
            ),
            id="test-id",
        )

    mock_batch_inspector.compute_metric_run.assert_not_called()


def test_run_metrics_list_creates_metric_run_then_raises_on_any_metric_exception(
    mock_context, mocker: MockerFixture
):
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    # Using a real metric with a real exception in the metric run to test the exception handling
    mock_metric_run = MetricRun(
        metrics=[
            # Metric with an exception within the MetricRun should cause the action to raise:
            ColumnMetric[int](
                batch_id="batch_id",
                metric_name="column_values.null.count",
                value=1,
                exception=MetricException(
                    type="test-exception",
                    message="exception message",
                ),
                column="col1",
            ),
            # Also, a Metric with no exception within the MetricRun should still cause the action to raise:
            ColumnMetric[int](
                batch_id="batch_id",
                metric_name="column_values.null.count",
                value=2,
                exception=None,
                column="col2",
            ),
        ]
    )
    mock_batch_inspector.compute_metric_list_run.return_value = mock_metric_run

    action = MetricListAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    with pytest.raises(RuntimeError):
        action.run(
            event=RunMetricsEvent(
                type="metrics_request.received",
                datasource_name="test-datasource",
                data_asset_name="test-data-asset",
                metric_list=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
            ),
            id="test-id",
        )
    mock_metric_repository.add_metric_run.assert_called_once_with(mock_metric_run)
