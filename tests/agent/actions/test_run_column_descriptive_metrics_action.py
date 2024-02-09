from __future__ import annotations

import uuid
from unittest.mock import Mock

import pytest
from great_expectations.data_context import CloudDataContext
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
)

from great_expectations_cloud.agent.actions import ColumnDescriptiveMetricsAction
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunColumnDescriptiveMetricsEvent,
)

pytestmark = pytest.mark.unit


def test_run_column_descriptive_metrics_computes_metric_run():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_repository = Mock(spec=MetricRepository)
    mock_batch_inspector = Mock(spec=BatchInspector)

    action = ColumnDescriptiveMetricsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    action._raise_on_any_metric_exception = Mock()

    action.run(
        event=RunColumnDescriptiveMetricsEvent(
            type="column_descriptive_metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
        ),
        id="test-id",
    )

    mock_batch_inspector.compute_metric_run.assert_called_once()


def test_run_column_descriptive_metrics_creates_metric_run():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_repository = Mock(spec=MetricRepository)
    mock_batch_inspector = Mock(spec=BatchInspector)

    mock_metric_run = Mock(spec=MetricRun)
    mock_batch_inspector.compute_metric_run.return_value = mock_metric_run

    action = ColumnDescriptiveMetricsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    action._raise_on_any_metric_exception = Mock()

    action.run(
        event=RunColumnDescriptiveMetricsEvent(
            type="column_descriptive_metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
        ),
        id="test-id",
    )

    mock_metric_repository.add_metric_run.assert_called_once_with(mock_metric_run)


def test_run_column_descriptive_metrics_returns_action_result():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_repository = Mock(spec=MetricRepository)
    mock_batch_inspector = Mock(spec=BatchInspector)

    metric_run_id = uuid.uuid4()
    mock_metric_repository.add_metric_run.return_value = metric_run_id

    action = ColumnDescriptiveMetricsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )
    action._raise_on_any_metric_exception = Mock()

    action_result = action.run(
        event=RunColumnDescriptiveMetricsEvent(
            type="column_descriptive_metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
        ),
        id="test-id",
    )

    assert action_result.type == "column_descriptive_metrics_request.received"
    assert action_result.id == "test-id"
    assert action_result.created_resources == [
        CreatedResource(resource_id=str(metric_run_id), type="MetricRun"),
    ]


def test_run_column_descriptive_metrics_raises_on_test_connection_failure():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_repository = Mock(spec=MetricRepository)
    mock_batch_inspector = Mock(spec=BatchInspector)

    mock_datasource = Mock()
    mock_context.get_datasource.return_value = mock_datasource
    mock_datasource.test_connection.side_effect = TestConnectionError()

    action = ColumnDescriptiveMetricsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    with pytest.raises(TestConnectionError):
        action.run(
            event=RunColumnDescriptiveMetricsEvent(
                type="column_descriptive_metrics_request.received",
                datasource_name="test-datasource",
                data_asset_name="test-data-asset",
            ),
            id="test-id",
        )

    mock_batch_inspector.compute_metric_run.assert_not_called()


def test_run_column_descriptive_metrics_creates_metric_run_then_raises_on_any_metric_exception():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_repository = Mock(spec=MetricRepository)
    mock_batch_inspector = Mock(spec=BatchInspector)

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
    mock_batch_inspector.compute_metric_run.return_value = mock_metric_run

    action = ColumnDescriptiveMetricsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    with pytest.raises(RuntimeError):
        action.run(
            event=RunColumnDescriptiveMetricsEvent(
                type="column_descriptive_metrics_request.received",
                datasource_name="test-datasource",
                data_asset_name="test-data-asset",
            ),
            id="test-id",
        )

    mock_metric_repository.add_metric_run.assert_called_once_with(mock_metric_run)
