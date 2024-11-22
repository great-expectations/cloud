from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

import great_expectations.expectations as gx_expectations
import pytest
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from great_expectations.experimental.metric_repository.metrics import (
    MetricRun,
    TableMetric,
)

from great_expectations_cloud.agent.actions.generate_schema_change_expectations_action import (
    GenerateSchemaChangeExpectationsAction,
)
from great_expectations_cloud.agent.models import GenerateSchemaChangeExpectationsEvent

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def data_asset() -> TableAsset:
    return TableAsset(
        name="test-data-asset",
        table_name="test_table",
        schema_name="test_schema",
    )


@pytest.fixture
def metric_run() -> MetricRun:
    mock_metric_run = MetricRun(
        metrics=[
            TableMetric(
                batch_id="batch_id",
                metric_name="table.columns",
                value=["col1", "col2"],
                exception=None,
            ),
            TableMetric(
                batch_id="batch_id",
                metric_name="table.column_types",
                value=[
                    {"name": "col1", "type": "INT"},
                    {"name": "col2", "type": "INT"},
                ],
                exception=None,
            ),
        ]
    )
    return mock_metric_run


@pytest.mark.parametrize(
    "data_asset_names, expected_created_resources",
    [
        (["test-data-asset1"], 2),
        (["test-data-asset1", "test-data-asset2"], 4),
    ],
)
def test_generate_schema_change_expectations_action_success(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    data_asset_names,
    expected_created_resources,
    data_asset,
    metric_run,
):
    # setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateSchemaChangeExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # mock the methods
    action._retrieve_asset_from_asset_name = mocker.Mock()  # type: ignore[method-assign]
    action._retrieve_asset_from_asset_name.return_value = data_asset

    action._get_metrics = mocker.Mock()  # type: ignore[method-assign]
    action._get_metrics.return_value = (metric_run, uuid.uuid4())

    action._add_schema_change_expectation = mocker.Mock()  # type: ignore[method-assign]
    expectation = gx_expectations.ExpectTableColumnsToMatchSet(
        column_set=metric_run.metrics[0].value
    )
    expectation.id = str(uuid.uuid4())
    action._add_schema_change_expectation.return_value = expectation

    # run the action
    return_value = action.run(
        event=GenerateSchemaChangeExpectationsEvent(
            type="generate_schema_change_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=data_asset_names,
            create_expectations=True,
        ),
        id="test-id",
    )

    # assert
    assert len(return_value.created_resources) == expected_created_resources
    assert return_value.type == "generate_schema_change_expectations_request.received"


def test_action_failure_in_retrieve_asset_from_asset_name(
    mock_context: CloudDataContext, mocker: MockerFixture, data_asset, metric_run, caplog
):
    # setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateSchemaChangeExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # mock the methods
    action._retrieve_asset_from_asset_name = mocker.Mock()  # type: ignore[method-assign]
    action._retrieve_asset_from_asset_name.side_effect = RuntimeError(
        "Failed to retrieve asset: data-asset1"
    )

    action._get_metrics = mocker.Mock()  # type: ignore[method-assign]
    action._get_metrics.return_value = (metric_run, uuid.uuid4())

    action._add_schema_change_expectation = mocker.Mock()  # type: ignore[method-assign]
    expectation = gx_expectations.ExpectTableColumnsToMatchSet(
        column_set=metric_run.metrics[0].value
    )
    expectation.id = str(uuid.uuid4())
    action._add_schema_change_expectation.return_value = expectation

    # run the action
    return_value = action.run(
        event=GenerateSchemaChangeExpectationsEvent(
            type="generate_schema_change_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["data-asset1"],
            create_expectations=True,
        ),
        id="test-id",
    )

    assert len(return_value.created_resources) == 0
    assert return_value.type == "generate_schema_change_expectations_request.received"
    # both of these are part of the same message
    assert "asset_name: data-asset1 failed with error" in caplog.text
    assert "Failed to retrieve asset: data-asset1" in caplog.text


def test_action_failure_in_get_metrics(
    mock_context: CloudDataContext, mocker: MockerFixture, data_asset, metric_run, caplog
):
    # setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateSchemaChangeExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # mock the methods
    action._retrieve_asset_from_asset_name = mocker.Mock()  # type: ignore[method-assign]
    action._retrieve_asset_from_asset_name.return_value = data_asset

    action._get_metrics = mocker.Mock()  # type: ignore[method-assign]
    action._get_metrics.side_effect = RuntimeError("One or more metrics failed to compute.")

    action._add_schema_change_expectation = mocker.Mock()  # type: ignore[method-assign]
    expectation = gx_expectations.ExpectTableColumnsToMatchSet(
        column_set=metric_run.metrics[0].value
    )
    expectation.id = str(uuid.uuid4())
    action._add_schema_change_expectation.return_value = expectation

    # run the action
    return_value = action.run(
        event=GenerateSchemaChangeExpectationsEvent(
            type="generate_schema_change_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["data-asset1"],
            create_expectations=True,
        ),
        id="test-id",
    )

    assert len(return_value.created_resources) == 0
    assert return_value.type == "generate_schema_change_expectations_request.received"
    # both of these are part of the same message
    assert "asset_name: data-asset1 failed with error" in caplog.text
    assert "One or more metrics failed to compute." in caplog.text


def test_action_failure_in_add_schema_change_expectation(
    mock_context: CloudDataContext, mocker: MockerFixture, data_asset, metric_run, caplog
):
    # setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateSchemaChangeExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # mock the methods
    action._retrieve_asset_from_asset_name = mocker.Mock()  # type: ignore[method-assign]
    action._retrieve_asset_from_asset_name.return_value = data_asset

    action._get_metrics = mocker.Mock()  # type: ignore[method-assign]
    action._get_metrics.return_value = (metric_run, uuid.uuid4())

    action._add_schema_change_expectation = mocker.Mock()  # type: ignore[method-assign]
    gx_expectations.ExpectTableColumnsToMatchSet(column_set=metric_run.metrics[0].value)
    action._add_schema_change_expectation.side_effect = RuntimeError(
        "Failed to add expectation to suite: test-suite"
    )

    # run the action
    return_value = action.run(
        event=GenerateSchemaChangeExpectationsEvent(
            type="generate_schema_change_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["data-asset1"],
            create_expectations=True,
        ),
        id="test-id",
    )

    assert len(return_value.created_resources) == 0
    assert return_value.type == "generate_schema_change_expectations_request.received"
    # both of these are part of the same message
    assert "asset_name: data-asset1 failed with error" in caplog.text
    assert "Failed to add expectation to suite" in caplog.text
