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
    PartialSchemaChangeExpectationError,
)
from great_expectations_cloud.agent.models import GenerateSchemaChangeExpectationsEvent

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
    from great_expectations.datasource.fluent import DataAsset
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit

LOGGER = logging.getLogger(__name__)


# https://docs.pytest.org/en/7.1.x/how-to/monkeypatch.html
@pytest.fixture
def mock_response_success(monkeypatch):
    def mock_data_asset(self, event: GenerateSchemaChangeExpectationsEvent, asset_name: str):
        return TableAsset(
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_metrics(self, data_asset: DataAsset):
        return MetricRun(
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

    def mock_schema_change_expectation(self, metric_run: MetricRun, expectation_suite_name: str):
        return gx_expectations.ExpectTableColumnsToMatchSet(
            column_set=["col1", "col2"], id=str(uuid.uuid4())
        )

    monkeypatch.setattr(
        GenerateSchemaChangeExpectationsAction, "_retrieve_asset_from_asset_name", mock_data_asset
    )
    monkeypatch.setattr(GenerateSchemaChangeExpectationsAction, "_get_metrics", mock_metrics)
    monkeypatch.setattr(
        GenerateSchemaChangeExpectationsAction,
        "_add_schema_change_expectation",
        mock_schema_change_expectation,
    )


@pytest.fixture
def mock_response_failed_asset(monkeypatch):
    def mock_data_asset(self, event: GenerateSchemaChangeExpectationsEvent, asset_name: str):
        raise RuntimeError(f"Failed to retrieve asset: {asset_name}")  # noqa: TRY003 # following pattern in code

    def mock_metrics(self, data_asset: DataAsset):
        return MetricRun(
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

    def mock_schema_change_expectation(self, metric_run: MetricRun, expectation_suite_name: str):
        return gx_expectations.ExpectTableColumnsToMatchSet(
            column_set=["col1", "col2"], id=str(uuid.uuid4())
        )

    monkeypatch.setattr(
        GenerateSchemaChangeExpectationsAction, "_retrieve_asset_from_asset_name", mock_data_asset
    )
    monkeypatch.setattr(GenerateSchemaChangeExpectationsAction, "_get_metrics", mock_metrics)
    monkeypatch.setattr(
        GenerateSchemaChangeExpectationsAction,
        "_add_schema_change_expectation",
        mock_schema_change_expectation,
    )


@pytest.fixture
def mock_response_failed_metrics(monkeypatch):
    def mock_data_asset(self, event: GenerateSchemaChangeExpectationsEvent, asset_name: str):
        return TableAsset(
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_metrics(self, data_asset: DataAsset):
        raise RuntimeError("One or more metrics failed to compute.")  # noqa: TRY003 # following pattern in code

    def mock_schema_change_expectation(self, metric_run: MetricRun, expectation_suite_name: str):
        return gx_expectations.ExpectTableColumnsToMatchSet(
            column_set=["col1", "col2"], id=str(uuid.uuid4())
        )

    monkeypatch.setattr(
        GenerateSchemaChangeExpectationsAction, "_retrieve_asset_from_asset_name", mock_data_asset
    )
    monkeypatch.setattr(GenerateSchemaChangeExpectationsAction, "_get_metrics", mock_metrics)
    monkeypatch.setattr(
        GenerateSchemaChangeExpectationsAction,
        "_add_schema_change_expectation",
        mock_schema_change_expectation,
    )


@pytest.fixture
def mock_response_failed_schema_change(monkeypatch):
    def mock_data_asset(self, event: GenerateSchemaChangeExpectationsEvent, asset_name: str):
        return TableAsset(
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_metrics(self, data_asset: DataAsset):
        return MetricRun(
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

    def mock_schema_change_expectation(self, metric_run: MetricRun, expectation_suite_name: str):
        raise RuntimeError("Failed to add expectation to suite: test-suite")  # noqa: TRY003 # following pattern in code

    monkeypatch.setattr(
        GenerateSchemaChangeExpectationsAction, "_retrieve_asset_from_asset_name", mock_data_asset
    )
    monkeypatch.setattr(GenerateSchemaChangeExpectationsAction, "_get_metrics", mock_metrics)
    monkeypatch.setattr(
        GenerateSchemaChangeExpectationsAction,
        "_add_schema_change_expectation",
        mock_schema_change_expectation,
    )


@pytest.mark.parametrize(
    "data_asset_names, expected_created_resources",
    [
        (["test-data-asset1"], 2),
        (["test-data-asset1", "test-data-asset2"], 4),
    ],
)
def test_generate_schema_change_expectations_action_success(
    mock_response_success,
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    data_asset_names,
    expected_created_resources,
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
    mock_response_failed_asset, mock_context: CloudDataContext, mocker: MockerFixture, caplog
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
    mock_response_failed_metrics, mock_context: CloudDataContext, mocker: MockerFixture, caplog
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
    # run the action
    with pytest.raises(PartialSchemaChangeExpectationError) as e:
        action.run(
            event=GenerateSchemaChangeExpectationsEvent(
                type="generate_schema_change_expectations_request.received",
                organization_id=uuid.uuid4(),
                datasource_name="test-datasource",
                data_assets=["data-asset1"],
                create_expectations=True,
            ),
            id="test-id",
        )

    # These are part of the same message
    assert "Failed to generate schema change expectations for 1 of the 1 assets." in str(e.value)
    assert "Asset: data-asset1" in str(e.value)
    assert "One or more metrics failed to compute." in str(e.value)


def test_action_failure_in_add_schema_change_expectation(
    mock_response_failed_schema_change,
    mock_context: CloudDataContext,
    mocker: MockerFixture,
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

    # run the action
    with pytest.raises(PartialSchemaChangeExpectationError) as e:
        action.run(
            event=GenerateSchemaChangeExpectationsEvent(
                type="generate_schema_change_expectations_request.received",
                organization_id=uuid.uuid4(),
                datasource_name="test-datasource",
                data_assets=["data-asset1"],
                create_expectations=True,
            ),
            id="test-id",
        )

    # These are part of the same message
    assert "Failed to generate schema change expectations for 1 of the 1 assets." in str(e.value)
    assert "Failed to add expectation to suite: test-suite" in str(e.value)
    assert "Asset: data-asset1" in str(e.value)
