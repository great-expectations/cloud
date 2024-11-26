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


@pytest.fixture
def mock_metrics_list() -> list[TableMetric]:
    return [
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


# https://docs.pytest.org/en/7.1.x/how-to/monkeypatch.html
@pytest.fixture
def mock_response_success(monkeypatch, mock_metrics_list: list[TableMetric]):
    def mock_data_asset(self, event: GenerateSchemaChangeExpectationsEvent, asset_name: str):
        return TableAsset(
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_metrics(self, data_asset: DataAsset):
        return MetricRun(metrics=mock_metrics_list)

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
def mock_multi_asset_success_and_failure(monkeypatch, mock_metrics_list: list[TableMetric]):
    def mock_data_asset(self, event: GenerateSchemaChangeExpectationsEvent, asset_name: str):
        if "retrieve-fail" in asset_name:
            raise RuntimeError(f"Failed to retrieve asset: {asset_name}")  # noqa: TRY003 # following pattern in code
        else:
            return TableAsset(
                name=asset_name,
                table_name="test_table",
                schema_name="test_schema",
            )

    def mock_metrics(self, data_asset: DataAsset):
        if "metric-fail" in data_asset.name:
            raise RuntimeError("One or more metrics failed to compute.")  # noqa: TRY003 # following pattern in code
        else:
            return MetricRun(metrics=mock_metrics_list)

    def mock_schema_change_expectation(self, metric_run: MetricRun, expectation_suite_name: str):
        # The data asset name is contained in the expectation_suite_name
        # Here we are simulating a failure to add an expectation to the suite, for suite names that contain "schema-fail"
        if "schema-fail" in expectation_suite_name:
            raise RuntimeError("Failed to add expectation to suite: test-suite")  # noqa: TRY003 # following pattern in code
        else:
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


@pytest.mark.parametrize(
    "succeeding_data_asset_names, failing_data_asset_names, error_message_header",
    [
        pytest.param(
            ["test-data-asset1"],
            ["retrieve-fail-asset-1"],
            "Unable to fetch schemas for 1 of 2 Data Assets.",
            id="Single asset passing, single asset failing",
        ),
        pytest.param(
            ["test-data-asset1", "test-data-asset2"],
            ["retrieve-fail-asset-1"],
            "Unable to fetch schemas for 1 of 3 Data Assets.",
            id="Multiple assets passing, single asset failing",
        ),
        pytest.param(
            ["test-data-asset1", "test-data-asset2"],
            [
                "retrieve-fail-asset-1",
                "retrieve-fail-asset-2",
                "metric-fail-asset-1",
                "metric-fail-asset-2",
            ],
            "Unable to fetch schemas for 4 of 6 Data Assets.",
            id="Multiple assets passing, multiple assets failing",
        ),
        pytest.param(
            ["test-data-asset1", "test-data-asset2"],
            [
                "retrieve-fail-asset-1",
                "retrieve-fail-asset-2",
                "metric-fail-asset-1",
                "metric-fail-asset-2",
                "schema-fail-asset-1",
                "schema-fail-asset-2",
                "schema-fail-asset-3",
                "schema-fail-asset-4",
                "schema-fail-asset-5",
            ],
            "Unable to fetch schemas for 9 of 11 Data Assets.",
            id="Multiple assets passing, multiple assets failing, more than max display errors",
        ),
    ],
)
def test_succeeding_and_failing_assets_together(
    mock_multi_asset_success_and_failure,
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    succeeding_data_asset_names: list[str],
    failing_data_asset_names: list[str],
    error_message_header: str,
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
                data_assets=succeeding_data_asset_names + failing_data_asset_names,
                create_expectations=True,
            ),
            id="test-id",
        )

    # These are part of the same message
    assert error_message_header in str(e.value)
    error_message_footer = "Check your connection details, delete and recreate these Data Assets."
    assert error_message_footer in str(e.value)
    for asset_name in failing_data_asset_names:
        assert asset_name in str(e.value)
