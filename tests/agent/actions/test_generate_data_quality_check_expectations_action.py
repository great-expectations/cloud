from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable

import great_expectations.expectations as gx_expectations
import pytest
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from great_expectations.expectations.metadata_types import DataQualityIssues
from great_expectations.expectations.window import Offset
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    Metric,
    MetricRun,
    MetricTypes,
    TableMetric,
)

from great_expectations_cloud.agent.actions.generate_data_quality_check_expectations_action import (
    GenerateDataQualityCheckExpectationsAction,
    PartialGenerateDataQualityCheckExpectationError,
)
from great_expectations_cloud.agent.models import GenerateDataQualityCheckExpectationsEvent

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
    from great_expectations.datasource.fluent import DataAsset
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def mock_metrics_list() -> list[TableMetric[list[str | dict[str, str]]]]:
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


TABLE_ASSET_ID = uuid.uuid4()


# https://docs.pytest.org/en/7.1.x/how-to/monkeypatch.html
@pytest.fixture
def mock_response_success_no_pre_existing_anomaly_detection_coverage(
    monkeypatch, mock_metrics_list: list[TableMetric[list[str | dict[str, str]]]]
):
    def mock_data_asset(self, event: GenerateDataQualityCheckExpectationsEvent, asset_name: str):
        return TableAsset(
            id=TABLE_ASSET_ID,
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_metrics(self, data_asset: DataAsset[Any, Any]):
        return MetricRun(metrics=mock_metrics_list), uuid.uuid4()

    def mock_no_anomaly_detection_coverage(self, data_asset: DataAsset[Any, Any]):
        return {}

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_retrieve_asset_from_asset_name",
        mock_data_asset,
    )
    monkeypatch.setattr(GenerateDataQualityCheckExpectationsAction, "_get_metrics", mock_metrics)
    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_no_anomaly_detection_coverage,
    )


@pytest.fixture
def mock_response_success_pre_existing_volume_anomaly_detection_coverage(
    monkeypatch, mock_metrics_list: list[TableMetric[list[str | dict[str, str]]]]
):
    def mock_data_asset(self, event: GenerateDataQualityCheckExpectationsEvent, asset_name: str):
        return TableAsset(
            id=TABLE_ASSET_ID,
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_metrics(self, data_asset: DataAsset[Any, Any]):
        return MetricRun(metrics=mock_metrics_list), uuid.uuid4()

    def mock_pre_existing_volume_anomaly_detection_coverage(self, data_asset: DataAsset[Any, Any]):
        return {DataQualityIssues.VOLUME: ["only need key to exist"]}

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_retrieve_asset_from_asset_name",
        mock_data_asset,
    )
    monkeypatch.setattr(GenerateDataQualityCheckExpectationsAction, "_get_metrics", mock_metrics)
    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_pre_existing_volume_anomaly_detection_coverage,
    )


@pytest.fixture
def mock_response_success_pre_existing_completeness_anomaly_detection_coverage(
    monkeypatch, mock_metrics_list: list[TableMetric[list[str | dict[str, str]]]]
):
    def mock_data_asset(self, event: GenerateDataQualityCheckExpectationsEvent, asset_name: str):
        return TableAsset(
            id=TABLE_ASSET_ID,
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_metrics(self, data_asset: DataAsset[Any, Any]):
        return MetricRun(metrics=mock_metrics_list), uuid.uuid4()

    def mock_pre_existing_completeness_anomaly_detection_coverage(
        self, data_asset: DataAsset[Any, Any]
    ):
        return {
            DataQualityIssues.COMPLETENESS: [
                {
                    "config": {
                        "kwargs": {
                            "column": "col1",
                        }
                    }
                }
            ]
        }  # only mocking necessary fields

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_retrieve_asset_from_asset_name",
        mock_data_asset,
    )
    monkeypatch.setattr(GenerateDataQualityCheckExpectationsAction, "_get_metrics", mock_metrics)
    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_pre_existing_completeness_anomaly_detection_coverage,
    )


@pytest.fixture
def mock_multi_asset_success_and_failure(
    monkeypatch, mock_metrics_list: list[TableMetric[list[str | dict[str, str]]]]
):
    failing_asset_id = uuid.UUID("00000000-0000-0000-0000-000000000001")

    def mock_data_asset(self, event: GenerateDataQualityCheckExpectationsEvent, asset_name: str):
        if "retrieve-fail" in asset_name:
            raise RuntimeError(f"Failed to retrieve asset: {asset_name}")  # noqa: TRY003 # following pattern in code
        elif "schema-fail" in asset_name:
            return TableAsset(
                id=failing_asset_id,
                name=asset_name,
                table_name="test_table",
                schema_name="test_schema",
            )
        else:
            return TableAsset(
                name=asset_name,
                table_name="test_table",
                schema_name="test_schema",
            )

    def mock_metrics(self, data_asset: DataAsset[Any, Any]):
        if "metric-fail" in data_asset.name:
            raise RuntimeError("One or more metrics failed to compute.")  # noqa: TRY003 # following pattern in code
        else:
            return MetricRun(metrics=mock_metrics_list)

    def mock_no_anomaly_detection_coverage(self, data_asset: DataAsset[Any, Any]):
        return {}

    def mock_schema_change_expectation(
        self, metric_run: MetricRun, asset_id: uuid.UUID, created_via: str
    ):
        # The data asset name is contained in the expectation_suite_name
        # Here we are simulating a failure to add an expectation to the suite, for suite names that contain "schema-fail"
        if asset_id == failing_asset_id:
            error_msg = "Failed to add autogenerated expectation: expect_table_columns_to_match_set"
            raise RuntimeError(error_msg)
        else:
            return uuid.uuid4()

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_retrieve_asset_from_asset_name",
        mock_data_asset,
    )
    monkeypatch.setattr(GenerateDataQualityCheckExpectationsAction, "_get_metrics", mock_metrics)
    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_no_anomaly_detection_coverage,
    )
    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_add_schema_change_expectation",
        mock_schema_change_expectation,
    )


@pytest.fixture
def mock_metric_repository(mocker):
    return mocker.Mock(spec=MetricRepository)


@pytest.fixture
def mock_batch_inspector(mocker):
    return mocker.Mock(spec=BatchInspector)


MockCompletenessMetrics = Callable[[int, int], list[Metric[Any]]]


@pytest.fixture
def mock_completeness_metrics() -> MockCompletenessMetrics:
    def _completeness_metrics(null_count: int, row_count: int) -> list[Metric[Any]]:
        return [
            TableMetric(
                batch_id="batch_id",
                metric_name=MetricTypes.TABLE_COLUMNS,
                value=["col1", "col2"],
                exception=None,
            ),
            TableMetric(
                batch_id="batch_id",
                metric_name=MetricTypes.TABLE_ROW_COUNT,
                value=row_count,
                exception=None,
            ),
            ColumnMetric(
                batch_id="batch_id",
                metric_name=MetricTypes.COLUMN_NULL_COUNT,
                value=null_count,
                exception=None,
                column="col1",
            ),
        ]

    return _completeness_metrics


@pytest.mark.parametrize(
    "data_asset_names, expected_created_resources",
    [
        (["test-data-asset1"], 2),
        (
            ["test-data-asset1", "test-data-asset2"],
            4,
        ),
    ],
)
def test_generate_schema_change_expectations_action_success(
    mock_response_success_no_pre_existing_anomaly_detection_coverage,
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    data_asset_names,
    expected_created_resources,
):
    # setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # run the action
    mocker.patch(
        f"{GenerateDataQualityCheckExpectationsAction.__module__}.{GenerateDataQualityCheckExpectationsAction.__name__}._create_expectation_for_asset",
        return_value=uuid.uuid4(),
    )
    mock_create_expectation_for_asset = mocker.spy(
        GenerateDataQualityCheckExpectationsAction, "_create_expectation_for_asset"
    )
    return_value = action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=data_asset_names,
            selected_data_quality_issues=[DataQualityIssues.SCHEMA],
            created_via="asset_creation",
        ),
        id="test-id",
    )

    # assert
    assert len(return_value.created_resources) == expected_created_resources
    assert return_value.type == "generate_data_quality_check_expectations_request.received"
    mock_create_expectation_for_asset.assert_called()
    mock_create_expectation_for_asset.assert_called_with(
        expectation=gx_expectations.ExpectTableColumnsToMatchSet(
            column_set=["col1", "col2"],
        ),
        asset_id=TABLE_ASSET_ID,
        created_via="asset_creation",
    )


def test_anomaly_detection_expectation_not_created_if_asset_already_has_coverage(
    mock_response_success_pre_existing_volume_anomaly_detection_coverage,
    mock_context: CloudDataContext,
    mocker: MockerFixture,
):
    """
    If the asset already has volume change anomaly detection coverage, but both volume and schema data quality
    issues have been selected, we should skip creating volume expectations, and only create one for schema.
    """
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # run the action
    mocker.patch(
        f"{GenerateDataQualityCheckExpectationsAction.__module__}.{GenerateDataQualityCheckExpectationsAction.__name__}._create_expectation_for_asset",
        return_value=uuid.uuid4(),
    )
    mock_create_expectation_for_asset = mocker.spy(
        GenerateDataQualityCheckExpectationsAction, "_create_expectation_for_asset"
    )
    return_value = action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["data_asset_name"],
            selected_data_quality_issues=[DataQualityIssues.SCHEMA, DataQualityIssues.VOLUME],
            created_via="new_expectation",
        ),
        id="test-id",
    )
    mock_create_expectation_for_asset.assert_called_once()
    mock_create_expectation_for_asset.assert_called_with(
        expectation=gx_expectations.ExpectTableColumnsToMatchSet(
            column_set=["col1", "col2"],
        ),
        asset_id=TABLE_ASSET_ID,
        created_via="new_expectation",
    )
    assert return_value.type == "generate_data_quality_check_expectations_request.received"


def test_generate_completeness_expectation_not_added_when_coverage_already_exists(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    mock_metric_repository: MetricRepository,
    mock_batch_inspector: BatchInspector,
    mock_completeness_metrics: MockCompletenessMetrics,
):
    """Test that when completeness coverage exists, a new Expectation is not created for covered columns."""
    # Setup
    metrics = mock_completeness_metrics(30, 100)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # Mock the methods that would be called
    def mock_retrieve_asset(event, asset_name):
        return TableAsset(
            id=TABLE_ASSET_ID,
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_get_metrics(data_asset):
        return MetricRun(metrics=metrics), uuid.uuid4()

    def mock_get_coverage(data_asset_id):
        return {DataQualityIssues.COMPLETENESS: [{"config": {"kwargs": {"column": "col1"}}}]}

    mocker.patch.object(action, "_retrieve_asset_from_asset_name", side_effect=mock_retrieve_asset)
    mocker.patch.object(action, "_get_metrics", side_effect=mock_get_metrics)
    mocker.patch.object(
        action, "_get_current_anomaly_detection_coverage", side_effect=mock_get_coverage
    )

    # Mock the _create_expectation_for_asset method to capture created expectations
    created_expectations = []

    def mock_create_expectation(expectation, asset_id, created_via):
        created_expectations.append(expectation)
        return uuid.uuid4()

    mocker.patch.object(
        action, "_create_expectation_for_asset", side_effect=mock_create_expectation
    )

    # Run the action with expect_non_null_proportion_enabled=True
    return_value = action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["test-data-asset1"],
            selected_data_quality_issues=[DataQualityIssues.COMPLETENESS],
        ),
        id="test-id",
    )

    # Assert
    assert (
        len(return_value.created_resources) == 1
    )  # MetricRun + 0 Expectations (since coverage exists)
    assert return_value.created_resources[0].type == "MetricRun"
    assert len(created_expectations) == 0


@pytest.mark.parametrize(
    "succeeding_data_asset_names, failing_data_asset_names, error_message_header",
    [
        pytest.param(
            ["test-data-asset1"],
            ["retrieve-fail-asset-1"],
            "Unable to autogenerate expectations for 1 of 2 Data Assets.",
            id="Single asset passing, single asset failing",
        ),
        pytest.param(
            ["test-data-asset1", "test-data-asset2"],
            ["retrieve-fail-asset-1"],
            "Unable to autogenerate expectations for 1 of 3 Data Assets.",
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
            "Unable to autogenerate expectations for 4 of 6 Data Assets.",
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
            "Unable to autogenerate expectations for 9 of 11 Data Assets.",
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

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # run the action
    with pytest.raises(PartialGenerateDataQualityCheckExpectationError) as e:
        action.run(
            event=GenerateDataQualityCheckExpectationsEvent(
                type="generate_data_quality_check_expectations_request.received",
                organization_id=uuid.uuid4(),
                datasource_name="test-datasource",
                data_assets=succeeding_data_asset_names + failing_data_asset_names,
                selected_data_quality_issues=[DataQualityIssues.SCHEMA],
            ),
            id="test-id",
        )

    # These are part of the same message
    assert error_message_header in str(e.value)
    error_message_footer = "Check your connection details, delete and recreate these Data Assets."
    assert error_message_footer in str(e.value)
    for asset_name in failing_data_asset_names:
        assert asset_name in str(e.value)


def test_missing_table_columns_metric_raises_runtime_error(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
):
    # Setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    # Create a mock data asset
    mock_data_asset = mocker.Mock()
    mock_data_asset.id = uuid.uuid4()
    mock_data_asset.test_connection.return_value = None

    # Create a metric run without TABLE_COLUMNS metric
    mock_metric_run = mocker.Mock(spec=MetricRun)
    mock_metric_run.metrics = [
        # No TABLE_COLUMNS metric in this list
        TableMetric(
            batch_id="batch_id",
            metric_name=MetricTypes.TABLE_ROW_COUNT,
            value=100,
            exception=None,
        )
    ]

    # Configure the batch inspector to return our mock metric run
    mock_batch_inspector.compute_metric_list_run.return_value = mock_metric_run
    mock_metric_repository.add_metric_run.return_value = uuid.uuid4()

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # Create the event
    event = GenerateDataQualityCheckExpectationsEvent(
        type="generate_data_quality_check_expectations_request.received",
        organization_id=uuid.uuid4(),
        datasource_name="test-datasource",
        data_assets=["test-asset"],
        selected_data_quality_issues=[DataQualityIssues.SCHEMA],
    )

    # Act & Assert
    with pytest.raises(PartialGenerateDataQualityCheckExpectationError):
        action.run(event=event, id="test-id")


@pytest.mark.parametrize(
    "data_asset_names, expected_created_resources",
    [
        (["test-data-asset1"], 2),
        (
            ["test-data-asset1", "test-data-asset2"],
            4,
        ),
    ],
)
def test_generate_volume_change_forecast_expectations_action_success(
    mock_response_success_no_pre_existing_anomaly_detection_coverage,
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    data_asset_names,
    expected_created_resources,
):
    # setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # run the action
    mocker.patch(
        f"{GenerateDataQualityCheckExpectationsAction.__module__}.{GenerateDataQualityCheckExpectationsAction.__name__}._create_expectation_for_asset",
        return_value=uuid.uuid4(),
    )
    mock_create_expectation_for_asset = mocker.spy(
        GenerateDataQualityCheckExpectationsAction, "_create_expectation_for_asset"
    )
    return_value = action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=data_asset_names,
            selected_data_quality_issues=[
                DataQualityIssues.VOLUME
            ],  # <--- Only supports volume for now
            use_forecast=True,  # <--- feature flag
        ),
        id="test-id",
    )

    # assert
    call = mock_create_expectation_for_asset.call_args
    expectation = call.kwargs["expectation"]
    mock_create_expectation_for_asset.assert_called()
    assert len(return_value.created_resources) == expected_created_resources
    assert return_value.type == "generate_data_quality_check_expectations_request.received"
    assert isinstance(expectation, gx_expectations.ExpectTableRowCountToBeBetween)
    assert isinstance(expectation.windows, list)
    assert len(expectation.windows) == 2
    assert expectation.windows[0].constraint_fn == "forecast"
    assert isinstance(expectation.windows[0].parameter_name, str)
    assert expectation.windows[0].range == 1
    assert expectation.windows[0].offset == Offset(positive=0.0, negative=0.0)
    assert expectation.windows[0].strict is True
    assert expectation.windows[1].constraint_fn == "forecast"
    assert isinstance(expectation.windows[1].parameter_name, str)
    assert expectation.windows[1].range == 1
    assert expectation.windows[1].offset == Offset(positive=0.0, negative=0.0)
    assert expectation.windows[1].strict is True
    assert expectation.strict_min is False
    assert expectation.strict_max is False
    assert isinstance(expectation.min_value, dict)
    assert isinstance(expectation.max_value, dict)
    assert "$PARAMETER" in expectation.min_value
    assert "$PARAMETER" in expectation.max_value
    assert isinstance(expectation.min_value["$PARAMETER"], str)
    assert isinstance(expectation.max_value["$PARAMETER"], str)
    assert call.kwargs["asset_id"] == TABLE_ASSET_ID


def test_generate_completeness_expectations_with_proportion_approach(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    mock_metric_repository: MetricRepository,
    mock_batch_inspector: BatchInspector,
    mock_completeness_metrics: MockCompletenessMetrics,
):
    """Test that a single ExpectColumnProportionOfNonNullValuesToBeBetween expectation is created for completeness."""
    # Setup
    metrics = mock_completeness_metrics(30, 100)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # Mock the methods that would be called
    def mock_retrieve_asset(event, asset_name):
        return TableAsset(
            id=TABLE_ASSET_ID,
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_get_metrics(data_asset):
        return MetricRun(metrics=metrics), uuid.uuid4()

    def mock_get_coverage(data_asset_id):
        return {}

    mocker.patch.object(action, "_retrieve_asset_from_asset_name", side_effect=mock_retrieve_asset)
    mocker.patch.object(action, "_get_metrics", side_effect=mock_get_metrics)
    mocker.patch.object(
        action, "_get_current_anomaly_detection_coverage", side_effect=mock_get_coverage
    )

    # Mock the _create_expectation_for_asset method to capture created expectations
    created_expectations = []

    def mock_create_expectation(expectation, asset_id, created_via):
        created_expectations.append(expectation)
        return uuid.uuid4()

    mocker.patch.object(
        action, "_create_expectation_for_asset", side_effect=mock_create_expectation
    )

    # Run the action
    return_value = action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["test-data-asset1"],
            selected_data_quality_issues=[DataQualityIssues.COMPLETENESS],
        ),
        id="test-id",
    )

    # Assert
    assert len(return_value.created_resources) == 2  # MetricRun + 1 Expectation
    assert return_value.created_resources[0].type == "MetricRun"
    assert return_value.created_resources[1].type == "Expectation"

    # Verify only one expectation was created and it's the correct type
    assert len(created_expectations) == 1
    expectation = created_expectations[0]
    assert isinstance(expectation, gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween)
    assert expectation.column

    # For mixed nulls (30/100), we expect 2 windows with min and max values
    assert expectation.windows is not None
    assert len(expectation.windows) == 2  # min and max windows
    assert isinstance(expectation.min_value, dict) and "$PARAMETER" in expectation.min_value
    assert isinstance(expectation.max_value, dict) and "$PARAMETER" in expectation.max_value


def test_generate_completeness_forecast_expectations_action_success(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    mock_metric_repository: MetricRepository,
    mock_batch_inspector: BatchInspector,
    mock_completeness_metrics: MockCompletenessMetrics,
):
    """Test that a single ExpectColumnProportionOfNonNullValuesToBeBetween expectation is created for completeness with forecast."""
    # Setup
    metrics = mock_completeness_metrics(30, 100)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # Mock the methods that would be called
    def mock_retrieve_asset(event, asset_name):
        return TableAsset(
            id=TABLE_ASSET_ID,
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_get_metrics(data_asset):
        return MetricRun(metrics=metrics), uuid.uuid4()

    def mock_get_coverage(data_asset_id):
        return {}

    mocker.patch.object(action, "_retrieve_asset_from_asset_name", side_effect=mock_retrieve_asset)
    mocker.patch.object(action, "_get_metrics", side_effect=mock_get_metrics)
    mocker.patch.object(
        action, "_get_current_anomaly_detection_coverage", side_effect=mock_get_coverage
    )

    # Mock the _create_expectation_for_asset method to capture created expectations
    created_expectations = []

    def mock_create_expectation(expectation, asset_id, created_via):
        created_expectations.append(expectation)
        return uuid.uuid4()

    mocker.patch.object(
        action, "_create_expectation_for_asset", side_effect=mock_create_expectation
    )

    # Run the action
    return_value = action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["test-data-asset1"],
            selected_data_quality_issues=[DataQualityIssues.COMPLETENESS],
            use_forecast=True,  # <--- feature flag
        ),
        id="test-id",
    )

    # Assert
    assert len(return_value.created_resources) == 2  # MetricRun + 1 Expectation
    assert return_value.created_resources[0].type == "MetricRun"
    assert return_value.created_resources[1].type == "Expectation"

    # Verify only one expectation was created and it's the correct type
    assert len(created_expectations) == 1
    expectation = created_expectations[0]
    assert isinstance(expectation, gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween)
    assert expectation.column

    # For mixed nulls (30/100), we expect 2 windows with min and max values
    assert expectation.windows is not None
    assert len(expectation.windows) == 2  # min and max windows
    assert isinstance(expectation.min_value, dict) and "$PARAMETER" in expectation.min_value
    assert isinstance(expectation.max_value, dict) and "$PARAMETER" in expectation.max_value
    assert expectation.windows[0].constraint_fn == "forecast"
    assert expectation.windows[1].constraint_fn == "forecast"


@pytest.mark.parametrize(
    "null_count, row_count, expected_expectation_count",
    [
        pytest.param(0, 100, 1, id="no_nulls_one_expectation"),
        pytest.param(100, 100, 1, id="all_nulls_one_expectation"),
        pytest.param(30, 100, 1, id="mixed_nulls_one_expectation"),
    ],
)
def test_completeness_expectations_count_based_on_data(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    null_count: int,
    row_count: int,
    expected_expectation_count: int,
    mock_metric_repository: MetricRepository,
    mock_batch_inspector: BatchInspector,
    mock_completeness_metrics: MockCompletenessMetrics,
):
    # Setup
    metrics = mock_completeness_metrics(null_count, row_count)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # Mock the methods that would be called
    def mock_retrieve_asset(event, asset_name):
        return TableAsset(
            id=TABLE_ASSET_ID,
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_get_metrics(data_asset):
        return MetricRun(metrics=metrics), uuid.uuid4()

    def mock_get_coverage(data_asset_id):
        return {}

    mocker.patch.object(action, "_retrieve_asset_from_asset_name", side_effect=mock_retrieve_asset)
    mocker.patch.object(action, "_get_metrics", side_effect=mock_get_metrics)
    mocker.patch.object(
        action, "_get_current_anomaly_detection_coverage", side_effect=mock_get_coverage
    )

    # Mock the _create_expectation_for_asset method to capture created expectations
    created_expectations = []

    def mock_create_expectation(expectation, asset_id, created_via):
        created_expectations.append(expectation)
        return uuid.uuid4()

    mocker.patch.object(
        action, "_create_expectation_for_asset", side_effect=mock_create_expectation
    )

    # Run the action
    action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["test-data-asset1"],
            selected_data_quality_issues=[DataQualityIssues.COMPLETENESS],
        ),
        id="test-id",
    )

    # Assert expectation count
    assert len(created_expectations) == expected_expectation_count


def test_generate_completeness_expectations_edge_cases(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    mock_metric_repository: MetricRepository,
    mock_batch_inspector: BatchInspector,
    mock_completeness_metrics: MockCompletenessMetrics,
):
    """Test edge cases where non_null_proportion is exactly 0 or 1."""
    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    def mock_retrieve_asset(event, asset_name):
        return TableAsset(
            id=TABLE_ASSET_ID,
            name="test-data-asset",
            table_name="test_table",
            schema_name="test_schema",
        )

    def mock_get_coverage(data_asset_id):
        return {}

    mocker.patch.object(action, "_retrieve_asset_from_asset_name", side_effect=mock_retrieve_asset)
    mocker.patch.object(
        action, "_get_current_anomaly_detection_coverage", side_effect=mock_get_coverage
    )

    # All nulls (non_null_proportion = 0)
    created_expectations = []

    def mock_create_expectation(expectation, asset_id, created_via):
        created_expectations.append(expectation)
        return uuid.uuid4()

    def mock_get_metrics_all_nulls(data_asset):
        metrics = mock_completeness_metrics(100, 100)  # 100 nulls out of 100 rows
        return MetricRun(metrics=metrics), uuid.uuid4()

    mocker.patch.object(action, "_get_metrics", side_effect=mock_get_metrics_all_nulls)
    mocker.patch.object(
        action, "_create_expectation_for_asset", side_effect=mock_create_expectation
    )

    action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["test-data-asset1"],
            selected_data_quality_issues=[DataQualityIssues.COMPLETENESS],
        ),
        id="test-id",
    )

    # Verify expectation for all nulls case
    assert len(created_expectations) == 1
    expectation = created_expectations[0]
    assert isinstance(expectation, gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween)
    assert expectation.column
    assert expectation.max_value == 0
    assert expectation.min_value is None
    assert expectation.windows is None

    # No nulls (non_null_proportion = 1)
    created_expectations.clear()

    def mock_get_metrics_no_nulls(data_asset):
        metrics = mock_completeness_metrics(0, 100)  # 0 nulls out of 100 rows
        return MetricRun(metrics=metrics), uuid.uuid4()

    mocker.patch.object(action, "_get_metrics", side_effect=mock_get_metrics_no_nulls)

    action.run(
        event=GenerateDataQualityCheckExpectationsEvent(
            type="generate_data_quality_check_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["test-data-asset1"],
            selected_data_quality_issues=[DataQualityIssues.COMPLETENESS],
        ),
        id="test-id",
    )

    # Verify expectation for no nulls case
    assert len(created_expectations) == 1
    expectation = created_expectations[0]
    assert isinstance(expectation, gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween)
    assert expectation.column
    assert expectation.min_value == 1
    assert expectation.max_value is None
    assert expectation.windows is None


def test_verify_contents_of_http_payload(
    mock_response_success_no_pre_existing_anomaly_detection_coverage,
    mock_context: CloudDataContext,
    mocker: MockerFixture,
):
    """Test that the contents of the HTTP payload are correct."""
    # setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    # Create a test expectation
    expectation = gx_expectations.ExpectTableColumnsToMatchSet(
        column_set=["col1", "col2"],
    )

    # Mock the HTTP response
    mock_response = mocker.MagicMock()
    mock_response.status_code = 201  # Use integer, not enum
    mock_response.json.return_value = {"data": {"id": str(uuid.uuid4())}}

    # Mock the session
    mock_session = mocker.MagicMock()
    mock_session.post.return_value = mock_response
    mock_session.__enter__.return_value = mock_session
    mock_session.__exit__.return_value = None

    # Mock the create_session function
    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_data_quality_check_expectations_action.create_session",
        return_value=mock_session,
    )

    # Call the method
    action._create_expectation_for_asset(
        expectation=expectation,
        asset_id=TABLE_ASSET_ID,
        created_via="asset_creation",
    )

    # Verify that the session.post was called
    mock_session.post.assert_called_once()

    # Get the call arguments
    call_args = mock_session.post.call_args

    # Verify the JSON payload contains the expected fields
    json_payload = call_args.kwargs["json"]
    assert json_payload["severity"] == "warning"
    assert json_payload["expectation_type"] == "expect_table_columns_to_match_set"
    assert json_payload["autogenerated"] is True
    assert json_payload["created_via"] == "asset_creation"
    assert "kwargs" in json_payload
    assert json_payload["kwargs"]["column_set"] == ["col1", "col2"]


if __name__ == "__main__":
    print(GenerateDataQualityCheckExpectationsEvent.__module__)
