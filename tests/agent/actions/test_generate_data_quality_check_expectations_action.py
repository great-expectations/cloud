from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable

import great_expectations.expectations as gx_expectations
import pytest
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from great_expectations.expectations.metadata_types import (
    DataQualityIssues,
    FailureSeverity,
)
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
    _strip_bracketed_error_code,
)
from great_expectations_cloud.agent.models import (
    DomainContext,
    GenerateDataQualityCheckExpectationsEvent,
)

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
        non_null_count = row_count - null_count
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
                metric_name=MetricTypes.COLUMN_NON_NULL_COUNT,
                value=non_null_count,
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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
            workspace_id=uuid.uuid4(),
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
            severity=FailureSeverity.WARNING,
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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
            workspace_id=uuid.uuid4(),
        ),
        id="test-id",
    )
    mock_create_expectation_for_asset.assert_called_once()
    mock_create_expectation_for_asset.assert_called_with(
        expectation=gx_expectations.ExpectTableColumnsToMatchSet(
            column_set=["col1", "col2"],
            severity=FailureSeverity.WARNING,
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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
            workspace_id=uuid.uuid4(),
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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
                workspace_id=uuid.uuid4(),
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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
    )

    # Create the event
    event = GenerateDataQualityCheckExpectationsEvent(
        type="generate_data_quality_check_expectations_request.received",
        organization_id=uuid.uuid4(),
        datasource_name="test-datasource",
        data_assets=["test-asset"],
        selected_data_quality_issues=[DataQualityIssues.SCHEMA],
        workspace_id=uuid.uuid4(),
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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
            workspace_id=uuid.uuid4(),
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
    assert expectation.severity == FailureSeverity.WARNING
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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
            workspace_id=uuid.uuid4(),
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
    assert expectation.severity == FailureSeverity.WARNING

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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
            workspace_id=uuid.uuid4(),
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
    assert expectation.severity == FailureSeverity.WARNING

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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
            workspace_id=uuid.uuid4(),
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
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
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
            workspace_id=uuid.uuid4(),
        ),
        id="test-id",
    )

    # Verify expectation for all nulls case (non_null_proportion = 0)
    assert len(created_expectations) == 1
    expectation = created_expectations[0]
    assert isinstance(expectation, gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween)
    assert expectation.column
    assert expectation.min_value is None
    assert expectation.max_value == 0
    assert expectation.windows is None
    assert expectation.severity == FailureSeverity.WARNING

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
            workspace_id=uuid.uuid4(),
        ),
        id="test-id",
    )

    # Verify expectation for no nulls case (non_null_proportion = 1)
    assert len(created_expectations) == 1
    expectation = created_expectations[0]
    assert isinstance(expectation, gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween)
    assert expectation.column
    assert expectation.max_value is None
    assert expectation.min_value == 1
    assert expectation.windows is None
    assert expectation.severity == FailureSeverity.WARNING


def test_test_connection_error_logs_at_warning_not_error(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
):
    """
    Test that TestConnectionError is logged at WARNING level (not ERROR/exception).

    This is critical because:
    - ERROR level logs are captured by Sentry and create alerts
    - TestConnectionError represents user configuration issues (e.g., table not found, permission denied)
    - We want these in logs for debugging but NOT in Sentry as they cause alert fatigue

    See GX-1859 for context.
    """
    # Setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
    )

    # Mock _retrieve_asset_from_asset_name to raise TestConnectionError
    test_connection_error_msg = (
        "Attempt to connect to table: nonexistent_table failed. [DELTA_TABLE_NOT_FOUND]"
    )

    def mock_retrieve_asset_raises_test_connection_error(event, asset_name):
        raise TestConnectionError(message=test_connection_error_msg)

    mocker.patch.object(
        action,
        "_retrieve_asset_from_asset_name",
        side_effect=mock_retrieve_asset_raises_test_connection_error,
    )

    # Capture logs at DEBUG level to ensure we catch all levels
    with caplog.at_level(
        logging.DEBUG,
        logger="great_expectations_cloud.agent.actions.generate_data_quality_check_expectations_action",
    ):
        # Run the action - expect PartialGenerateDataQualityCheckExpectationError
        with pytest.raises(PartialGenerateDataQualityCheckExpectationError) as exc_info:
            action.run(
                event=GenerateDataQualityCheckExpectationsEvent(
                    type="generate_data_quality_check_expectations_request.received",
                    organization_id=uuid.uuid4(),
                    datasource_name="test-datasource",
                    data_assets=["failing-asset"],
                    selected_data_quality_issues=[],
                    workspace_id=uuid.uuid4(),
                ),
                id="test-id",
            )

    # Verify the exception message includes the asset name
    assert "failing-asset" in str(exc_info.value)

    # Verify WARNING log was emitted (not ERROR)
    warning_logs = [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING and "User configuration error" in record.message
    ]
    assert len(warning_logs) == 1, f"Expected exactly 1 WARNING log, got {len(warning_logs)}"
    assert "failing-asset" in warning_logs[0].message

    # Verify NO ERROR logs were emitted for this TestConnectionError
    error_logs = [
        record
        for record in caplog.records
        if record.levelno == logging.ERROR and "failing-asset" in str(record.message)
    ]
    assert len(error_logs) == 0, (
        f"Expected no ERROR logs for TestConnectionError, but got: {error_logs}"
    )


def test_non_test_connection_error_logs_at_error(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
):
    """
    Test that non-TestConnectionError exceptions are still logged at ERROR level.

    This ensures we haven't accidentally suppressed all error logging -
    only user configuration errors (TestConnectionError) should be at WARNING.
    """
    # Setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
    )

    # Mock _retrieve_asset_from_asset_name to raise a RuntimeError (not TestConnectionError)
    def mock_retrieve_asset_raises_runtime_error(event, asset_name):
        raise RuntimeError("Unexpected internal error")  # noqa: TRY003

    mocker.patch.object(
        action,
        "_retrieve_asset_from_asset_name",
        side_effect=mock_retrieve_asset_raises_runtime_error,
    )

    # Capture logs
    with caplog.at_level(
        logging.DEBUG,
        logger="great_expectations_cloud.agent.actions.generate_data_quality_check_expectations_action",
    ):
        with pytest.raises(PartialGenerateDataQualityCheckExpectationError):
            action.run(
                event=GenerateDataQualityCheckExpectationsEvent(
                    type="generate_data_quality_check_expectations_request.received",
                    organization_id=uuid.uuid4(),
                    datasource_name="test-datasource",
                    data_assets=["failing-asset"],
                    selected_data_quality_issues=[],
                    workspace_id=uuid.uuid4(),
                ),
                id="test-id",
            )

    # Verify ERROR log was emitted (not WARNING)
    error_logs = [
        record
        for record in caplog.records
        if record.levelno == logging.ERROR and "failing-asset" in str(record.message)
    ]
    assert len(error_logs) == 1, (
        f"Expected exactly 1 ERROR log for RuntimeError, got {len(error_logs)}"
    )


@pytest.mark.parametrize(
    "input_message, expected_output",
    [
        pytest.param(
            "[DELTA_TABLE_NOT_FOUND] The table or view cannot be found.",
            "The table or view cannot be found.",
            id="databricks_delta_table_not_found",
        ),
        pytest.param(
            "[PERMISSION_DENIED] User does not have SELECT permission.",
            "User does not have SELECT permission.",
            id="databricks_permission_denied",
        ),
        pytest.param(
            "[INCOMPATIBLE_VIEW_SCHEMA_CHANGE] View schema changed.",
            "View schema changed.",
            id="databricks_incompatible_view",
        ),
        pytest.param(
            "No bracketed code at start",
            "No bracketed code at start",
            id="no_bracketed_code",
        ),
        pytest.param(
            "Message with [BRACKETED] in middle",
            "Message with [BRACKETED] in middle",
            id="bracketed_code_in_middle_preserved",
        ),
        pytest.param(
            "  [LEADING_WHITESPACE] Message after whitespace.",
            "Message after whitespace.",
            id="leading_whitespace_stripped",
        ),
        pytest.param(
            "[ERROR_CODE_123] Message with numbers in code.",
            "Message with numbers in code.",
            id="error_code_with_numbers",
        ),
    ],
)
def test_strip_bracketed_error_code(input_message: str, expected_output: str):
    """Test that bracketed error codes are stripped from the beginning of messages."""
    assert _strip_bracketed_error_code(input_message) == expected_output


def test_error_message_includes_asset_error_details(
    mock_context: CloudDataContext,
    mocker: MockerFixture,
):
    """
    Test that PartialGenerateDataQualityCheckExpectationError includes
    per-asset error details with bracketed codes stripped.
    """
    # Setup
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateDataQualityCheckExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
    )

    # Mock _retrieve_asset_from_asset_name to raise TestConnectionError with bracketed code
    databricks_error_msg = (
        "[DELTA_TABLE_NOT_FOUND] The table or view `schema`.`table` cannot be found."
    )

    def mock_retrieve_asset_raises(event, asset_name):
        raise TestConnectionError(message=databricks_error_msg)

    mocker.patch.object(
        action,
        "_retrieve_asset_from_asset_name",
        side_effect=mock_retrieve_asset_raises,
    )

    # Run the action
    with pytest.raises(PartialGenerateDataQualityCheckExpectationError) as exc_info:
        action.run(
            event=GenerateDataQualityCheckExpectationsEvent(
                type="generate_data_quality_check_expectations_request.received",
                organization_id=uuid.uuid4(),
                datasource_name="test-datasource",
                data_assets=["my_table"],
                selected_data_quality_issues=[],
                workspace_id=uuid.uuid4(),
            ),
            id="test-id",
        )

    error_message = str(exc_info.value)

    # Verify the message includes the asset name with error detail
    assert "my_table:" in error_message
    # Verify the bracketed code was stripped
    assert "[DELTA_TABLE_NOT_FOUND]" not in error_message
    # Verify the descriptive portion was preserved
    assert "The table or view `schema`.`table` cannot be found." in error_message
    # Verify header and footer are present
    assert "Unable to autogenerate expectations for 1 of 1 Data Assets." in error_message
    assert "Check your connection details" in error_message


if __name__ == "__main__":
    print(GenerateDataQualityCheckExpectationsEvent.__module__)
