from __future__ import annotations

import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING

import great_expectations.expectations as gxe
import pytest
from great_expectations import ExpectationSuite, ValidationDefinition
from great_expectations.core.factory import ValidationDefinitionFactory
from great_expectations.core.partitioners import ColumnPartitionerDaily
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import SnowflakeDatasource
from great_expectations.datasource.fluent.sql_datasource import TableAsset

from great_expectations_cloud.agent.actions.generate_expectations_action import (
    CREATED_VIA_EXPECT_AI,
    MAX_PRUNED_EXPECTATIONS,
    ExpectationPruner,
    GenerateExpectationsAction,
)
from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import AssetReviewAgentResult
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    GenerateExpectationsOutputMetrics,
)
from great_expectations_cloud.agent.models import (
    CreatedResource,
    DomainContext,
    GenerateExpectationsEvent,
)
from great_expectations_cloud.agent.services.expectation_draft_config_service import (
    CreatedResourceTypes,
    ExpectationDraftConfigError,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from tests.agent.conftest import MockCreateSessionType


@pytest.fixture()
def mock_openai_credentials(mocker: MockerFixture) -> None:
    """Mock ensure_openai_credentials so tests don't require a real OPENAI_API_KEY."""
    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations_action.ensure_openai_credentials",
    )


@pytest.fixture(scope="function")
def fluent_snowflake_datasource(mocker: MockerFixture) -> SnowflakeDatasource:
    datasource = SnowflakeDatasource(
        name="test",
        user="test",
        account="test",
        password="test",
        database="test",
        schema="test",
        warehouse="test",
        role="test",
    )
    mocker.patch.object(TableAsset, "test_connection")
    mocker.patch.object(TableAsset, "validate_batch_definition")
    asset = datasource.add_table_asset(
        "test_table_asset",
        table_name="demo",
    )
    asset.add_batch_definition(
        "test_batch_definition",
        partitioner=ColumnPartitionerDaily(column_name="date"),
    )
    return datasource


@pytest.fixture(scope="module")
def example_suite() -> ExpectationSuite:
    return ExpectationSuite(
        name="example_suite",
        id="123",
        expectations=[gxe.ExpectColumnToExist(column="example-column")],
    )


@pytest.fixture(scope="function")
def managed_example_suite() -> ExpectationSuite:
    suite = ExpectationSuite(
        name="test_table_asset 12345 GX-Managed Expectation Suite",
        id="321",
        expectations=[gxe.ExpectColumnToExist(column="example-column")],
    )
    return suite


@pytest.fixture(scope="module")
def managed_batch_definition_name() -> str:
    return "test_table_asset - GX-Managed Batch Definition"


@pytest.fixture(scope="function")
def managed_mock_context(
    fluent_snowflake_datasource: SnowflakeDatasource,
    managed_example_suite: ExpectationSuite,
    managed_batch_definition_name: str,
    example_suite: ExpectationSuite,
    base_url: str,
    auth_key: str,
    organization_id: uuid.UUID,
    workspace_id: uuid.UUID,
    mocker: MockerFixture,
) -> CloudDataContext:
    # set up managed batch definition
    asset_name = "test_table_asset"
    asset = fluent_snowflake_datasource.get_asset(asset_name)
    mocker.patch.object(TableAsset, "validate_batch_definition")
    batch_def = asset.add_batch_definition(
        name=managed_batch_definition_name,
        partitioner=ColumnPartitionerDaily(column_name="date"),
    )
    managed_validation_def = ValidationDefinition(
        name=f"{asset_name} ABCD GX-Managed Validation",
        data=batch_def,
        suite=managed_example_suite,
    )
    unmanaged_validation_def = ValidationDefinition(
        name="test_validation_definition",
        data=asset.get_batch_definition("test_batch_definition"),
        suite=example_suite,
    )

    mock_context = mocker.MagicMock(autospec=CloudDataContext)
    mock_context.data_sources.get.return_value = fluent_snowflake_datasource
    mock_context.validation_definitions = mocker.PropertyMock(spec=ValidationDefinitionFactory)
    mock_context.validation_definitions.all.return_value = [
        unmanaged_validation_def,
        managed_validation_def,
    ]

    # Mock ge_cloud_config for the service to access API configuration
    mock_ge_cloud_config = mocker.MagicMock()
    mock_ge_cloud_config.base_url = base_url
    mock_ge_cloud_config.organization_id = str(organization_id)
    mock_ge_cloud_config.workspace_id = str(workspace_id)
    mock_ge_cloud_config.access_token = auth_key
    mock_context.ge_cloud_config = mock_ge_cloud_config

    return mock_context  # type: ignore[no-any-return]  # this is fine for testing


@pytest.fixture(scope="function")
def managed_mock_resources(
    managed_mock_context: CloudDataContext,
    managed_example_suite: ExpectationSuite,
    managed_batch_definition_name: str,
) -> tuple[CloudDataContext, ExpectationSuite, str]:
    return (managed_mock_context, managed_example_suite, managed_batch_definition_name)


def ai_generated_suite() -> ExpectationSuite:
    return ExpectationSuite(
        name="echoes generated suite",
        id=None,
        expectations=[
            gxe.ExpectColumnToExist(column="a-column"),
            gxe.ExpectColumnMaxToBeBetween(column="another-column", min_value=1),
        ],
    )


@pytest.mark.unit
def test_run_generate_expectations_action(
    managed_mock_resources: tuple[CloudDataContext, ExpectationSuite, str],
    base_url: str,
    auth_key: str,
    organization_id: uuid.UUID,
    workspace_id: uuid.UUID,
    mocker: MockerFixture,
    mock_create_session: MockCreateSessionType,
    mock_openai_credentials: None,
):
    managed_mock_context, _, managed_batch_definition_name = managed_mock_resources
    mock_runner = mocker.patch(
        "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.AssetReviewAgent.arun"
    )
    generated_suite = ai_generated_suite()
    generated_expectations = generated_suite.expectations
    mock_runner.return_value = AssetReviewAgentResult(
        expectation_suite=generated_suite,
        metrics=GenerateExpectationsOutputMetrics(column_names=[]),
    )

    # Mock the session for draft config creation
    _session = mock_create_session(
        "great_expectations_cloud.agent.actions.generate_expectations_action",
        "post",
        HTTPStatus.CREATED,
        {"data": [{"id": str(uuid.uuid4())} for _ in generated_expectations]},
    )

    generate_expectations_event = GenerateExpectationsEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        type="generate_expectations_action.received",
        datasource_name="test",
        data_asset_name="test_table_asset",
        batch_definition_name=managed_batch_definition_name,
        batch_parameters={},
    )

    generate_expectations_action = GenerateExpectationsAction(
        context=managed_mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    # Mock the metric service to return a non-zero row count
    mock_metric_result = mocker.MagicMock()
    mock_metric_result.value = 100  # Non-zero to avoid empty table error
    mock_metric_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations_action.MetricService"
    )
    mock_metric_service.return_value.get_metric_result.return_value = mock_metric_result

    # Mock the _create_expectation_draft_configs method to verify it's called
    mock_create_draft_configs = mocker.patch.object(
        generate_expectations_action,
        "_create_expectation_draft_configs",
        return_value=mocker.MagicMock(
            id="test-id",
            type=generate_expectations_event.type,
            created_resources=[],
        ),
    )

    # Action
    result = generate_expectations_action.run(
        event=generate_expectations_event,
        id="test-id",
    )

    # Assert
    assert result.type == generate_expectations_event.type
    mock_runner.assert_called_once()
    mock_create_draft_configs.assert_called_once()
    assert mock_create_draft_configs.call_args.kwargs["id"] == "test-id"
    assert mock_create_draft_configs.call_args.kwargs["event"] == generate_expectations_event
    assert len(mock_create_draft_configs.call_args.kwargs["expectations"]) == len(
        generated_suite.expectations
    )


@pytest.mark.unit
def test_run_generate_expectations_action_post_fails(
    managed_mock_resources: tuple[CloudDataContext, ExpectationSuite, str],
    base_url: str,
    auth_key: str,
    organization_id: uuid.UUID,
    workspace_id: uuid.UUID,
    mocker: MockerFixture,
    mock_create_session: MockCreateSessionType,
    mock_openai_credentials: None,
):
    managed_mock_context, _, managed_batch_definition_name = managed_mock_resources
    mock_runner = mocker.patch(
        "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.AssetReviewAgent.arun"
    )
    mock_runner.return_value = AssetReviewAgentResult(
        expectation_suite=ai_generated_suite(),
        metrics=GenerateExpectationsOutputMetrics(column_names=[]),
    )
    mock_create_session(
        "great_expectations_cloud.agent.services.expectation_draft_config_service",
        "post",
        HTTPStatus.BAD_REQUEST,
        {},
    )

    generate_expectations_event = GenerateExpectationsEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        type="generate_expectations_action.received",
        datasource_name="test",
        data_asset_name="test_table_asset",
        batch_definition_name=managed_batch_definition_name,
        batch_parameters={},
    )

    generate_expectations_action = GenerateExpectationsAction(
        context=managed_mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    # Mock the metric service to return a non-zero row count
    mock_metric_result = mocker.MagicMock()
    mock_metric_result.value = 100  # Non-zero to avoid empty table error
    mock_metric_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations_action.MetricService"
    )
    mock_metric_service.return_value.get_metric_result.return_value = mock_metric_result

    # Action
    with pytest.raises(ExpectationDraftConfigError):
        generate_expectations_action.run(
            event=generate_expectations_event,
            id="test-id",
        )


@pytest.mark.unit
def test_run_generate_expectations_action_prunes_expectations(
    managed_mock_resources: tuple[CloudDataContext, ExpectationSuite, str],
    base_url: str,
    auth_key: str,
    organization_id: uuid.UUID,
    workspace_id: uuid.UUID,
    mocker: MockerFixture,
    mock_create_session: MockCreateSessionType,
    mock_openai_credentials: None,
):
    """Test to ensure we call our pruning algorithm;
    More unit tests on just the algorithm below"""
    # Arrange
    suite_with_many_expectations = ExpectationSuite(
        name="whatever",
        expectations=[
            gxe.ExpectColumnMaxToBeBetween(column=f"wat-{i}", min_value=1) for i in range(20)
        ],
    )

    managed_mock_context, _, managed_batch_definition_name = managed_mock_resources
    mock_runner = mocker.patch(
        "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.AssetReviewAgent.arun"
    )
    mock_runner.return_value = AssetReviewAgentResult(
        expectation_suite=suite_with_many_expectations,
        metrics=GenerateExpectationsOutputMetrics(column_names=[]),
    )

    # Mock the session for draft config creation
    _session = mock_create_session(
        "great_expectations_cloud.agent.actions.generate_expectations_action",
        "post",
        HTTPStatus.CREATED,
        {"data": [{"id": str(uuid.uuid4())} for _ in range(MAX_PRUNED_EXPECTATIONS)]},
    )

    generate_expectations_event = GenerateExpectationsEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        type="generate_expectations_action.received",
        datasource_name="test",
        data_asset_name="test_table_asset",
        batch_definition_name=managed_batch_definition_name,
        batch_parameters={},
    )

    generate_expectations_action = GenerateExpectationsAction(
        context=managed_mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    # Mock the metric service to return a non-zero row count
    mock_metric_result = mocker.MagicMock()
    mock_metric_result.value = 100  # Non-zero to avoid empty table error
    mock_metric_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations_action.MetricService"
    )
    mock_metric_service.return_value.get_metric_result.return_value = mock_metric_result

    # Mock the _create_expectation_draft_configs method to verify it's called with pruned expectations
    mock_create_draft_configs = mocker.patch.object(
        generate_expectations_action,
        "_create_expectation_draft_configs",
        return_value=mocker.MagicMock(
            id="test-id",
            type=generate_expectations_event.type,
            created_resources=[],
        ),
    )

    # Action
    result = generate_expectations_action.run(
        event=generate_expectations_event,
        id="test-id",
    )

    # Assert
    assert result.type == generate_expectations_event.type
    mock_runner.assert_called_once()
    mock_create_draft_configs.assert_called_once()
    # Verify that the number of expectations passed to create_draft_configs matches MAX_PRUNED_EXPECTATIONS
    assert (
        len(mock_create_draft_configs.call_args.kwargs["expectations"]) == MAX_PRUNED_EXPECTATIONS
    )


class TestExpectationPruning:
    """Test cases around pruning expectations to reduce noise."""

    COL_NAME = "foo"
    PRUNER = ExpectationPruner(max_expectations=10)

    @pytest.mark.unit
    def test_one_expectation_per_column_when_many_expectations(self):
        first_expectation = gxe.ExpectColumnMinToBeBetween(
            column=self.COL_NAME, min_value=3, max_value=45
        )
        second_expectation = gxe.ExpectColumnMaxToBeBetween(
            column=self.COL_NAME, min_value=2, max_value=46
        )

        output = self.PRUNER.prune_expectations(
            [
                first_expectation,
                second_expectation,
                *[
                    gxe.ExpectColumnMaxToBeBetween(column=f"wat-{i}", min_value=1)
                    for i in range(MAX_PRUNED_EXPECTATIONS * 2)
                ],
            ]
        )

        assert first_expectation in output
        assert second_expectation not in output

    @pytest.mark.unit
    def test_max_of_two_expectation_per_column(self):
        first_expectation = gxe.ExpectColumnMinToBeBetween(
            column=self.COL_NAME, min_value=3, max_value=45
        )
        second_expectation = gxe.ExpectColumnMaxToBeBetween(
            column=self.COL_NAME, min_value=2, max_value=46
        )
        third_expectation = gxe.ExpectColumnMeanToBeBetween(
            column=self.COL_NAME, min_value=2, max_value=46
        )
        output = self.PRUNER.prune_expectations(
            [
                first_expectation,
                second_expectation,
                third_expectation,
            ],
        )

        assert output == [first_expectation, second_expectation]

    @pytest.mark.unit
    def test_max_of_pruned_expectations_is_module_constant(self):
        output = self.PRUNER.prune_expectations(
            [
                gxe.ExpectColumnMaxToBeBetween(column=f"wat-{i}", min_value=1)
                for i in range(MAX_PRUNED_EXPECTATIONS * 2)
            ]
        )

        assert len(output) == MAX_PRUNED_EXPECTATIONS

    @pytest.mark.unit
    def test_prune_invalid_columns_keeps_valid_single_column(self):
        """Test that expectations with valid single column are kept."""
        valid_columns = {"col1", "col2", "col3"}
        expectation = gxe.ExpectColumnMinToBeBetween(column="col1", min_value=3, max_value=45)

        output = self.PRUNER.prune_invalid_columns([expectation], valid_columns)

        assert expectation in output
        assert len(output) == 1

    @pytest.mark.unit
    def test_prune_invalid_columns_prunes_invalid_single_column(self):
        """Test that expectations with invalid single column are pruned."""
        valid_columns = {"col1", "col2", "col3"}
        expectation = gxe.ExpectColumnMinToBeBetween(
            column="invalid_col", min_value=3, max_value=45
        )

        output = self.PRUNER.prune_invalid_columns([expectation], valid_columns)

        assert expectation not in output
        assert len(output) == 0

    @pytest.mark.unit
    def test_prune_invalid_columns_keeps_valid_column_list(self):
        """Test that expectations with all valid columns in column_list are kept."""
        valid_columns = {"col1", "col2", "col3"}
        expectation = gxe.ExpectCompoundColumnsToBeUnique(
            column_list=["col1", "col2"], description="Test"
        )

        output = self.PRUNER.prune_invalid_columns([expectation], valid_columns)

        assert expectation in output
        assert len(output) == 1

    @pytest.mark.unit
    def test_prune_invalid_columns_prunes_invalid_column_in_list(self):
        """Test that expectations with any invalid column in column_list are pruned."""
        valid_columns = {"col1", "col2", "col3"}
        expectation = gxe.ExpectCompoundColumnsToBeUnique(
            column_list=["col1", "invalid_col"], description="Test"
        )

        output = self.PRUNER.prune_invalid_columns([expectation], valid_columns)

        assert expectation not in output
        assert len(output) == 0

    @pytest.mark.unit
    def test_prune_invalid_columns_keeps_expectations_without_column_fields(self):
        """Test that expectations without column or column_list fields are kept."""
        valid_columns = {"col1", "col2"}
        # UnexpectedRowsExpectation doesn't have column or column_list
        expectation = gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {{batch}} WHERE col1 IS NULL",
            description="Test",
        )

        output = self.PRUNER.prune_invalid_columns([expectation], valid_columns)

        assert expectation in output
        assert len(output) == 1

    @pytest.mark.unit
    def test_prune_invalid_columns_mixed_expectations(self):
        """Test pruning with a mix of valid and invalid expectations."""
        valid_columns = {"col1", "col2", "col3"}
        valid_single = gxe.ExpectColumnMinToBeBetween(column="col1", min_value=1, max_value=10)
        invalid_single = gxe.ExpectColumnMaxToBeBetween(
            column="invalid_col", min_value=1, max_value=10
        )
        valid_list = gxe.ExpectCompoundColumnsToBeUnique(
            column_list=["col1", "col2"], description="Test"
        )
        invalid_list = gxe.ExpectCompoundColumnsToBeUnique(
            column_list=["col1", "invalid_col"], description="Test"
        )
        no_column = gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {{batch}}", description="Test"
        )

        expectations: list[gxe.Expectation] = [
            valid_single,
            invalid_single,
            valid_list,
            invalid_list,
            no_column,
        ]

        output = self.PRUNER.prune_invalid_columns(expectations, valid_columns)

        assert valid_single in output
        assert invalid_single not in output
        assert valid_list in output
        assert invalid_list not in output
        assert no_column in output
        assert len(output) == 3

    @pytest.mark.unit
    def test_prune_invalid_columns_all_columns_valid_in_list(self):
        """Test that expectations with all valid columns in column_list are kept."""
        valid_columns = {"col1", "col2", "col3"}
        expectation = gxe.ExpectCompoundColumnsToBeUnique(
            column_list=["col1", "col2", "col3"], description="Test"
        )

        output = self.PRUNER.prune_invalid_columns([expectation], valid_columns)

        assert expectation in output
        assert len(output) == 1


@pytest.mark.unit
def test_create_expectation_draft_configs_uses_api_config(
    base_url: str,
    auth_key: str,
    organization_id: uuid.UUID,
    workspace_id: uuid.UUID,
    mock_context,
    mocker: MockerFixture,
):
    """Test that _create_expectation_draft_configs correctly uses DraftConfigAPIConfig."""
    # Arrange
    generate_expectations_event = GenerateExpectationsEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        type="generate_expectations_action.received",
        datasource_name="test_datasource",
        data_asset_name="test_asset",
        batch_definition_name="test_batch",
        batch_parameters={},
    )

    generate_expectations_action = GenerateExpectationsAction(
        context=mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    # Mock the ExpectationDraftConfigService
    mock_service_class = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations_action.ExpectationDraftConfigService"
    )
    mock_service_instance = mock_service_class.return_value
    mock_service_instance.create_expectation_draft_configs.return_value = [
        CreatedResource(
            resource_id="test-resource-1",
            type=CreatedResourceTypes.EXPECTATION_DRAFT_CONFIG,
        ),
        CreatedResource(
            resource_id="test-resource-2",
            type=CreatedResourceTypes.EXPECTATION_DRAFT_CONFIG,
        ),
    ]

    # Create some test expectations
    test_expectations: list[gxe.Expectation] = [
        gxe.ExpectColumnValuesToBeBetween(column="test_col", min_value=1, max_value=10),
        gxe.ExpectColumnValuesToNotBeNull(column="test_col2"),
    ]

    # Action
    result = generate_expectations_action._create_expectation_draft_configs(
        id="test-id", event=generate_expectations_event, expectations=test_expectations
    )

    # Assert
    # Verify ExpectationDraftConfigService was instantiated correctly
    mock_service_class.assert_called_once_with(
        context=mock_context, created_via=CREATED_VIA_EXPECT_AI
    )

    # Verify create_expectation_draft_configs was called with the right parameters
    mock_service_instance.create_expectation_draft_configs.assert_called_once()
    call_args = mock_service_instance.create_expectation_draft_configs.call_args

    # Check the method arguments
    assert call_args.kwargs["data_source_name"] == "test_datasource"
    assert call_args.kwargs["data_asset_name"] == "test_asset"
    assert call_args.kwargs["expectations"] == test_expectations
    assert call_args.kwargs["event_id"] == "test-id"

    # Verify the result structure
    assert result.id == "test-id"
    assert result.type == generate_expectations_event.type
    assert len(result.created_resources) == 2


@pytest.mark.unit
def test_run_generate_expectations_action_missing_openai_credentials(
    managed_mock_resources: tuple[CloudDataContext, ExpectationSuite, str],
    base_url: str,
    auth_key: str,
    organization_id: uuid.UUID,
    workspace_id: uuid.UUID,
    mocker: MockerFixture,
):
    """Test that run raises ValueError when OpenAI credentials are not set."""
    managed_mock_context, _, managed_batch_definition_name = managed_mock_resources

    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations_action.ensure_openai_credentials",
        side_effect=ValueError(
            "OpenAI credentials are not set. Please set the OPENAI_API_KEY environment variable to enable ExpectAI."
        ),
    )

    generate_expectations_event = GenerateExpectationsEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        type="generate_expectations_action.received",
        datasource_name="test",
        data_asset_name="test_table_asset",
        batch_definition_name=managed_batch_definition_name,
        batch_parameters={},
    )

    generate_expectations_action = GenerateExpectationsAction(
        context=managed_mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    with pytest.raises(ValueError, match="OpenAI credentials are not set"):
        generate_expectations_action.run(
            event=generate_expectations_event,
            id="test-id",
        )


@pytest.mark.unit
def test_run_generate_expectations_action_empty_table_error(
    managed_mock_resources: tuple[CloudDataContext, ExpectationSuite, str],
    base_url: str,
    auth_key: str,
    organization_id: uuid.UUID,
    workspace_id: uuid.UUID,
    mocker: MockerFixture,
    mock_openai_credentials: None,
):
    """Test that GenerateExpectationsAction raises RuntimeError when the table has no records."""
    # Arrange
    managed_mock_context, _, managed_batch_definition_name = managed_mock_resources

    # Mock the batch definition and batch
    mock_batch_definition = mocker.MagicMock()
    mock_batch = mocker.MagicMock()
    mock_batch_definition.get_batch.return_value = mock_batch
    # Mock the method chain: context.data_sources.get().get_asset().get_batch_definition()
    mock_asset = mocker.MagicMock()
    mock_asset.get_batch_definition.return_value = mock_batch_definition
    # Mock the get_asset method on the datasource
    mock_datasource = mocker.MagicMock()
    mock_datasource.get_asset.return_value = mock_asset
    # Use patch.object to avoid mypy issues with return_value
    mocker.patch.object(managed_mock_context.data_sources, "get", return_value=mock_datasource)

    # Mock the metric service to return a row count of 0
    mock_metric_result = mocker.MagicMock()
    mock_metric_result.value = 0
    mock_metric_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations_action.MetricService"
    )
    mock_metric_service.return_value.get_metric_result.return_value = mock_metric_result

    generate_expectations_event = GenerateExpectationsEvent(
        organization_id=organization_id,
        workspace_id=workspace_id,
        type="generate_expectations_action.received",
        datasource_name="test",
        data_asset_name="test_table_asset",
        batch_definition_name=managed_batch_definition_name,
        batch_parameters={},
    )

    generate_expectations_action = GenerateExpectationsAction(
        context=managed_mock_context,
        base_url=base_url,
        domain_context=DomainContext(
            organization_id=organization_id,
            workspace_id=workspace_id,
        ),
        auth_key=auth_key,
    )

    # Action & Assert
    with pytest.raises(RuntimeError) as exc_info:
        generate_expectations_action.run(
            event=generate_expectations_event,
            id="test-id",
        )

    # Verify the error message
    assert "Could not generate Expectations because the Data Asset has no records" in str(
        exc_info.value
    )
    assert "Ensure the table or view connected to your Data Asset has records and try again" in str(
        exc_info.value
    )
