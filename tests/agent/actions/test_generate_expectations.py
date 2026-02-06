from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest
from great_expectations import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToNotBeNull,
)
from great_expectations.metrics.batch.batch_column_types import (
    BatchColumnTypesResult,
    ColumnType,
)
from great_expectations.metrics.metric_results import MetricResult

from great_expectations_cloud.agent.actions.generate_expectations import (
    GenerateExpectationsAction,
)
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.models import (
    EXPECTATION_DRAFT_CONFIG,
    CreatedResource,
    DomainContext,
    GenerateExpectationsEvent,
    RunRdAgentEvent,
)
from great_expectations_cloud.agent.services.expectation_service import ListExpectationsError

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


@pytest.fixture
def mock_event() -> GenerateExpectationsEvent:
    """Create a mock GenerateExpectationsEvent."""
    return GenerateExpectationsEvent(
        type="generate_expectations_action.received",
        workspace_id=uuid.uuid4(),
        datasource_name="test_datasource",
        data_asset_name="test_asset",
        batch_definition_name="test_batch_definition",
        batch_parameters=None,
    )


@pytest.fixture
def mock_legacy_event() -> RunRdAgentEvent:
    """Create a mock RunRdAgentEvent (legacy)."""
    return RunRdAgentEvent(
        type="rd_agent_action.received",
        workspace_id=uuid.uuid4(),
        datasource_name="test_datasource",
        data_asset_name="test_asset",
        batch_definition_name="test_batch_definition",
        batch_parameters=None,
    )


def test_generate_expectations_success(
    mock_context: CloudDataContext,
    mock_event: GenerateExpectationsEvent,
    mocker: MockerFixture,
):
    """Test successful expectation generation."""
    # ARRANGE
    mocker.patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})

    org_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    domain_context = DomainContext(organization_id=org_id, workspace_id=workspace_id)

    action = GenerateExpectationsAction(
        context=mock_context,
        base_url="https://test-base-url",
        domain_context=domain_context,
        auth_key="test-auth-key",
    )

    # Mock batch has rows
    mock_batch_definition = mocker.MagicMock()
    mock_context.data_sources.get.return_value.get_asset.return_value.get_batch_definition.return_value = mock_batch_definition

    # Mock row count metric (not zero)
    mock_row_count_result = MetricResult(value=100)

    # Mock column types metric
    mock_column_types_result = BatchColumnTypesResult(
        value=[
            ColumnType(name="col1", type="INTEGER"),
            ColumnType(name="col2", type="VARCHAR"),
        ]
    )

    # Mock AssetReviewAgent
    mock_expectations = [
        ExpectColumnValuesToNotBeNull(column="col1"),
        ExpectColumnValuesToBeInSet(column="col2", value_set=["a", "b"]),
    ]
    mock_suite = ExpectationSuite(
        name="test-suite",
        expectations=mock_expectations,
    )
    mock_agent_class = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.AssetReviewAgent"
    )
    mock_agent_instance = mock_agent_class.return_value
    mock_agent_instance.arun = mocker.AsyncMock(return_value=mock_suite)

    # Mock ExpectationService
    mock_expectation_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.ExpectationService"
    )
    mock_expectation_service.return_value.get_existing_expectations_by_data_asset.return_value = []

    # Mock ExpectationDraftConfigService
    draft_id = str(uuid.uuid4())
    mock_draft_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.ExpectationDraftConfigService"
    )
    mock_draft_service.return_value.create_expectation_draft_configs.return_value = [
        CreatedResource(resource_id=draft_id, type=EXPECTATION_DRAFT_CONFIG)
    ]

    # Mock MetricService
    mock_metric_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.MetricService"
    )
    mock_metric_service.return_value.get_metric_result.side_effect = [
        mock_row_count_result,
        mock_column_types_result,
    ]

    # ACT
    result = action.run(event=mock_event, id="test-id")

    # ASSERT
    assert result.id == "test-id"
    assert result.type == mock_event.type
    assert len(result.created_resources) == 1
    assert result.created_resources[0].resource_id == draft_id
    assert result.created_resources[0].type == EXPECTATION_DRAFT_CONFIG


def test_generate_expectations_missing_openai_credentials(
    mock_context: CloudDataContext,
    mock_event: GenerateExpectationsEvent,
    mocker: MockerFixture,
):
    """Test that missing OpenAI credentials raises GXAgentError."""
    # ARRANGE
    mocker.patch.dict("os.environ", {}, clear=True)

    domain_context = DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4())
    action = GenerateExpectationsAction(
        context=mock_context,
        base_url="https://test-base-url",
        domain_context=domain_context,
        auth_key="test-auth-key",
    )

    # ACT & ASSERT
    with pytest.raises(GXAgentError) as exc_info:
        action.run(event=mock_event, id="test-id")

    assert "OpenAI credentials not configured" in str(exc_info.value)
    assert "OPENAI_API_KEY" in str(exc_info.value)


def test_generate_expectations_empty_batch(
    mock_context: CloudDataContext,
    mock_event: GenerateExpectationsEvent,
    mocker: MockerFixture,
):
    """Test that empty batch (0 rows) raises RuntimeError."""
    # ARRANGE
    mocker.patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})

    domain_context = DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4())
    action = GenerateExpectationsAction(
        context=mock_context,
        base_url="https://test-base-url",
        domain_context=domain_context,
        auth_key="test-auth-key",
    )

    # Mock batch has 0 rows
    mock_batch_definition = mocker.MagicMock()
    mock_context.data_sources.get.return_value.get_asset.return_value.get_batch_definition.return_value = mock_batch_definition

    mock_row_count_result = MetricResult(value=0)
    mock_metric_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.MetricService"
    )
    mock_metric_service.return_value.get_metric_result.return_value = mock_row_count_result

    # ACT & ASSERT
    with pytest.raises(RuntimeError) as exc_info:
        action.run(event=mock_event, id="test-id")

    assert "no records" in str(exc_info.value).lower()


def test_generate_expectations_handles_list_expectations_error(
    mock_context: CloudDataContext,
    mock_event: GenerateExpectationsEvent,
    mocker: MockerFixture,
):
    """Test that ExpectationService errors are logged and handled gracefully."""
    # ARRANGE
    mocker.patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})

    domain_context = DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4())
    action = GenerateExpectationsAction(
        context=mock_context,
        base_url="https://test-base-url",
        domain_context=domain_context,
        auth_key="test-auth-key",
    )

    # Mock batch has rows
    mock_batch_definition = mocker.MagicMock()
    mock_context.data_sources.get.return_value.get_asset.return_value.get_batch_definition.return_value = mock_batch_definition

    mock_row_count_result = MetricResult(value=100)
    mock_column_types_result = BatchColumnTypesResult(
        value=[ColumnType(name="col1", type="INTEGER")]
    )

    # Mock ExpectationService to raise error
    mock_expectation_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.ExpectationService"
    )
    mock_expectation_service.return_value.get_existing_expectations_by_data_asset.side_effect = (
        ListExpectationsError(status_code=404, asset_id="test-asset")
    )

    # Mock AssetReviewAgent
    mock_suite = ExpectationSuite(name="test-suite", expectations=[])
    mock_agent_class = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.AssetReviewAgent"
    )
    mock_agent_instance = mock_agent_class.return_value
    mock_agent_instance.arun = mocker.AsyncMock(return_value=mock_suite)

    # Mock other services
    mock_draft_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.ExpectationDraftConfigService"
    )
    mock_draft_service.return_value.create_expectation_draft_configs.return_value = []

    mock_metric_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.MetricService"
    )
    mock_metric_service.return_value.get_metric_result.side_effect = [
        mock_row_count_result,
        mock_column_types_result,
    ]

    # ACT
    result = action.run(event=mock_event, id="test-id")

    # ASSERT - should complete successfully even with expectation service error
    assert result.id == "test-id"


def test_generate_expectations_with_legacy_event(
    mock_context: CloudDataContext,
    mock_legacy_event: RunRdAgentEvent,
    mocker: MockerFixture,
):
    """Test that action works with legacy RunRdAgentEvent."""
    # ARRANGE
    mocker.patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})

    domain_context = DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4())
    action = GenerateExpectationsAction(
        context=mock_context,
        base_url="https://test-base-url",
        domain_context=domain_context,
        auth_key="test-auth-key",
    )

    # Mock all dependencies
    mock_batch_definition = mocker.MagicMock()
    mock_context.data_sources.get.return_value.get_asset.return_value.get_batch_definition.return_value = mock_batch_definition

    mock_row_count_result = MetricResult(value=100)
    mock_column_types_result = BatchColumnTypesResult(
        value=[ColumnType(name="col1", type="INTEGER")]
    )

    mock_suite = ExpectationSuite(name="test-suite", expectations=[])
    mock_agent_class = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.AssetReviewAgent"
    )
    mock_agent_instance = mock_agent_class.return_value
    mock_agent_instance.arun = mocker.AsyncMock(return_value=mock_suite)

    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.ExpectationService"
    ).return_value.get_existing_expectations_by_data_asset.return_value = []

    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.ExpectationDraftConfigService"
    ).return_value.create_expectation_draft_configs.return_value = []

    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.MetricService"
    ).return_value.get_metric_result.side_effect = [
        mock_row_count_result,
        mock_column_types_result,
    ]

    # ACT
    result = action.run(event=mock_legacy_event, id="test-id")

    # ASSERT
    assert result.id == "test-id"
    assert result.type == mock_legacy_event.type


def test_generate_expectations_prunes_invalid_columns(
    mock_context: CloudDataContext,
    mock_event: GenerateExpectationsEvent,
    mocker: MockerFixture,
):
    """Test that expectations for invalid columns are pruned."""
    # ARRANGE
    mocker.patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})

    domain_context = DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4())
    action = GenerateExpectationsAction(
        context=mock_context,
        base_url="https://test-base-url",
        domain_context=domain_context,
        auth_key="test-auth-key",
    )

    mock_batch_definition = mocker.MagicMock()
    mock_context.data_sources.get.return_value.get_asset.return_value.get_batch_definition.return_value = mock_batch_definition

    mock_row_count_result = MetricResult(value=100)

    # Only col1 and col2 are valid
    mock_column_types_result = BatchColumnTypesResult(
        value=[
            ColumnType(name="col1", type="INTEGER"),
            ColumnType(name="col2", type="VARCHAR"),
        ]
    )

    # Mock AssetReviewAgent to return expectations including invalid column
    mock_expectations = [
        ExpectColumnValuesToNotBeNull(column="col1"),  # Valid
        ExpectColumnValuesToNotBeNull(column="invalid_col"),  # Invalid - should be pruned
        ExpectColumnValuesToBeInSet(column="col2", value_set=["a"]),  # Valid
    ]
    mock_suite = ExpectationSuite(name="test-suite", expectations=mock_expectations)
    mock_agent_class = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.AssetReviewAgent"
    )
    mock_agent_instance = mock_agent_class.return_value
    mock_agent_instance.arun = mocker.AsyncMock(return_value=mock_suite)

    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.ExpectationService"
    ).return_value.get_existing_expectations_by_data_asset.return_value = []

    mock_draft_service = mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.ExpectationDraftConfigService"
    )
    mock_draft_service.return_value.create_expectation_draft_configs.return_value = []

    mocker.patch(
        "great_expectations_cloud.agent.actions.generate_expectations.MetricService"
    ).return_value.get_metric_result.side_effect = [
        mock_row_count_result,
        mock_column_types_result,
    ]

    # ACT
    _result = action.run(event=mock_event, id="test-id")

    # ASSERT
    # Verify that create_expectation_draft_configs was called with pruned list
    call_args = mock_draft_service.return_value.create_expectation_draft_configs.call_args
    expectations_arg = call_args.kwargs["expectations"]

    # Should only have 2 expectations (invalid_col was pruned)
    assert len(expectations_arg) == 2
    column_names = [getattr(exp, "column", None) for exp in expectations_arg]
    assert "invalid_col" not in column_names
    assert "col1" in column_names
    assert "col2" in column_names
