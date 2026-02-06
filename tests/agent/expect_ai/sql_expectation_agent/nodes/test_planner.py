"""Unit tests for SQL expectation agent planner node."""

from __future__ import annotations

from collections import namedtuple
from unittest.mock import Mock, create_autospec, patch

import pandas as pd
import pytest
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import ColumnPartitioner
from great_expectations.datasource.fluent.sql_datasource import (
    SQLDatasource,
    TableAsset,
)
from great_expectations.metrics.batch.batch_column_types import (
    BatchColumnTypes,
    BatchColumnTypesResult,
)
from great_expectations.metrics.batch.sample_values import (
    SampleValues,
    SampleValuesResult,
)
from great_expectations.metrics.metric_results import MetricErrorResult
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from great_expectations_cloud.agent.expect_ai.exceptions import (
    InvalidAssetTypeError,
    InvalidDataSourceTypeError,
)
from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.planner import (
    SqlPlannerNode,
)
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    CoreMetrics,
    SqlExpectationInput,
    SqlExpectationState,
)

# Mock column type for testing
MockColumn = namedtuple("MockColumn", ["name", "type"])  # noqa: PYI024


class TestSqlPlannerNodeInitialization:
    """Test SqlPlannerNode initialization."""

    @pytest.mark.unit
    def test_init_with_metric_service(self) -> None:
        """Test that SqlPlannerNode initializes correctly with a metric service."""
        mock_metric_service = Mock(spec=MetricService)
        planner = SqlPlannerNode(metric_service=mock_metric_service)

        assert planner._metric_service is mock_metric_service


class TestSqlPlannerNodeCall:
    """Test SqlPlannerNode.__call__ method."""

    @pytest.fixture
    def mock_metric_service(self) -> Mock:
        """Create a mock metric service."""
        return Mock(spec=MetricService)

    @pytest.fixture
    def mock_core_metrics(self) -> Mock:
        """Create mock core metrics."""
        # Create a mock core_metrics object directly
        mock_core_metrics = Mock()

        # Create a proper BatchDefinition mock
        mock_batch_definition = Mock(spec=BatchDefinition[ColumnPartitioner])
        mock_batch_definition.id = "test_batch_def_id"
        mock_batch_definition.name = "test_batch_def"
        mock_batch_definition.data_asset_name = "test_asset"
        mock_core_metrics.batch_definition = mock_batch_definition

        mock_core_metrics.sql_dialect = "SQL dialect: postgresql"
        mock_core_metrics.table_name = "Table name: users"

        mock_schema_result = create_autospec(BatchColumnTypesResult, instance=True)
        mock_schema_result.value = [
            MockColumn(name="id", type="INTEGER"),
            MockColumn(name="name", type="VARCHAR"),
        ]
        mock_core_metrics.schema_result = mock_schema_result

        mock_sample_values_result = create_autospec(SampleValuesResult, instance=True)
        mock_sample_values_result.value = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
        )
        mock_core_metrics.sample_values_result = mock_sample_values_result

        return mock_core_metrics

    @pytest.fixture
    def sample_input(self) -> SqlExpectationInput:
        """Create sample input for testing."""
        return SqlExpectationInput(
            organization_id="test_org",
            workspace_id="test_workspace",
            user_prompt="Test user prompt",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
        )

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create a mock RunnableConfig."""
        return Mock(spec=RunnableConfig)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_success(
        self,
        mock_metric_service: Mock,
        mock_core_metrics: Mock,
        sample_input: SqlExpectationInput,
        mock_config: Mock,
    ) -> None:
        """Test successful call to SqlPlannerNode."""
        planner = SqlPlannerNode(metric_service=mock_metric_service)

        # Mock get_core_metrics to return our mock_core_metrics
        with patch.object(planner, "get_core_metrics", return_value=mock_core_metrics):
            result = await planner(state=sample_input, config=mock_config)

            assert isinstance(result, SqlExpectationState)
            assert result.organization_id == sample_input.organization_id
            assert result.user_prompt == sample_input.user_prompt
            assert result.data_source_name == sample_input.data_source_name
            assert result.data_asset_name == sample_input.data_asset_name
            assert result.batch_definition_name == sample_input.batch_definition_name
            assert result.batch_definition == mock_core_metrics.batch_definition
            assert len(result.messages) == 4
            assert isinstance(result.messages[0], SystemMessage)
            assert isinstance(result.messages[1], HumanMessage)
            assert isinstance(result.messages[2], HumanMessage)
            assert isinstance(result.messages[3], HumanMessage)


class TestSqlPlannerNodeUserPromptMessage:
    """Test SqlPlannerNode.user_prompt_message method."""

    @pytest.fixture
    def planner(self) -> SqlPlannerNode:
        """Create a SqlPlannerNode instance."""
        mock_metric_service = Mock(spec=MetricService)
        return SqlPlannerNode(metric_service=mock_metric_service)

    @pytest.fixture
    def sample_input(self) -> SqlExpectationInput:
        """Create sample input for testing."""
        return SqlExpectationInput(
            organization_id="test_org",
            workspace_id="test_workspace",
            user_prompt="Test user prompt for data quality",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
        )

    @pytest.mark.unit
    def test_user_prompt_message_format(
        self, planner: SqlPlannerNode, sample_input: SqlExpectationInput
    ) -> None:
        """Test that user_prompt_message formats the message correctly."""
        result = planner.user_prompt_message(sample_input)

        assert isinstance(result, HumanMessage)
        expected_content = "User Input: ```Test user prompt for data quality```\n\n"
        assert result.content == expected_content

    @pytest.mark.unit
    def test_user_prompt_message_with_special_characters(self, planner: SqlPlannerNode) -> None:
        """Test user_prompt_message with special characters in prompt."""
        input_with_special_chars = SqlExpectationInput(
            organization_id="test_org",
            workspace_id="test_workspace",
            user_prompt="Test with 'quotes' and \"double quotes\" and newlines\n",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
        )

        result = planner.user_prompt_message(input_with_special_chars)

        assert isinstance(result, HumanMessage)
        expected_content = (
            "User Input: ```Test with 'quotes' and \"double quotes\" and newlines\n```\n\n"
        )
        assert result.content == expected_content


class TestSqlPlannerNodeSchemaMessage:
    """Test SqlPlannerNode.schema_message method."""

    @pytest.fixture
    def planner(self) -> SqlPlannerNode:
        """Create a SqlPlannerNode instance."""
        mock_metric_service = Mock(spec=MetricService)
        return SqlPlannerNode(metric_service=mock_metric_service)

    @pytest.mark.unit
    def test_schema_message_with_successful_result(self, planner: SqlPlannerNode) -> None:
        """Test schema_message with successful BatchColumnTypesResult."""
        # Create a mock core_metrics object directly instead of using CoreMetrics constructor
        mock_core_metrics = Mock()
        mock_core_metrics.sql_dialect = "SQL dialect: postgresql"
        mock_core_metrics.table_name = "Table name: users"

        # Create a mock that will pass isinstance check for BatchColumnTypesResult
        mock_schema_result = create_autospec(BatchColumnTypesResult, instance=True)
        mock_schema_result.value = [
            MockColumn(name="id", type="INTEGER"),
            MockColumn(name="name", type="VARCHAR"),
            MockColumn(name="age", type="INTEGER"),
        ]
        mock_core_metrics.schema_result = mock_schema_result

        result = planner.schema_message(mock_core_metrics)

        assert isinstance(result, HumanMessage)
        expected_content = (
            "SQL dialect: postgresql\n"
            "Table name: users\n"
            "Table schema in CSV format with header:\n"
            "column_name,column_type\n"
            "id,INTEGER\n"
            "name,VARCHAR\n"
            "age,INTEGER"
        )
        assert result.content == expected_content

    @pytest.mark.unit
    def test_schema_message_with_error_result(self, planner: SqlPlannerNode) -> None:
        """Test schema_message with MetricErrorResult."""
        # Create a mock core_metrics object directly
        mock_core_metrics = Mock()
        mock_core_metrics.sql_dialect = "SQL dialect: mysql"
        mock_core_metrics.table_name = "Table name: products"

        mock_error_result = create_autospec(MetricErrorResult, instance=True)
        mock_error_result.value = Mock()
        mock_error_result.value.exception_message = "Database connection failed"
        mock_core_metrics.schema_result = mock_error_result

        result = planner.schema_message(mock_core_metrics)

        assert isinstance(result, HumanMessage)
        expected_content = (
            "SQL dialect: mysql\n"
            "Table name: products\n"
            "Could not compute column types: Database connection failed"
        )
        assert result.content == expected_content

    @pytest.mark.unit
    def test_schema_message_with_empty_columns(self, planner: SqlPlannerNode) -> None:
        """Test schema_message with empty column list."""
        # Create a mock core_metrics object directly
        mock_core_metrics = Mock()
        mock_core_metrics.sql_dialect = "SQL dialect: sqlite"
        mock_core_metrics.table_name = "Table name: empty_table"

        mock_schema_result = create_autospec(BatchColumnTypesResult, instance=True)
        mock_schema_result.value = []
        mock_core_metrics.schema_result = mock_schema_result

        result = planner.schema_message(mock_core_metrics)

        assert isinstance(result, HumanMessage)
        expected_content = (
            "SQL dialect: sqlite\n"
            "Table name: empty_table\n"
            "Table schema in CSV format with header:\n"
            "column_name,column_type\n"
        )
        assert result.content == expected_content


class TestSqlPlannerNodeSampleValuesMessage:
    """Test SqlPlannerNode.sample_values_message method."""

    @pytest.fixture
    def planner(self) -> SqlPlannerNode:
        """Create a SqlPlannerNode instance."""
        mock_metric_service = Mock(spec=MetricService)
        return SqlPlannerNode(metric_service=mock_metric_service)

    @pytest.mark.unit
    def test_sample_values_message_with_data(self, planner: SqlPlannerNode) -> None:
        """Test sample_values_message with successful SampleValuesResult containing data."""
        # Create a mock core_metrics object directly
        mock_core_metrics = Mock()

        mock_sample_values_result = create_autospec(SampleValuesResult, instance=True)
        mock_sample_values_result.value = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )
        mock_core_metrics.sample_values_result = mock_sample_values_result

        result = planner.sample_values_message(mock_core_metrics)

        assert isinstance(result, HumanMessage)
        expected_content = (
            "Table sample values in CSV format with header:\n\n"
            "id,name,age\n"
            "1,Alice,25\n"
            "2,Bob,30\n"
            "3,Charlie,35"
        )
        assert result.content == expected_content

    @pytest.mark.unit
    def test_sample_values_message_with_empty_data(self, planner: SqlPlannerNode) -> None:
        """Test sample_values_message with empty DataFrame."""
        # Create a mock core_metrics object directly
        mock_core_metrics = Mock()

        mock_sample_values_result = create_autospec(SampleValuesResult, instance=True)
        mock_sample_values_result.value = pd.DataFrame()
        mock_core_metrics.sample_values_result = mock_sample_values_result

        result = planner.sample_values_message(mock_core_metrics)

        assert isinstance(result, HumanMessage)
        expected_content = "Table sample values in CSV format with header:\n\n"
        assert result.content == expected_content

    @pytest.mark.unit
    def test_sample_values_message_with_error_result(self, planner: SqlPlannerNode) -> None:
        """Test sample_values_message with MetricErrorResult."""
        # Create a mock core_metrics object directly
        mock_core_metrics = Mock()

        mock_error_result = create_autospec(MetricErrorResult, instance=True)
        mock_error_result.value = Mock()
        mock_error_result.value.exception_message = "Query timeout"
        mock_core_metrics.sample_values_result = mock_error_result

        result = planner.sample_values_message(mock_core_metrics)

        assert isinstance(result, HumanMessage)
        expected_content = "Could not compute sample values: Query timeout"
        assert result.content == expected_content


class TestSqlPlannerNodeGetCoreMetrics:
    """Test SqlPlannerNode.get_core_metrics method."""

    @pytest.fixture
    def mock_metric_service(self) -> Mock:
        """Create a mock metric service."""
        return Mock(spec=MetricService)

    @pytest.fixture
    def planner(self, mock_metric_service: Mock) -> SqlPlannerNode:
        """Create a SqlPlannerNode instance."""
        return SqlPlannerNode(metric_service=mock_metric_service)

    @pytest.mark.unit
    def test_get_core_metrics_success(
        self, planner: SqlPlannerNode, mock_metric_service: Mock
    ) -> None:
        """Test successful get_core_metrics execution."""
        # Set up mocks
        mock_datasource = Mock(spec=SQLDatasource)
        mock_asset = Mock(spec=TableAsset)
        mock_asset.table_name = "test_table"

        # Create a more realistic BatchDefinition mock
        mock_batch_definition = Mock(spec=BatchDefinition[ColumnPartitioner])
        mock_batch_definition.id = "test_batch_def_id"
        mock_batch_definition.name = "test_batch_def"
        mock_batch_definition.data_asset_name = "test_asset"

        mock_batch = Mock()
        mock_execution_engine = Mock()
        mock_execution_engine.dialect.name = "postgresql"

        # Set up return values
        mock_metric_service.get_data_source.return_value = mock_datasource
        mock_datasource.get_asset.return_value = mock_asset
        mock_asset.get_batch_definition.return_value = mock_batch_definition
        mock_batch_definition.get_batch.return_value = mock_batch
        mock_datasource.get_execution_engine.return_value = mock_execution_engine

        # Mock compute_metrics results
        mock_schema_result = create_autospec(BatchColumnTypesResult, instance=True)

        mock_sample_values_result = create_autospec(SampleValuesResult, instance=True)

        mock_batch.compute_metrics.return_value = (mock_schema_result, mock_sample_values_result)

        result = planner.get_core_metrics(
            data_source_name="test_datasource",
            asset_name="test_asset",
            batch_definition_name="test_batch_def",
        )

        assert isinstance(result, CoreMetrics)
        # Note: Pydantic v1 transforms Mock objects during validation, so we focus on testing
        # that the method calls were made correctly and basic attributes are set
        assert result.batch_definition is not None
        assert result.sql_dialect == "SQL dialect: postgresql"
        assert result.table_name == "Table name: test_table"
        assert result.schema_result is not None
        assert result.sample_values_result is not None

        # Verify method calls
        mock_metric_service.get_data_source.assert_called_once_with("test_datasource")
        mock_datasource.get_asset.assert_called_once_with("test_asset")
        mock_asset.get_batch_definition.assert_called_once_with("test_batch_def")
        mock_batch_definition.get_batch.assert_called_once()
        mock_batch.compute_metrics.assert_called_once()

    @pytest.mark.unit
    def test_get_core_metrics_invalid_datasource_type(
        self, planner: SqlPlannerNode, mock_metric_service: Mock
    ) -> None:
        """Test get_core_metrics with invalid datasource type."""
        # Mock a non-SQL datasource
        mock_invalid_datasource = Mock()  # Not a SQLDatasource
        mock_metric_service.get_data_source.return_value = mock_invalid_datasource

        with pytest.raises(InvalidDataSourceTypeError) as exc_info:
            planner.get_core_metrics(
                data_source_name="test_datasource",
                asset_name="test_asset",
                batch_definition_name="test_batch_def",
            )

        assert "Invalid data source type" in str(exc_info.value)
        assert "SQLDatasource" in str(exc_info.value)

    @pytest.mark.unit
    def test_get_core_metrics_invalid_asset_type(
        self, planner: SqlPlannerNode, mock_metric_service: Mock
    ) -> None:
        """Test get_core_metrics with invalid asset type."""
        # Set up mocks
        mock_datasource = Mock(spec=SQLDatasource)
        mock_invalid_asset = Mock()  # Not a TableAsset

        mock_metric_service.get_data_source.return_value = mock_datasource
        mock_datasource.get_asset.return_value = mock_invalid_asset

        with pytest.raises(InvalidAssetTypeError) as exc_info:
            planner.get_core_metrics(
                data_source_name="test_datasource",
                asset_name="test_asset",
                batch_definition_name="test_batch_def",
            )

        assert "Invalid asset type" in str(exc_info.value)
        assert "TableAsset" in str(exc_info.value)

    @pytest.mark.unit
    def test_get_core_metrics_with_metric_computation(
        self, planner: SqlPlannerNode, mock_metric_service: Mock
    ) -> None:
        """Test that get_core_metrics calls compute_metrics with correct metric types."""
        # Set up mocks
        mock_datasource = Mock(spec=SQLDatasource)
        mock_asset = Mock(spec=TableAsset)
        mock_asset.table_name = "test_table"

        # Create a more realistic BatchDefinition mock
        mock_batch_definition = Mock(spec=BatchDefinition[ColumnPartitioner])
        mock_batch_definition.id = "test_batch_def_id"
        mock_batch_definition.name = "test_batch_def"
        mock_batch_definition.data_asset_name = "test_asset"

        mock_batch = Mock()
        mock_execution_engine = Mock()
        mock_execution_engine.dialect.name = "mysql"

        # Set up return values
        mock_metric_service.get_data_source.return_value = mock_datasource
        mock_datasource.get_asset.return_value = mock_asset
        mock_asset.get_batch_definition.return_value = mock_batch_definition
        mock_batch_definition.get_batch.return_value = mock_batch
        mock_datasource.get_execution_engine.return_value = mock_execution_engine

        # Mock compute_metrics results
        mock_schema_result = create_autospec(BatchColumnTypesResult, instance=True)

        mock_sample_values_result = create_autospec(SampleValuesResult, instance=True)

        mock_batch.compute_metrics.return_value = (mock_schema_result, mock_sample_values_result)

        planner.get_core_metrics(
            data_source_name="test_datasource",
            asset_name="test_asset",
            batch_definition_name="test_batch_def",
        )

        # Verify compute_metrics was called with correct metric types
        mock_batch.compute_metrics.assert_called_once()
        call_args = mock_batch.compute_metrics.call_args[0][0]
        assert len(call_args) == 2
        assert isinstance(call_args[0], BatchColumnTypes)
        assert isinstance(call_args[1], SampleValues)


class TestSqlPlannerNodeSystemMessage:
    """Test SqlPlannerNode system message constant."""

    @pytest.mark.unit
    def test_system_message_exists(self) -> None:
        """Test that the system message is defined and is a string."""
        assert hasattr(SqlPlannerNode, "SYSTEM_MESSAGE")
        assert isinstance(SqlPlannerNode.SYSTEM_MESSAGE, str)
        assert len(SqlPlannerNode.SYSTEM_MESSAGE.strip()) > 0

    @pytest.mark.unit
    def test_system_message_content(self) -> None:
        """Test that the system message has expected content."""
        message = SqlPlannerNode.SYSTEM_MESSAGE
        assert "data quality" in message.lower()
        assert "expert" in message.lower() or "knowledgeable" in message.lower()
