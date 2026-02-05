"""Unit tests for SQL expectation agent SQL validator node."""

from __future__ import annotations

from unittest.mock import Mock, create_autospec

import pytest
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import ColumnPartitioner
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig

from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.sql_validator import (
    MAX_SQL_REWRITE_ATTEMPTS,
    SqlValidatorNode,
)
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    SqlExpectationState,
)
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner


class TestSqlValidatorNodeInitialization:
    """Test SqlValidatorNode initialization."""

    @pytest.mark.unit
    def test_init_with_query_runner(self) -> None:
        """Test that SqlValidatorNode initializes correctly with a query runner."""
        mock_query_runner = Mock(spec=QueryRunner)
        mock_metric_service = Mock(spec=MetricService)
        validator = SqlValidatorNode(
            query_runner=mock_query_runner, metric_service=mock_metric_service
        )

        assert validator._query_runner is mock_query_runner
        assert validator._metric_service is mock_metric_service


class TestSqlValidatorNodeCall:
    """Test SqlValidatorNode.__call__ method."""

    @pytest.fixture
    def mock_query_runner(self) -> Mock:
        """Create a mock query runner."""
        return Mock(spec=QueryRunner)

    @pytest.fixture
    def mock_metric_service(self) -> Mock:
        """Create a mock metric service."""
        mock_metric_service = Mock(spec=MetricService)
        mock_datasource = Mock()
        mock_asset = Mock()
        mock_asset.table_name = "test_asset"
        mock_datasource.get_asset.return_value = mock_asset
        mock_metric_service.get_data_source.return_value = mock_datasource
        return mock_metric_service

    @pytest.fixture
    def validator(self, mock_query_runner: Mock, mock_metric_service: Mock) -> SqlValidatorNode:
        """Create a SqlValidatorNode instance."""
        return SqlValidatorNode(query_runner=mock_query_runner, metric_service=mock_metric_service)

    @pytest.fixture
    def mock_batch_definition(self) -> Mock:
        """Create a mock batch definition."""
        batch_def = create_autospec(BatchDefinition[ColumnPartitioner], instance=True)
        batch_def.id = "test_batch_def_id"
        batch_def.name = "test_batch_def"
        batch_def.data_asset_name = "test_asset"
        return batch_def  # type: ignore[no-any-return]

    @pytest.fixture
    def base_state(self, mock_batch_definition: Mock) -> SqlExpectationState:
        """Create a base state for testing."""
        return SqlExpectationState(
            organization_id="test_org",
            user_prompt="Test user prompt",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=mock_batch_definition,
            messages=[HumanMessage(content="test message")],
            potential_sql="SELECT * FROM {batch} WHERE value IS NULL",
            sql_validation_attempts=0,
        )

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create a mock RunnableConfig."""
        return Mock(spec=RunnableConfig)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_sql_validation_success(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test successful SQL validation."""
        # Setup mock to return successful compilation
        mock_query_runner.check_query_compiles.return_value = (True, None)

        result = await validator(state=base_state, config=mock_config)

        # Verify the query runner was called with correct parameters
        expected_sql = "SELECT * FROM test_asset WHERE value IS NULL"
        mock_query_runner.check_query_compiles.assert_called_once_with(
            data_source_name="test_datasource",
            query_text=expected_sql,
        )

        # Verify the result
        assert result["success"] is True
        assert result["sql_validation_attempts"] == 1
        assert result["error"] is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_sql_validation_failure(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test SQL validation failure."""
        # Setup mock to return compilation failure
        error_message = "Syntax error in SQL query"
        mock_query_runner.check_query_compiles.return_value = (False, error_message)

        result = await validator(state=base_state, config=mock_config)

        # Verify the query runner was called with correct parameters
        expected_sql = "SELECT * FROM test_asset WHERE value IS NULL"
        mock_query_runner.check_query_compiles.assert_called_once_with(
            data_source_name="test_datasource",
            query_text=expected_sql,
        )

        # Verify the result
        assert result["success"] is False
        assert result["sql_validation_attempts"] == 1
        assert result["error"] == error_message

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_no_sql_to_validate(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test when there's no SQL to validate."""
        # Set potential_sql to None
        base_state.potential_sql = None

        result = await validator(state=base_state, config=mock_config)

        # Verify the query runner was not called
        mock_query_runner.check_query_compiles.assert_not_called()

        # Verify the result
        assert result["success"] is True
        assert result["sql_validation_attempts"] == 1
        assert result["error"] == "No SQL generated to validate"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_empty_sql_to_validate(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test when potential_sql is an empty string."""
        # Set potential_sql to empty string
        base_state.potential_sql = ""

        result = await validator(state=base_state, config=mock_config)

        # Verify the query runner was not called
        mock_query_runner.check_query_compiles.assert_not_called()

        # Verify the result
        assert result["success"] is True
        assert result["sql_validation_attempts"] == 1
        assert result["error"] == "No SQL generated to validate"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_max_attempts_exceeded(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test when maximum SQL rewrite attempts are exceeded."""
        # Set attempts to maximum
        base_state.sql_validation_attempts = MAX_SQL_REWRITE_ATTEMPTS

        result = await validator(state=base_state, config=mock_config)

        # Verify the query runner was not called
        mock_query_runner.check_query_compiles.assert_not_called()

        # Verify the result
        assert result["success"] is True
        assert result["sql_validation_attempts"] == MAX_SQL_REWRITE_ATTEMPTS + 1
        assert (
            result["error"] == f"Query failed to compile after {MAX_SQL_REWRITE_ATTEMPTS} attempts."
        )

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_batch_replacement_in_sql(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test that {batch} placeholder is correctly replaced with data asset name."""
        # Use SQL with multiple {batch} placeholders
        base_state.potential_sql = (
            "SELECT * FROM {batch} WHERE {batch}.id IN (SELECT id FROM {batch})"
        )

        # Setup mock to return successful compilation
        mock_query_runner.check_query_compiles.return_value = (True, None)

        result = await validator(state=base_state, config=mock_config)

        # Verify the {batch} was replaced with data_asset_name
        expected_sql = "SELECT * FROM test_asset WHERE test_asset.id IN (SELECT id FROM test_asset)"
        mock_query_runner.check_query_compiles.assert_called_once_with(
            data_source_name="test_datasource",
            query_text=expected_sql,
        )

        assert result["success"] is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_increments_attempts_on_success(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test that attempts counter is incremented on success."""
        # Start with some attempts
        base_state.sql_validation_attempts = 1

        # Setup mock to return successful compilation
        mock_query_runner.check_query_compiles.return_value = (True, None)

        result = await validator(state=base_state, config=mock_config)

        # Verify attempts were incremented
        assert result["sql_validation_attempts"] == 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_increments_attempts_on_failure(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test that attempts counter is incremented on failure."""
        # Start with some attempts
        base_state.sql_validation_attempts = 1

        # Setup mock to return compilation failure
        mock_query_runner.check_query_compiles.return_value = (False, "Some error")

        result = await validator(state=base_state, config=mock_config)

        # Verify attempts were incremented
        assert result["sql_validation_attempts"] == 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_query_runner_exception_handling(
        self,
        validator: SqlValidatorNode,
        mock_query_runner: Mock,
        base_state: SqlExpectationState,
        mock_config: Mock,
    ) -> None:
        """Test handling when query runner raises an exception."""
        # Setup mock to raise an exception
        mock_query_runner.check_query_compiles.side_effect = Exception("Database connection error")

        # The validator should let the exception propagate
        with pytest.raises(Exception, match="Database connection error"):
            await validator(state=base_state, config=mock_config)


class TestSqlValidatorNodeConstants:
    """Test constants used in SqlValidatorNode."""

    @pytest.mark.unit
    def test_max_sql_rewrite_attempts_constant(self) -> None:
        """Test that MAX_SQL_REWRITE_ATTEMPTS is correctly defined."""
        assert MAX_SQL_REWRITE_ATTEMPTS == 3
        assert isinstance(MAX_SQL_REWRITE_ATTEMPTS, int)
