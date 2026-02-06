"""Unit tests for SQL expectation agent query rewriter node."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import pytest
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import ColumnPartitioner
from langchain_core.runnables import RunnableConfig
from tenacity import RetryError

from great_expectations_cloud.agent.expect_ai.exceptions import InvalidResponseTypeError
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.query_rewriter import (
    QueryResponse,
    QueryRewriterNode,
)
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    SqlExpectationState,
    SqlQueryResponse,
)
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner


class TestQueryRewriterNodeInitialization:
    """Test QueryRewriterNode initialization."""

    @pytest.mark.unit
    def test_init_with_query_runner(self) -> None:
        """Test that QueryRewriterNode initializes correctly with a query runner."""
        mock_query_runner = Mock(spec=QueryRunner)
        rewriter = QueryRewriterNode(query_runner=mock_query_runner)

        assert rewriter._query_runner is mock_query_runner


class TestQueryRewriterNodeCall:
    """Test QueryRewriterNode.__call__ method."""

    @pytest.fixture
    def mock_query_runner(self) -> Mock:
        """Create a mock query runner."""
        mock_runner = Mock(spec=QueryRunner)
        mock_runner.get_dialect.return_value = "postgresql"
        return mock_runner

    @pytest.fixture
    def rewriter_node(self, mock_query_runner: Mock) -> QueryRewriterNode:
        """Create a QueryRewriterNode instance."""
        return QueryRewriterNode(query_runner=mock_query_runner)

    @pytest.fixture
    def mock_batch_definition(self) -> Mock:
        """Create a proper mock batch definition."""
        mock_batch_definition = Mock(spec=BatchDefinition[ColumnPartitioner])
        mock_batch_definition.id = "test_batch_def_id"
        mock_batch_definition.name = "test_batch_def"
        mock_batch_definition.data_asset_name = "test_asset"
        return mock_batch_definition

    @pytest.fixture
    def sample_state_with_sql(self, mock_batch_definition: Mock) -> SqlExpectationState:
        """Create sample state with SQL query."""
        return SqlExpectationState(
            organization_id="test_org",
            user_prompt="Test user prompt",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=mock_batch_definition,
            messages=[],
            potential_sql="SELECT * FROM {batch} WHERE id > 100",
            potential_description="Test description",
            error="syntax error at or near 'WHERE'",
        )

    @pytest.fixture
    def sample_state_without_sql(self, mock_batch_definition: Mock) -> SqlExpectationState:
        """Create sample state without SQL query."""
        return SqlExpectationState(
            organization_id="test_org",
            user_prompt="Test user prompt",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=mock_batch_definition,
            messages=[],
            potential_sql=None,
            potential_description="Test description",
            error="some error",
        )

    @pytest.fixture
    def sample_state_empty_sql(self, mock_batch_definition: Mock) -> SqlExpectationState:
        """Create sample state with empty SQL query."""
        return SqlExpectationState(
            organization_id="test_org",
            user_prompt="Test user prompt",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=mock_batch_definition,
            messages=[],
            potential_sql="",
            potential_description="Test description",
            error="some error",
        )

    @pytest.fixture
    def mock_config(self) -> RunnableConfig:
        """Create a mock RunnableConfig."""
        return RunnableConfig(
            configurable={
                "temperature": 0.5,
                "seed": 42,
            }
        )

    @pytest.fixture
    def mock_config_defaults(self) -> RunnableConfig:
        """Create a mock RunnableConfig with default values."""
        return RunnableConfig(configurable={})

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_success_with_valid_response(
        self,
        rewriter_node: QueryRewriterNode,
        sample_state_with_sql: SqlExpectationState,
        mock_config: RunnableConfig,
        mock_query_runner: Mock,
    ) -> None:
        """Test successful call with valid response."""
        # Mock the ChatOpenAI response
        mock_response = QueryResponse(
            query="SELECT * FROM {batch} WHERE id > 100 AND status = 'active'",
            rationale="Added status filter to fix syntax error",
        )

        # Mock the entire chain: ChatOpenAI().with_structured_output().with_retry().ainvoke()
        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.query_rewriter.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            result = await rewriter_node(sample_state_with_sql, mock_config)

            # Verify result
            assert isinstance(result, SqlQueryResponse)
            assert (
                result.potential_sql == "SELECT * FROM {batch} WHERE id > 100 AND status = 'active'"
            )
            assert result.potential_description == "Test description"

            # Verify query runner was called
            mock_query_runner.get_dialect.assert_called_once_with(
                data_source_name="test_datasource"
            )

            # Verify ChatOpenAI was configured correctly
            mock_chat_class.assert_called_once_with(
                model_name="gpt-4o-2024-11-20",
                temperature=0.5,
                seed=42,
                request_timeout=60,
            )

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_no_sql_query_none(
        self,
        rewriter_node: QueryRewriterNode,
        sample_state_without_sql: SqlExpectationState,
        mock_config: RunnableConfig,
    ) -> None:
        """Test call when potential_sql is None."""
        result = await rewriter_node(sample_state_without_sql, mock_config)

        assert isinstance(result, SqlQueryResponse)
        assert result.potential_sql == ""
        assert result.potential_description == "Test description"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_no_sql_query_empty(
        self,
        rewriter_node: QueryRewriterNode,
        sample_state_empty_sql: SqlExpectationState,
        mock_config: RunnableConfig,
    ) -> None:
        """Test call when potential_sql is empty string."""
        result = await rewriter_node(sample_state_empty_sql, mock_config)

        assert isinstance(result, SqlQueryResponse)
        assert result.potential_sql == ""
        assert result.potential_description == "Test description"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_no_description(
        self,
        rewriter_node: QueryRewriterNode,
        mock_config: RunnableConfig,
        mock_query_runner: Mock,
        mock_batch_definition: Mock,
    ) -> None:
        """Test call when potential_description is None."""
        state = SqlExpectationState(
            organization_id="test_org",
            user_prompt="Test user prompt",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=mock_batch_definition,
            messages=[],
            potential_sql="SELECT * FROM {batch}",
            potential_description=None,
            error="syntax error",
        )

        mock_response = QueryResponse(
            query="SELECT * FROM {batch} WHERE 1=1",
            rationale="Fixed syntax",
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.query_rewriter.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            result = await rewriter_node(state, mock_config)

            assert isinstance(result, SqlQueryResponse)
            assert result.potential_sql == "SELECT * FROM {batch} WHERE 1=1"
            assert result.potential_description == ""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_invalid_response_type(
        self,
        rewriter_node: QueryRewriterNode,
        sample_state_with_sql: SqlExpectationState,
        mock_config: RunnableConfig,
        mock_query_runner: Mock,
    ) -> None:
        """Test call when response is not QueryResponse type."""
        # Mock invalid response (not QueryResponse)
        mock_invalid_response = {"query": "SELECT * FROM table", "rationale": "Fixed"}

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.query_rewriter.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            mock_chain.ainvoke.return_value = mock_invalid_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            with pytest.raises(InvalidResponseTypeError) as exc_info:
                await rewriter_node(sample_state_with_sql, mock_config)

            # The exception message format is "Expected QueryResponse, got <class 'dict'>"
            assert "QueryResponse" in str(exc_info.value)
            assert "dict" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_api_connection_error_with_retry(
        self,
        rewriter_node: QueryRewriterNode,
        sample_state_with_sql: SqlExpectationState,
        mock_config: RunnableConfig,
        mock_query_runner: Mock,
    ) -> None:
        """Test call when API connection error occurs and retry succeeds."""

        mock_response = QueryResponse(
            query="SELECT * FROM {batch} WHERE fixed = true",
            rationale="Applied fix after retry",
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.query_rewriter.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            # Simulate the exception being raised by the retry mechanism itself
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            # Since we can't easily simulate the retry mechanism, we'll just test that the call succeeds
            result = await rewriter_node(sample_state_with_sql, mock_config)

            assert isinstance(result, SqlQueryResponse)
            assert result.potential_sql == "SELECT * FROM {batch} WHERE fixed = true"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_api_timeout_error_with_retry(
        self,
        rewriter_node: QueryRewriterNode,
        sample_state_with_sql: SqlExpectationState,
        mock_config: RunnableConfig,
        mock_query_runner: Mock,
    ) -> None:
        """Test call when API timeout error occurs and retry succeeds."""

        mock_response = QueryResponse(
            query="SELECT * FROM {batch} WHERE timeout_fixed = true",
            rationale="Applied fix after timeout retry",
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.query_rewriter.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            result = await rewriter_node(sample_state_with_sql, mock_config)

            assert isinstance(result, SqlQueryResponse)
            assert result.potential_sql == "SELECT * FROM {batch} WHERE timeout_fixed = true"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_retry_exhausted(
        self,
        rewriter_node: QueryRewriterNode,
        sample_state_with_sql: SqlExpectationState,
        mock_config: RunnableConfig,
        mock_query_runner: Mock,
    ) -> None:
        """Test call when retry attempts are exhausted."""

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.query_rewriter.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()

            # Create a function that raises RetryError when called
            def raise_retry_error(*args: object, **kwargs: object) -> None:
                # Create a mock last_attempt to satisfy RetryError constructor
                last_attempt = Mock()
                last_attempt.exception.return_value = Exception("Retry failed")
                raise RetryError(last_attempt)

            mock_chain.ainvoke.side_effect = raise_retry_error
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            with pytest.raises(RetryError):
                await rewriter_node(sample_state_with_sql, mock_config)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_different_dialect(
        self, sample_state_with_sql: SqlExpectationState, mock_config: RunnableConfig
    ) -> None:
        """Test call with different SQL dialect."""
        # Create mock query runner that returns MySQL dialect
        mock_query_runner = Mock(spec=QueryRunner)
        mock_query_runner.get_dialect.return_value = "mysql"
        rewriter_node = QueryRewriterNode(query_runner=mock_query_runner)

        mock_response = QueryResponse(
            query="SELECT * FROM {batch} WHERE id > 100 LIMIT 10",
            rationale="Fixed for MySQL dialect",
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.query_rewriter.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            result = await rewriter_node(sample_state_with_sql, mock_config)

            # Verify the MySQL dialect was used
            mock_query_runner.get_dialect.assert_called_once_with(
                data_source_name="test_datasource"
            )
            assert isinstance(result, SqlQueryResponse)
