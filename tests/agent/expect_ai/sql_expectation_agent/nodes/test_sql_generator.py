"""Unit tests for SQL expectation agent SQL generator node."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import pytest
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import ColumnPartitioner
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from openai import APIConnectionError, APITimeoutError
from pydantic import ValidationError

from great_expectations_cloud.agent.expect_ai.exceptions import InvalidResponseTypeError
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.sql_generator import (
    SqlGeneratorNode,
)
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    SqlAndDescriptionResponse,
    SqlExpectationState,
    SqlQueryResponse,
)
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner


class TestSqlGeneratorNodeInitialization:
    """Test SqlGeneratorNode initialization."""

    @pytest.mark.unit
    def test_init_with_query_runner(self) -> None:
        """Test that SqlGeneratorNode initializes correctly with a query runner."""
        mock_query_runner = Mock(spec=QueryRunner)
        generator = SqlGeneratorNode(query_runner=mock_query_runner)

        assert generator._query_runner is mock_query_runner


class TestSqlGeneratorNodeCall:
    """Test SqlGeneratorNode.__call__ method."""

    @pytest.fixture
    def mock_query_runner(self) -> Mock:
        """Create a mock query runner."""
        mock_runner = Mock(spec=QueryRunner)
        mock_runner.get_dialect.return_value = "postgresql"
        return mock_runner

    @pytest.fixture
    def generator_node(self, mock_query_runner: Mock) -> SqlGeneratorNode:
        """Create a SqlGeneratorNode instance."""
        return SqlGeneratorNode(query_runner=mock_query_runner)

    @pytest.fixture
    def mock_batch_definition(self) -> Mock:
        """Create a proper mock batch definition."""
        mock_batch_definition = Mock(spec=BatchDefinition[ColumnPartitioner])
        mock_batch_definition.id = "test_batch_def_id"
        mock_batch_definition.name = "test_batch_def"
        mock_batch_definition.data_asset_name = "test_asset"
        return mock_batch_definition

    @pytest.fixture
    def sample_state(self, mock_batch_definition: Mock) -> SqlExpectationState:
        """Create sample state for testing."""
        return SqlExpectationState(
            organization_id="test_org",
            user_prompt="Every customer must have an email address",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=mock_batch_definition,
            messages=[HumanMessage(content="Previous conversation message")],
        )

    @pytest.fixture
    def sample_state_empty_messages(self, mock_batch_definition: Mock) -> SqlExpectationState:
        """Create sample state with empty messages."""
        return SqlExpectationState(
            organization_id="test_org",
            user_prompt="All products must have a price",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=mock_batch_definition,
            messages=[],
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
        generator_node: SqlGeneratorNode,
        sample_state: SqlExpectationState,
        mock_config: RunnableConfig,
        mock_query_runner: Mock,
    ) -> None:
        """Test successful call with valid response."""
        # Mock the response from call_model
        mock_response = SqlAndDescriptionResponse(
            sql="SELECT customer_id, customer_name FROM {batch} WHERE email IS NULL OR email = ''",
            description="Expect all customers to have an email address",
        )

        # Mock call_model method
        with patch.object(
            generator_node, "call_model", new_callable=AsyncMock, return_value=mock_response
        ) as mock_call_model:
            result = await generator_node(sample_state, mock_config)

            # Verify result
            assert isinstance(result, SqlQueryResponse)
            assert (
                result.potential_sql
                == "SELECT customer_id, customer_name FROM {batch} WHERE email IS NULL OR email = ''"
            )
            assert result.potential_description == "Expect all customers to have an email address"

            # Verify query runner was called to get dialect
            mock_query_runner.get_dialect.assert_called_once_with(
                data_source_name="test_datasource"
            )

            # Verify call_model was called with correct arguments
            mock_call_model.assert_called_once()
            call_args = mock_call_model.call_args
            config_arg = call_args[1]["config"]
            messages_arg = call_args[1]["messages"]

            assert config_arg == mock_config
            assert len(messages_arg) == 4  # system + example + state.messages + task
            assert isinstance(messages_arg[0], SystemMessage)
            assert "postgresql" in messages_arg[0].content
            assert isinstance(messages_arg[1], HumanMessage)
            assert "Example:" in messages_arg[1].content
            assert messages_arg[2] == sample_state.messages[0]
            assert isinstance(messages_arg[3], HumanMessage)
            assert "conversation history" in messages_arg[3].content

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_with_empty_messages(
        self,
        generator_node: SqlGeneratorNode,
        sample_state_empty_messages: SqlExpectationState,
        mock_config: RunnableConfig,
        mock_query_runner: Mock,
    ) -> None:
        """Test call with empty messages list."""
        mock_response = SqlAndDescriptionResponse(
            sql="SELECT product_id FROM {batch} WHERE price IS NULL",
            description="Expect all products to have a price",
        )

        with patch.object(
            generator_node, "call_model", new_callable=AsyncMock, return_value=mock_response
        ) as mock_call_model:
            result = await generator_node(sample_state_empty_messages, mock_config)

            assert isinstance(result, SqlQueryResponse)
            assert result.potential_sql == "SELECT product_id FROM {batch} WHERE price IS NULL"
            assert result.potential_description == "Expect all products to have a price"

            # Verify messages structure with empty state messages
            call_args = mock_call_model.call_args
            messages_arg = call_args[1]["messages"]
            assert len(messages_arg) == 3  # system + example + task (no state messages)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_config_defaults(
        self,
        generator_node: SqlGeneratorNode,
        sample_state: SqlExpectationState,
        mock_config_defaults: RunnableConfig,
    ) -> None:
        """Test call with default config values."""
        mock_response = SqlAndDescriptionResponse(
            sql="SELECT * FROM {batch} WHERE status IS NULL",
            description="Expect all records to have status",
        )

        with patch.object(
            generator_node, "call_model", new_callable=AsyncMock, return_value=mock_response
        ) as mock_call_model:
            result = await generator_node(sample_state, mock_config_defaults)

            assert isinstance(result, SqlQueryResponse)

            # Verify call_model was called with default config
            mock_call_model.assert_called_once()
            call_args = mock_call_model.call_args
            config_arg = call_args[1]["config"]
            assert config_arg == mock_config_defaults

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_model_exception_propagation(
        self,
        generator_node: SqlGeneratorNode,
        sample_state: SqlExpectationState,
        mock_config: RunnableConfig,
    ) -> None:
        """Test that exceptions from call_model are propagated."""
        with patch.object(
            generator_node,
            "call_model",
            new_callable=AsyncMock,
            side_effect=InvalidResponseTypeError(str, SqlAndDescriptionResponse),
        ):
            with pytest.raises(InvalidResponseTypeError):
                await generator_node(sample_state, mock_config)


class TestSqlGeneratorNodeCallModel:
    """Test SqlGeneratorNode.call_model method."""

    @pytest.fixture
    def mock_query_runner(self) -> Mock:
        """Create a mock query runner."""
        return Mock(spec=QueryRunner)

    @pytest.fixture
    def generator_node(self, mock_query_runner: Mock) -> SqlGeneratorNode:
        """Create a SqlGeneratorNode instance."""
        return SqlGeneratorNode(query_runner=mock_query_runner)

    @pytest.fixture
    def sample_messages(self) -> list[BaseMessage]:
        """Create sample messages for testing."""
        return [
            SystemMessage(content="You are a SQL assistant"),
            HumanMessage(content="Generate SQL for validation"),
        ]

    @pytest.fixture
    def mock_config(self) -> RunnableConfig:
        """Create a mock RunnableConfig."""
        return RunnableConfig(
            configurable={
                "temperature": 0.7,
                "seed": 123,
            }
        )

    @pytest.fixture
    def mock_config_defaults(self) -> RunnableConfig:
        """Create a mock RunnableConfig with default values."""
        return RunnableConfig(configurable={})

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_model_success(
        self,
        generator_node: SqlGeneratorNode,
        sample_messages: list[BaseMessage],
        mock_config: RunnableConfig,
    ) -> None:
        """Test successful call_model execution."""
        mock_response = SqlAndDescriptionResponse(
            sql="SELECT * FROM {batch} WHERE condition = true",
            description="Expect condition to be true",
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.sql_generator.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            result = await generator_node.call_model(config=mock_config, messages=sample_messages)

            assert isinstance(result, SqlAndDescriptionResponse)
            assert result.sql == "SELECT * FROM {batch} WHERE condition = true"
            assert result.description == "Expect condition to be true"

            # Verify ChatOpenAI was configured correctly
            mock_chat_class.assert_called_once_with(
                model_name="gpt-4o-2024-11-20",
                temperature=0.7,
                seed=123,
                request_timeout=60,
            )

            # Verify structured output configuration
            mock_chat_class.return_value.with_structured_output.assert_called_once_with(
                schema=SqlAndDescriptionResponse, method="json_schema", strict=True
            )

            # Verify retry configuration
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.assert_called_once_with(
                retry_if_exception_type=(APIConnectionError, APITimeoutError),
                stop_after_attempt=2,
            )

            # Verify model invocation
            mock_chain.ainvoke.assert_called_once_with(sample_messages)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_model_with_defaults(
        self,
        generator_node: SqlGeneratorNode,
        sample_messages: list[BaseMessage],
        mock_config_defaults: RunnableConfig,
    ) -> None:
        """Test call_model with default configuration values."""
        mock_response = SqlAndDescriptionResponse(
            sql="SELECT id FROM {batch}",
            description="Expect valid IDs",
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.sql_generator.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            result = await generator_node.call_model(
                config=mock_config_defaults, messages=sample_messages
            )

            assert isinstance(result, SqlAndDescriptionResponse)

            # Verify default values were used
            mock_chat_class.assert_called_once_with(
                model_name="gpt-4o-2024-11-20",
                temperature=0.7,  # default
                seed=None,  # default
                request_timeout=60,
            )

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_model_invalid_response_type(
        self,
        generator_node: SqlGeneratorNode,
        sample_messages: list[BaseMessage],
        mock_config: RunnableConfig,
    ) -> None:
        """Test call_model raises InvalidResponseTypeError for wrong response type."""
        # Mock response that's not SqlAndDescriptionResponse
        mock_invalid_response = "invalid response"

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.sql_generator.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            mock_chain.ainvoke.return_value = mock_invalid_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            with pytest.raises(InvalidResponseTypeError) as exc_info:
                await generator_node.call_model(config=mock_config, messages=sample_messages)

            # The error message should contain the types
            error_message = str(exc_info.value)
            assert "SqlAndDescriptionResponse" in error_message
            assert "str" in error_message

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_model_api_connection_error_retry(
        self,
        generator_node: SqlGeneratorNode,
        sample_messages: list[BaseMessage],
        mock_config: RunnableConfig,
    ) -> None:
        """Test call_model handles APIConnectionError with retry."""
        mock_response = SqlAndDescriptionResponse(
            sql="SELECT * FROM {batch}",
            description="Expect valid data",
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.sql_generator.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            # Mock a successful response after retry
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            result = await generator_node.call_model(config=mock_config, messages=sample_messages)

            assert isinstance(result, SqlAndDescriptionResponse)
            # The with_retry should handle the retry logic internally

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_model_api_timeout_error_retry(
        self,
        generator_node: SqlGeneratorNode,
        sample_messages: list[BaseMessage],
        mock_config: RunnableConfig,
    ) -> None:
        """Test call_model handles APITimeoutError with retry."""
        mock_response = SqlAndDescriptionResponse(
            sql="SELECT * FROM {batch}",
            description="Expect valid data",
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes.sql_generator.ChatOpenAI"
        ) as mock_chat_class:
            mock_chain = AsyncMock()
            # Mock a successful response after retry
            mock_chain.ainvoke.return_value = mock_response
            mock_chat_class.return_value.with_structured_output.return_value.with_retry.return_value = mock_chain

            result = await generator_node.call_model(config=mock_config, messages=sample_messages)

            assert isinstance(result, SqlAndDescriptionResponse)


class TestSqlAndDescriptionResponseModel:
    """Test SqlAndDescriptionResponse Pydantic model."""

    @pytest.mark.unit
    def test_sql_and_description_response_valid(self) -> None:
        """Test SqlAndDescriptionResponse with valid data."""
        response = SqlAndDescriptionResponse(
            sql="SELECT * FROM table WHERE condition = 1", description="Expect condition to equal 1"
        )

        assert response.sql == "SELECT * FROM table WHERE condition = 1"
        assert response.description == "Expect condition to equal 1"

    @pytest.mark.unit
    def test_sql_and_description_response_missing_fields(self) -> None:
        """Test SqlAndDescriptionResponse with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            SqlAndDescriptionResponse(sql="SELECT * FROM table")  # type: ignore[call-arg]  # missing description

        assert "description" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            SqlAndDescriptionResponse(description="Some description")  # type: ignore[call-arg]  # missing sql

        assert "sql" in str(exc_info.value)

    @pytest.mark.unit
    def test_sql_and_description_response_empty_strings(self) -> None:
        """Test SqlAndDescriptionResponse allows empty strings."""
        response = SqlAndDescriptionResponse(sql="", description="")

        assert response.sql == ""
        assert response.description == ""


class TestSqlQueryResponseModel:
    """Test SqlQueryResponse Pydantic model."""

    @pytest.mark.unit
    def test_sql_query_response_valid(self) -> None:
        """Test SqlQueryResponse with valid data."""
        response = SqlQueryResponse(
            potential_sql="SELECT * FROM table", potential_description="Test description"
        )

        assert response.potential_sql == "SELECT * FROM table"
        assert response.potential_description == "Test description"

    @pytest.mark.unit
    def test_sql_query_response_missing_fields(self) -> None:
        """Test SqlQueryResponse with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            SqlQueryResponse(potential_sql="SELECT * FROM table")  # type: ignore[call-arg]  # missing potential_description

        assert "potential_description" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            SqlQueryResponse(potential_description="Some description")  # type: ignore[call-arg]  # missing potential_sql

        assert "potential_sql" in str(exc_info.value)
