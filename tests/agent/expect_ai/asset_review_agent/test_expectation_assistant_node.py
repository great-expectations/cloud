from __future__ import annotations

from typing import Any, cast
from unittest.mock import Mock, create_autospec, patch

import pytest
from great_expectations.core.batch_definition import BatchDefinition
from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from openai import APIConnectionError, APITimeoutError

from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import (
    ExpectationAssistantNode,
)
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import EchoesState
from great_expectations_cloud.agent.expect_ai.tools.metrics import AgentToolsManager


class TestExpectationAssistantNodeInitialization:
    @pytest.mark.unit
    def test_init_with_tools_manager(self) -> None:
        mock_tools_manager = Mock(spec=AgentToolsManager)
        node = ExpectationAssistantNode(tools_manager=mock_tools_manager)

        assert node._tools_manager is mock_tools_manager


class TestExpectationAssistantNodeCall:
    @pytest.fixture
    def mock_tools_manager(self) -> Mock:
        mock = Mock(spec=AgentToolsManager)
        mock.get_tools.return_value = []
        return mock

    @pytest.fixture
    def expectation_assistant_node(self, mock_tools_manager: Mock) -> ExpectationAssistantNode:
        return ExpectationAssistantNode(tools_manager=mock_tools_manager)

    @pytest.fixture
    def sample_state(self) -> EchoesState:
        batch_definition = create_autospec(BatchDefinition, instance=True)
        return EchoesState(
            organization_id="test_org",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=batch_definition,
            messages=[],
            potential_expectations=[],
            expectations=[],
        )

    @pytest.fixture
    def mock_config(self) -> RunnableConfig:
        return RunnableConfig(
            configurable={
                "temperature": 0.5,
                "seed": 42,
            }
        )

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_success(
        self,
        expectation_assistant_node: ExpectationAssistantNode,
        sample_state: EchoesState,
        mock_config: RunnableConfig,
        mock_tools_manager: Mock,
    ) -> None:
        mock_response = AIMessage(content="Test response", tool_calls=[])

        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat_class:
            mock_model = Mock()
            mock_model.bind_tools.return_value = mock_model
            mock_model.with_retry.return_value = mock_model
            mock_model.invoke = Mock(return_value=mock_response)
            mock_chat_class.return_value = mock_model

            result = await expectation_assistant_node(sample_state, mock_config)

            mock_chat_class.assert_called_once_with(
                model_name="gpt-4o-2024-11-20",
                temperature=0.5,  # From config
                seed=42,  # From config
                request_timeout=120,
            )

            mock_model.bind_tools.assert_called_once()

            mock_model.with_retry.assert_called_once()
            retry_kwargs = mock_model.with_retry.call_args[1]
            assert retry_kwargs["retry_if_exception_type"] == (APIConnectionError, APITimeoutError)
            assert retry_kwargs["stop_after_attempt"] == 2

            assert len(result.messages) == 1
            assert result.messages[0] == mock_response

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_with_system_message(
        self,
        expectation_assistant_node: ExpectationAssistantNode,
        sample_state: EchoesState,
        mock_config: RunnableConfig,
    ) -> None:
        mock_response = AIMessage(content="Test response", tool_calls=[])

        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat_class:
            mock_model = Mock()
            mock_model.bind_tools.return_value = mock_model
            mock_model.with_retry.return_value = mock_model
            mock_model.invoke = Mock(return_value=mock_response)
            mock_chat_class.return_value = mock_model

            await expectation_assistant_node(sample_state, mock_config)

            invoke_args = mock_model.invoke.call_args[0][0]
            assert len(invoke_args) == 1  # System message + empty state messages
            assert isinstance(invoke_args[0], SystemMessage)
            assert "Data Quality Assistant" in invoke_args[0].content

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_with_default_temperature(
        self,
        expectation_assistant_node: ExpectationAssistantNode,
        sample_state: EchoesState,
    ) -> None:
        mock_config = RunnableConfig(configurable={})
        mock_response = AIMessage(content="Test response", tool_calls=[])

        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat_class:
            mock_model = Mock()
            mock_model.bind_tools.return_value = mock_model
            mock_model.with_retry.return_value = mock_model
            mock_model.invoke = Mock(return_value=mock_response)
            mock_chat_class.return_value = mock_model

            await expectation_assistant_node(sample_state, mock_config)

            mock_chat_class.assert_called_once_with(
                model_name="gpt-4o-2024-11-20",
                temperature=0.2,  # Default
                seed=None,  # Default
                request_timeout=120,
            )

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_uses_retry_wrapper(
        self,
        expectation_assistant_node: ExpectationAssistantNode,
        sample_state: EchoesState,
        mock_config: RunnableConfig,
    ) -> None:
        mock_response = AIMessage(content="Test response", tool_calls=[])

        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat_class:
            mock_model = Mock()
            mock_model.bind_tools.return_value = mock_model
            mock_retry_wrapper = Mock()
            mock_retry_wrapper.invoke.return_value = mock_response
            mock_model.with_retry.return_value = mock_retry_wrapper
            mock_chat_class.return_value = mock_model

            result = await expectation_assistant_node(sample_state, mock_config)

            mock_model.with_retry.assert_called_once()
            mock_retry_wrapper.invoke.assert_called_once()
            assert len(result.messages) == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_clears_tool_calls_when_no_batches_remain(
        self,
        expectation_assistant_node: ExpectationAssistantNode,
        sample_state: EchoesState,
        mock_config: RunnableConfig,
    ) -> None:
        sample_state.metric_batches_executed = 3  # remaining_batches == 0
        response = AIMessage(
            content="ok", tool_calls=[{"id": "1", "name": "m", "args": {"column": "x"}}]
        )

        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat:
            model = Mock()
            model.bind_tools.return_value = model
            model.with_retry.return_value = model
            model.invoke.return_value = response
            mock_chat.return_value = model

            out = await expectation_assistant_node(sample_state, mock_config)
            assert isinstance(out.messages[-1], AIMessage)
            assert out.messages[-1].tool_calls == []
            assert out.planned_tool_calls == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_deduplicates_tool_calls(
        self,
        expectation_assistant_node: ExpectationAssistantNode,
        sample_state: EchoesState,
        mock_config: RunnableConfig,
    ) -> None:
        sample_state.metric_batches_executed = 0
        dupe = {"id": "2", "name": "metricB", "args": {"column": "b", "k": 2}}
        keep = {"id": "1", "name": "metricA", "args": {"column": "a", "k": 1}}
        dupe_args = cast("dict[str, Any]", dupe["args"])
        sample_state.executed_tool_signatures.add("metricB:b:" + str(sorted(dupe_args.items())))
        response = AIMessage(content="ok", tool_calls=[keep, dupe])

        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat:
            model = Mock()
            model.bind_tools.return_value = model
            model.with_retry.return_value = model
            model.invoke.return_value = response
            mock_chat.return_value = model

            out = await expectation_assistant_node(sample_state, mock_config)
            last_msg = out.messages[-1]
            assert isinstance(last_msg, AIMessage)
            kept = last_msg.tool_calls
            assert len(kept) == 1 and kept[0]["id"] == "1"
            assert out.planned_tool_calls == 1
