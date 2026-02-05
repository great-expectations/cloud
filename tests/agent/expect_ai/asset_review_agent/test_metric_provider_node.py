from __future__ import annotations

from unittest.mock import Mock, create_autospec

import pytest
from great_expectations.core.batch_definition import BatchDefinition
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.runnables import RunnableConfig

from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import MetricProviderNode
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import EchoesState
from great_expectations_cloud.agent.expect_ai.tools.metrics import AgentToolsManager


class TestMetricProviderNodeInitialization:
    @pytest.mark.unit
    def test_init_with_tools_manager(self) -> None:
        mock_tools_manager = Mock(spec=AgentToolsManager)
        node = MetricProviderNode(tools_manager=mock_tools_manager)

        assert node._tools_manager is mock_tools_manager


class TestMetricProviderNodeCall:
    @pytest.fixture
    def mock_tools_manager(self) -> Mock:
        mock = Mock(spec=AgentToolsManager)
        return mock

    @pytest.fixture
    def metric_provider_node(self, mock_tools_manager: Mock) -> MetricProviderNode:
        return MetricProviderNode(tools_manager=mock_tools_manager)

    @pytest.fixture
    def sample_state_with_tool_calls(self) -> EchoesState:
        ai_message = AIMessage(
            content="Getting metrics",
            tool_calls=[
                {
                    "id": "tool_1",
                    "name": "ColumnDescriptiveStats",
                    "args": {"column": "age"},
                },
                {
                    "id": "tool_2",
                    "name": "ColumnDistinctValues",
                    "args": {"column": "status"},
                },
            ],
        )
        return EchoesState(
            organization_id="test_org",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=create_autospec(BatchDefinition, instance=True),
            messages=[ai_message],
            potential_expectations=[],
            expectations=[],
        )

    @pytest.fixture
    def sample_state_without_tool_calls(self) -> EchoesState:
        return EchoesState(
            organization_id="test_org",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=create_autospec(BatchDefinition, instance=True),
            messages=[HumanMessage(content="No tools here")],
            potential_expectations=[],
            expectations=[],
        )

    @pytest.fixture
    def mock_config(self) -> RunnableConfig:
        return RunnableConfig(configurable={})

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_with_successful_metrics(
        self,
        metric_provider_node: MetricProviderNode,
        sample_state_with_tool_calls: EchoesState,
        mock_config: RunnableConfig,
        mock_tools_manager: Mock,
    ) -> None:
        mock_tool_1 = Mock()
        mock_tool_1.name = "ColumnDescriptiveStats"
        mock_tool_1.func = Mock(return_value="Stats: mean=35, std=10")

        mock_tool_2 = Mock()
        mock_tool_2.name = "ColumnDistinctValues"
        mock_tool_2.func = Mock(return_value="Values: ['active', 'inactive']")

        mock_tools_manager.get_tools.return_value = [mock_tool_1, mock_tool_2]

        result = await metric_provider_node(sample_state_with_tool_calls, mock_config)

        mock_tool_1.func.assert_called_once_with(
            batch_definition=sample_state_with_tool_calls.batch_definition,
            batch_parameters=sample_state_with_tool_calls.batch_parameters,
            column="age",
        )
        mock_tool_2.func.assert_called_once_with(
            batch_definition=sample_state_with_tool_calls.batch_definition,
            batch_parameters=sample_state_with_tool_calls.batch_parameters,
            column="status",
        )

        assert "messages" in result
        messages = result["messages"]
        assert len(messages) == 3  # 2 ToolMessages + 1 HumanMessage

        assert isinstance(messages[0], ToolMessage)
        assert messages[0].content == "Stats: mean=35, std=10"
        assert messages[0].tool_call_id == "tool_1"

        assert isinstance(messages[1], ToolMessage)
        assert messages[1].content == "Values: ['active', 'inactive']"
        assert messages[1].tool_call_id == "tool_2"

        assert isinstance(messages[2], HumanMessage)
        content_val = messages[2].content
        if isinstance(content_val, list):
            rendered = "".join(item if isinstance(item, str) else str(item) for item in content_val)
        else:
            rendered = str(content_val)
        assert rendered.startswith("Here is the info you asked for.")

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_with_failed_metric(
        self,
        metric_provider_node: MetricProviderNode,
        sample_state_with_tool_calls: EchoesState,
        mock_config: RunnableConfig,
        mock_tools_manager: Mock,
    ) -> None:
        mock_tool_1 = Mock()
        mock_tool_1.name = "ColumnDescriptiveStats"
        mock_tool_1.func = Mock(return_value="Could not compute metric: Column 'age' not found")

        mock_tool_2 = Mock()
        mock_tool_2.name = "ColumnDistinctValues"
        mock_tool_2.func = Mock(return_value="Values: ['active', 'inactive']")

        mock_tools_manager.get_tools.return_value = [mock_tool_1, mock_tool_2]

        result = await metric_provider_node(sample_state_with_tool_calls, mock_config)

        messages = result["messages"]

        assert isinstance(messages[0], ToolMessage)
        assert "METRIC FAILED" in messages[0].content
        assert "Metric: ColumnDescriptiveStats" in messages[0].content
        assert "Column: age" in messages[0].content
        assert "Error:" in messages[0].content
        assert "Action: Analyze this error" in messages[0].content

        assert isinstance(messages[1], ToolMessage)
        assert messages[1].content == "Values: ['active', 'inactive']"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_with_list_observation(
        self,
        metric_provider_node: MetricProviderNode,
        sample_state_with_tool_calls: EchoesState,
        mock_config: RunnableConfig,
        mock_tools_manager: Mock,
    ) -> None:
        mock_tool_1 = Mock()
        mock_tool_1.name = "ColumnDescriptiveStats"
        mock_tool_1.func = Mock(return_value=["Value 1", "Value 2", "Value 3"])

        mock_tool_2 = Mock()
        mock_tool_2.name = "ColumnDistinctValues"
        mock_tool_2.func = Mock(return_value="Regular string")

        mock_tools_manager.get_tools.return_value = [mock_tool_1, mock_tool_2]

        result = await metric_provider_node(sample_state_with_tool_calls, mock_config)

        messages = result["messages"]

        assert isinstance(messages[0], ToolMessage)
        assert messages[0].content == "Value 1\nValue 2\nValue 3"

        assert isinstance(messages[1], ToolMessage)
        assert messages[1].content == "Regular string"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_without_tool_calls(
        self,
        metric_provider_node: MetricProviderNode,
        sample_state_without_tool_calls: EchoesState,
        mock_config: RunnableConfig,
    ) -> None:
        result = await metric_provider_node(sample_state_without_tool_calls, mock_config)

        assert "messages" in result
        messages = result["messages"]
        assert len(messages) == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_skips_non_ai_messages(
        self,
        metric_provider_node: MetricProviderNode,
        mock_config: RunnableConfig,
    ) -> None:
        mixed_state = EchoesState(
            organization_id="test_org",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=create_autospec(BatchDefinition, instance=True),
            messages=[
                HumanMessage(content="Human message"),
                AIMessage(content="AI without tools"),
                AIMessage(
                    content="AI with tools",
                    tool_calls=[
                        {
                            "id": "tool_1",
                            "name": "TestTool",
                            "args": {"arg": "value"},
                        }
                    ],
                ),
            ],
            potential_expectations=[],
            expectations=[],
        )

        mock_tool = Mock()
        mock_tool.name = "TestTool"
        mock_tool.func = Mock(return_value="Tool result")

        mock_tools_manager = Mock(spec=AgentToolsManager)
        mock_tools_manager.get_tools.return_value = [mock_tool]

        node = MetricProviderNode(tools_manager=mock_tools_manager)
        result = await node(mixed_state, mock_config)

        messages = result["messages"]
        assert len(messages) == 2  # 1 ToolMessage + 1 HumanMessage
        assert isinstance(messages[0], ToolMessage)
        assert messages[0].content == "Tool result"
