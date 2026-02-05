from __future__ import annotations

from unittest.mock import AsyncMock, Mock, create_autospec, patch

import pytest
from great_expectations.core.batch_definition import BatchDefinition
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import (
    AssetReviewAgent,
    ExpectationBuilderNode,
    ExpectationBuilderState,
    MetricProviderNode,
    tools_condition,
)
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    DataQualityPlanComponent,
    EchoesState,
    ExistingExpectationContext,
)
from great_expectations_cloud.agent.expect_ai.expectations import AddExpectationsResponse
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner


@pytest.mark.unit
def test_get_raw_graph_for_langgraph_studio_compiles() -> None:
    agent = AssetReviewAgent(
        tools_manager=Mock(),
        query_runner=Mock(),
        metric_service=Mock(),
    )

    compiled = agent.get_raw_graph_for_langgraph_studio()
    assert isinstance(compiled, CompiledStateGraph)


@pytest.mark.unit
def test_tools_condition_returns_metric_provider_when_ai_message_has_tool_calls() -> None:
    # Arrange
    state = EchoesState(
        organization_id="org",
        data_source_name="ds",
        data_asset_name="asset",
        batch_definition_name="batch",
        batch_definition=create_autospec(BatchDefinition, instance=True),
        messages=[AIMessage(content="x", tool_calls=[{"id": "t1", "name": "n", "args": {}}])],
        potential_expectations=[],
        expectations=[],
    )

    # Act
    branch = tools_condition(state)

    # Assert
    assert branch == "metric_provider"


@pytest.mark.unit
def test_tools_condition_returns_quality_issue_summarizer_when_no_tool_calls() -> None:
    # Arrange
    state = EchoesState(
        organization_id="org",
        data_source_name="ds",
        data_asset_name="asset",
        batch_definition_name="batch",
        batch_definition=create_autospec(BatchDefinition, instance=True),
        messages=[AIMessage(content="x", tool_calls=[])],
        potential_expectations=[],
        expectations=[],
    )

    # Act
    branch = tools_condition(state)

    # Assert
    assert branch == "quality_issue_summarizer"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expectation_builder_node_returns_no_expectations_when_model_returns_empty_list() -> (
    None
):
    # Arrange
    query_runner = create_autospec(QueryRunner, instance=True)
    query_runner.get_dialect.return_value = "postgresql"

    node = ExpectationBuilderNode(
        sql_tools_manager=query_runner,
        templated_system_message="You are a SQL expert. Use {dialect}.",
        task_human_message="Build expectations",
    )

    plan_component = DataQualityPlanComponent(title="Title", plan_details="Details")
    state = ExpectationBuilderState(
        plan_component=plan_component,
        plan_development_messages=[],
        data_source_name="my_ds",
    )

    with patch(
        "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
    ) as mock_chat_class:
        mock_model = Mock()
        mock_model.with_structured_output.return_value = mock_model
        mock_model.with_retry.return_value = mock_model
        mock_model.ainvoke = AsyncMock(
            return_value=AddExpectationsResponse(rationale="", expectations=[])
        )
        mock_chat_class.return_value = mock_model

        # Act
        result = await node(state, RunnableConfig(configurable={}))

        # Assert
        assert result.potential_expectations == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_metric_provider_converts_batch_parameters_and_handles_missing_func() -> None:
    tools_manager = Mock()
    fake_tool = Mock()
    fake_tool.name = "metricX"
    fake_tool.func = None
    tools_manager.get_tools.return_value = [fake_tool]
    node = MetricProviderNode(tools_manager=tools_manager)

    batch_definition = create_autospec(BatchDefinition, instance=True)
    state = EchoesState(
        organization_id="org",
        data_source_name="ds",
        data_asset_name="asset",
        batch_definition_name="batch",
        batch_definition=batch_definition,
        batch_parameters={"limit": 10},
        messages=[
            AIMessage(
                content="x", tool_calls=[{"id": "t1", "name": "metricX", "args": {"column": "c"}}]
            )
        ],
        potential_expectations=[],
        expectations=[],
    )

    out = await node(state, RunnableConfig(configurable={}))
    assert isinstance(state.batch_parameters, dict)
    assert out["messages"], "Expected tool and human messages appended"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_existing_expectations_are_added_to_context() -> None:
    # Arrange
    existing_expectation_context = ExistingExpectationContext(
        domain="foo_table",
        expectation_type="expect_column_values_to_be_between",
        description="foo_description",
    )
    query_runner = create_autospec(QueryRunner, instance=True)
    query_runner.get_dialect.return_value = "postgresql"

    node = ExpectationBuilderNode(
        sql_tools_manager=query_runner,
        templated_system_message="You are a SQL expert. Use {dialect}.",
        task_human_message="Build expectations",
    )

    state = ExpectationBuilderState(
        plan_component=DataQualityPlanComponent(title="Title", plan_details="Details"),
        plan_development_messages=[],
        data_source_name="my_ds",
        existing_expectation_contexts=[
            existing_expectation_context,
        ],
    )

    with patch(
        "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
    ) as mock_chat_class:
        mock_model = Mock()
        mock_model.with_structured_output.return_value = mock_model
        mock_model.with_retry.return_value = mock_model
        mock_model.ainvoke = AsyncMock(
            return_value=AddExpectationsResponse(rationale="", expectations=[])
        )
        mock_chat_class.return_value = mock_model

        # Act
        await node(state, RunnableConfig(configurable={}))

        # Assert
        assert mock_model.ainvoke.call_args[0][0][3] == HumanMessage(
            content=f"""The following Expectations already exist, so do not redundantly generate them:\n- expect_column_values_to_be_between on {existing_expectation_context.domain}: {existing_expectation_context.description}""",
            additional_kwargs={},
            response_metadata={},
        )
