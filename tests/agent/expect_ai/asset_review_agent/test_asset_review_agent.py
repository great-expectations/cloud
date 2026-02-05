from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

import pytest
from great_expectations import ExpectationSuite
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.expectations import (
    ExpectColumnValuesToBeUnique as GXExpectColumnValuesToBeUnique,
)
from langgraph.constants import END, START
from langgraph.graph.state import CompiledStateGraph, StateGraph

from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import (
    AssetReviewAgent,
)
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    BatchParameters,
    GenerateExpectationsConfig,
    GenerateExpectationsInput,
    GenerateExpectationsOutput,
    GenerateExpectationsState,
)
from great_expectations_cloud.agent.expect_ai.expectations import ExpectColumnValuesToBeUnique
from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
from great_expectations_cloud.agent.expect_ai.tools.metrics import AgentToolsManager
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner


@pytest.fixture
def noop_graph_getter() -> CompiledStateGraph[GenerateExpectationsState, GenerateExpectationsConfig, GenerateExpectationsInput, GenerateExpectationsOutput]:
    # This graph should be essentially a no-op version of the agent's real graph, with matching input/output schemas
    builder = StateGraph(
        state_schema=GenerateExpectationsState,
        context_schema=GenerateExpectationsConfig,
        input_schema=GenerateExpectationsInput,
        output_schema=GenerateExpectationsOutput,
    )

    def mock_expectations_node(state: GenerateExpectationsInput) -> GenerateExpectationsState:
        batch_definition = mock.MagicMock(spec=BatchDefinition)
        return GenerateExpectationsState(
            organization_id=state.organization_id,
            workspace_id=state.workspace_id,
            data_source_name=state.data_source_name,
            data_asset_name=state.data_asset_name,
            batch_definition_name=state.batch_definition_name,
            batch_parameters=state.batch_parameters,
            batch_definition=batch_definition,
            messages=[],
            potential_expectations=[],
            expectations=[
                ExpectColumnValuesToBeUnique(
                    column="test_column",
                    description="test expectation",
                    mostly=1.0,
                )
            ],
        )

    builder.add_node("mock_expectations_node", mock_expectations_node)
    builder.add_edge(START, "mock_expectations_node")
    builder.add_edge("mock_expectations_node", END)
    return builder.compile()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_asset_review_agent_interface(
    noop_graph_getter: CompiledStateGraph[GenerateExpectationsState, GenerateExpectationsConfig, GenerateExpectationsInput, GenerateExpectationsOutput],
) -> None:
    organization_id = "test_org_id"
    data_source_name = "test_data_source_name"
    data_asset_name = "test_data_asset_name"
    batch_definition_name = "test_batch_definition_name"
    batch_parameters = BatchParameters({})

    agent = AssetReviewAgent(
        tools_manager=MagicMock(spec=AgentToolsManager),
        query_runner=MagicMock(spec=QueryRunner),
        metric_service=MagicMock(spec=MetricService),
    )
    with mock.patch.object(agent, "_build_agent_graph") as mock_build_agent_graph:
        mock_build_agent_graph.return_value = noop_graph_getter
        echoes_input = GenerateExpectationsInput(
            organization_id=organization_id,
            workspace_id="test_workspace_id",
            data_source_name=data_source_name,
            data_asset_name=data_asset_name,
            batch_definition_name=batch_definition_name,
            batch_parameters=batch_parameters,
        )
        result = await agent.arun(echoes_input=echoes_input)

    assert isinstance(result, ExpectationSuite)
    assert len(result.expectations) == 1
    assert isinstance(result.expectations[0], GXExpectColumnValuesToBeUnique)
