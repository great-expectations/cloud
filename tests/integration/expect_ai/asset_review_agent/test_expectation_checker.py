from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from great_expectations_cloud.agent.expect_ai.expectations import UnexpectedRowsExpectation
from great_expectations_cloud.agent.expect_ai.graphs.expectation_checker import (
    MAX_EXPECTATION_REWRITE_ATTEMPTS,
    ExpectationChecker,
    ExpectationCheckerInput,
    QueryRewriterInput,
    QueryRewriterNode,
)
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig


@pytest.fixture
def mock_query_runner() -> MagicMock:
    return MagicMock(spec=QueryRunner)


@pytest.fixture
def config() -> RunnableConfig:
    return {"configurable": {"temperature": 0.7, "seed": 42}}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_rewriter_node(mock_query_runner: MagicMock, config: RunnableConfig) -> None:
    mock_query_runner.get_dialect.return_value = "snowflake"

    node = QueryRewriterNode(sql_tools_manager=mock_query_runner)

    expectation = UnexpectedRowsExpectation(
        query="SELECT * FROM {batch} WHERE value < 0",
        description="Find negative values",
    )

    state = QueryRewriterInput(
        expectation=expectation, error="Syntax error", data_source_name="test_source"
    )

    result = await node(state, config)

    assert isinstance(result.expectation, UnexpectedRowsExpectation)
    assert result.expectation.description == expectation.description
    assert result.expectation.query != expectation.query
    mock_query_runner.get_dialect.assert_called_once_with(data_source_name="test_source")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expectation_checker_max_attempts(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    checker = ExpectationChecker(query_runner=mock_query_runner)
    mock_query_runner.check_query_compiles.return_value = (False, "Syntax error")

    expectation = UnexpectedRowsExpectation(
        query="SELECT * FROM {batch} WHERE value < 0",
        description="Find negative values",
    )

    input_state = ExpectationCheckerInput(
        expectation=expectation,
        data_source_name="test_source",
        data_asset_name="test_asset",
    )

    result = await checker.graph().ainvoke(input_state, config)

    assert result["attempts"] == MAX_EXPECTATION_REWRITE_ATTEMPTS
    assert result["success"] is True  # Returns success after max attempts
    assert result["error"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expectation_checker_success_after_rewrite(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    # Create a mock QueryRunner that simulates real behavior
    mock_query_runner.check_query_compiles.side_effect = [
        (False, "Syntax error"),  # First attempt fails
        (True, None),  # Second attempt succeeds after rewrite
    ]
    mock_query_runner.get_dialect.return_value = "snowflake"

    checker = ExpectationChecker(query_runner=mock_query_runner)

    expectation = UnexpectedRowsExpectation(
        query="SELECT * FROM {batch} WHERE value < 0",
        description="Find negative values",
    )

    input_state = ExpectationCheckerInput(
        expectation=expectation,
        data_source_name="test_source",
        data_asset_name="test_asset",
    )

    result = await checker.graph().ainvoke(input_state, config)

    assert result["success"] is True
    assert result["error"] is None
    assert isinstance(result["expectation"], UnexpectedRowsExpectation)
    assert result["attempts"] == 2

    # Verify the query was checked twice (original + rewrite)
    assert mock_query_runner.check_query_compiles.call_count == 2
    # ExpectationCheckerNode calls get_dialect() once (cached), QueryRewriterNode calls it once
    assert mock_query_runner.get_dialect.call_count == 2
