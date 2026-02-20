from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from great_expectations_cloud.agent.analytics import AgentAnalytics, RejectionReason
from great_expectations_cloud.agent.expect_ai.expectations import (
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToMatchRegex,
    ExpectCompoundColumnsToBeUnique,
    UnexpectedRowsExpectation,
)
from great_expectations_cloud.agent.expect_ai.graphs.expectation_checker import (
    ExpectationChecker,
    ExpectationCheckerInput,
    ExpectationCheckerNode,
    ExpectationCheckerState,
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expectation_checker_node_success(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    node = ExpectationCheckerNode(sql_tools_manager=mock_query_runner, analytics=AgentAnalytics())
    mock_query_runner.check_query_compiles.return_value = (True, None)

    expectation = UnexpectedRowsExpectation(
        query="SELECT * FROM {batch} WHERE value < 0",
        description="Find negative values",
    )

    state = ExpectationCheckerState(
        expectation=expectation,
        data_source_name="test_source",
        data_asset_name="test_asset",
    )

    result = await node(state, config)

    assert result.success is True
    assert result.error is None
    assert result.expectation == expectation
    mock_query_runner.check_query_compiles.assert_called_once_with(
        data_source_name="test_source",
        query_text="SELECT * FROM test_asset WHERE value < 0",
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expectation_checker_node_failure(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    node = ExpectationCheckerNode(sql_tools_manager=mock_query_runner, analytics=AgentAnalytics())
    mock_query_runner.check_query_compiles.return_value = (False, "Syntax error")

    expectation = UnexpectedRowsExpectation(
        query="SELECT * FROM {batch} WHERE value < 0",
        description="Find negative values",
    )

    state = ExpectationCheckerState(
        expectation=expectation,
        data_source_name="test_source",
        data_asset_name="test_asset",
    )

    result = await node(state, config)

    assert result.success is False
    assert result.error == "Syntax error"
    assert result.expectation == expectation


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expectation_checker_non_sql_expectation(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    """Test that non-UnexpectedRowsExpectation types pass through without checking"""
    node = ExpectationCheckerNode(sql_tools_manager=mock_query_runner, analytics=AgentAnalytics())

    expectation = ExpectColumnValuesToBeUnique(column="test_column", description="test", mostly=1.0)

    state = ExpectationCheckerState(
        expectation=expectation,
        data_source_name="test_source",
        data_asset_name="test_asset",
    )

    result = await node(state, config)

    assert result.success is True
    assert result.error is None
    assert result.expectation == expectation
    mock_query_runner.check_query_compiles.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expectation_checker_catches_invalid_construction(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    checker = ExpectationChecker(query_runner=mock_query_runner)

    invalid_expectation = ExpectCompoundColumnsToBeUnique(
        description="foo",
        column_list=["a"],  # the underlying expectaiton requires at least 2 columns
    )

    input_state = ExpectationCheckerInput(
        expectation=invalid_expectation,
        data_source_name="foo",
        data_asset_name="bar",
    )

    result = await checker.graph().ainvoke(input_state, config)

    assert result["success"] is True
    assert "pydantic" in result["error"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expectation_checker_node_only_replaces_batch_keyword(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    node = ExpectationCheckerNode(sql_tools_manager=mock_query_runner, analytics=AgentAnalytics())
    mock_query_runner.check_query_compiles.return_value = (True, None)

    expectation = UnexpectedRowsExpectation(
        # str.format() would attempt to replace {4} or {2} with non-existent replacement parameter at those indicies
        query="SELECT * FROM {batch} WHERE TO_CHAR(source_file_date, 'YYYY-MM') <> REGEXP_SUBSTR(source_file, '\\d{4}-\\d{2}')",
        description="Ensure `source_file_date` matches the date in the `source_file` path.",
    )

    state = ExpectationCheckerState(
        expectation=expectation,
        data_source_name="test_source",
        data_asset_name="test_asset",
    )

    result = await node(state, config)

    assert result.success is True
    assert result.error is None
    assert result.expectation == expectation
    mock_query_runner.check_query_compiles.assert_called_once_with(
        data_source_name="test_source",
        query_text="SELECT * FROM test_asset WHERE TO_CHAR(source_file_date, 'YYYY-MM') <> REGEXP_SUBSTR(source_file, '\\d{4}-\\d{2}')",
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expectation_checker_rejects_regex_for_mssql(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    """ExpectColumnValuesToMatchRegex must be rejected for mssql (no native regex support)."""
    mock_analytics = MagicMock(spec=AgentAnalytics)
    mock_query_runner.get_dialect.return_value = "mssql"
    node = ExpectationCheckerNode(sql_tools_manager=mock_query_runner, analytics=mock_analytics)

    expectation = ExpectColumnValuesToMatchRegex(
        column="email",
        regex=r"^[^@]+@[^@]+$",
        description="Validate email format",
        mostly=1.0,
    )
    state = ExpectationCheckerState(
        expectation=expectation,
        data_source_name="test_source",
        data_asset_name="test_asset",
    )

    result = await node(state, config)

    assert result.success is True
    assert result.error is not None
    assert "expect_column_values_to_match_regex" in result.error
    assert "SQL Server" in result.error
    mock_analytics.emit_expectation_rejected.assert_called_once_with(
        expectation_type="expect_column_values_to_match_regex",
        reason=RejectionReason.UNSUPPORTED_DIALECT,
    )
    mock_query_runner.check_query_compiles.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expectation_checker_allows_regex_for_non_mssql_dialect(
    mock_query_runner: MagicMock, config: RunnableConfig
) -> None:
    """ExpectColumnValuesToMatchRegex must pass through for dialects that support regex."""
    mock_analytics = MagicMock(spec=AgentAnalytics)
    mock_query_runner.get_dialect.return_value = "snowflake"
    node = ExpectationCheckerNode(sql_tools_manager=mock_query_runner, analytics=mock_analytics)

    expectation = ExpectColumnValuesToMatchRegex(
        column="email",
        regex=r"^[^@]+@[^@]+$",
        description="Validate email format",
        mostly=1.0,
    )
    state = ExpectationCheckerState(
        expectation=expectation,
        data_source_name="test_source",
        data_asset_name="test_asset",
    )

    result = await node(state, config)

    assert result.success is True
    assert result.error is None
    mock_analytics.emit_expectation_validated.assert_called_once_with(
        expectation_type="expect_column_values_to_match_regex"
    )
    mock_analytics.emit_expectation_rejected.assert_not_called()
    mock_query_runner.check_query_compiles.assert_not_called()
