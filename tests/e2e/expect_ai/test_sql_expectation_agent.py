from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from great_expectations.expectations import UnexpectedRowsExpectation

from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.agent import SqlExpectationAgent
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import SqlExpectationInput
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


@pytest.fixture(scope="module")
def sql_expectation_agent(context: CloudDataContext) -> SqlExpectationAgent:
    """Create SqlExpectationAgent with required dependencies"""
    query_runner = QueryRunner(context=context)
    metric_service = MetricService(context=context)

    return SqlExpectationAgent(
        query_runner=query_runner,
        metric_service=metric_service,
    )


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_sql_expectation_agent_heart_rate_expectation(
    sql_expectation_agent: SqlExpectationAgent,
    setup_test_data_source,
    get_org_id_from_env: str,
    get_workspace_id_from_env: str,
):
    """Test that SqlExpectationAgent can generate a valid expectation for heart rate data"""
    ds, asset, bd = setup_test_data_source

    heart_rate_prompt = """
    Max and min heart rate values should be within a normal
    range for a living human being.
    """

    expectation = await sql_expectation_agent.arun(
        input=SqlExpectationInput(
            organization_id=get_org_id_from_env,
            workspace_id=get_workspace_id_from_env,
            data_source_name=ds.name,
            data_asset_name=asset.name,
            batch_definition_name=bd.name,
            user_prompt=heart_rate_prompt,
        ),
    )

    # Verify the expectation was created successfully
    assert expectation is not None
    assert isinstance(expectation, UnexpectedRowsExpectation)
    assert expectation.unexpected_rows_query is not None
    assert expectation.description is not None

    # Verify the description follows length requirement
    assert len(expectation.description) < 100  # Should be descriptive but concise

    # Validate the SQL query
    assert isinstance(expectation.unexpected_rows_query, str)  # no eval params
    assert "{batch}" in expectation.unexpected_rows_query
    query_lower = expectation.unexpected_rows_query.lower()
    assert any(keyword in query_lower for keyword in ["heart_rate_max", "heart_rate_min"])


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_sql_expectation_agent_pace_expectation(
    sql_expectation_agent: SqlExpectationAgent,
    setup_test_data_source,
    get_org_id_from_env: str,
    get_workspace_id_from_env: str,
):
    """Test that SqlExpectationAgent can generate a valid expectation for pace data"""
    ds, asset, bd = setup_test_data_source

    normal_pace_prompt = """
    The pace of a running activity should be within a normal range for a fit human being.
    """

    expectation = await sql_expectation_agent.arun(
        input=SqlExpectationInput(
            organization_id=get_org_id_from_env,
            workspace_id=get_workspace_id_from_env,
            data_source_name=ds.name,
            data_asset_name=asset.name,
            batch_definition_name=bd.name,
            user_prompt=normal_pace_prompt,
        ),
    )

    # Verify the expectation was created successfully
    assert expectation is not None
    assert isinstance(expectation, UnexpectedRowsExpectation)
    assert expectation.unexpected_rows_query is not None
    assert expectation.description is not None

    # Verify the description follows length requirement
    assert len(expectation.description) < 100

    # Validate the SQL query
    assert isinstance(expectation.unexpected_rows_query, str)  # no eval params
    assert "{batch}" in expectation.unexpected_rows_query
    query_lower = expectation.unexpected_rows_query.lower()
    assert "pace" in query_lower


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_sql_expectation_agent_grade_adjusted_distance_expectation(
    sql_expectation_agent: SqlExpectationAgent,
    setup_test_data_source,
    get_org_id_from_env: str,
    get_workspace_id_from_env: str,
):
    """Test that SqlExpectationAgent can generate a valid expectation for grade adjusted distance"""
    ds, asset, bd = setup_test_data_source

    graded_adjusted_prompt = """
    The grade adjusted distance of an activity should always be within 5% of the actual distance.
    """

    expectation = await sql_expectation_agent.arun(
        input=SqlExpectationInput(
            organization_id=get_org_id_from_env,
            workspace_id=get_workspace_id_from_env,
            data_source_name=ds.name,
            data_asset_name=asset.name,
            batch_definition_name=bd.name,
            user_prompt=graded_adjusted_prompt,
        ),
    )

    # Verify the expectation was created successfully
    assert expectation is not None
    assert isinstance(expectation, UnexpectedRowsExpectation)
    assert expectation.unexpected_rows_query is not None
    assert expectation.description is not None

    # Verify the description follows length requirement
    assert len(expectation.description) < 100

    # Validate the SQL query
    assert isinstance(expectation.unexpected_rows_query, str)  # no eval params
    assert "{batch}" in expectation.unexpected_rows_query
    query_lower = expectation.unexpected_rows_query.lower()
    assert any(keyword in query_lower for keyword in ["distance", "grade", "adjusted"])
