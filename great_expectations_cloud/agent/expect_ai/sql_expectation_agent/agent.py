from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from great_expectations.expectations import UnexpectedRowsExpectation
from langgraph.constants import END, START
from langgraph.graph.state import CompiledStateGraph, StateGraph

from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.nodes import (
    QueryRewriterNode,
    SqlGeneratorNode,
    SqlPlannerNode,
    SqlValidatorNode,
)
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    SqlExpectationConfig,
    SqlExpectationInput,
    SqlExpectationOutput,
    SqlExpectationState,
)

if TYPE_CHECKING:
    from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
    from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner


PLANNER_NODE = "planner"
SQL_GENERATOR_NODE = "sql_generator"
SQL_VALIDATOR_NODE = "sql_validator"
QUERY_REWRITER_NODE = "query_rewriter"
CREATE_OUTPUT_NODE = "create_output"


class SqlExpectationAgent:
    def __init__(
        self,
        query_runner: QueryRunner,
        metric_service: MetricService,
    ):
        self._query_runner = query_runner
        self._metric_service = metric_service

    async def arun(
        self,
        input: SqlExpectationInput,
        thread_id: str | None = None,
        temperature: float = 0.7,
        seed: int | None = None,
    ) -> UnexpectedRowsExpectation:
        if thread_id is None:
            thread_id = str(uuid4())

        agent = self._build_agent_graph()

        output = await agent.ainvoke(
            input,
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "temperature": temperature,
                    "seed": seed,
                },
            },
        )
        result = SqlExpectationOutput(**output)
        return UnexpectedRowsExpectation(
            unexpected_rows_query=result.sql,
            description=result.description,
        )

    def _build_agent_graph(
        self,
    ) -> CompiledStateGraph[
        SqlExpectationState, SqlExpectationConfig, SqlExpectationInput, SqlExpectationOutput
    ]:
        builder = self._get_graph_builder()
        return builder.compile()

    def _get_graph_builder(
        self,
    ) -> StateGraph[
        SqlExpectationState, SqlExpectationConfig, SqlExpectationInput, SqlExpectationOutput
    ]:
        builder = StateGraph(
            state_schema=SqlExpectationState,
            context_schema=SqlExpectationConfig,
            input_schema=SqlExpectationInput,
            output_schema=SqlExpectationOutput,
        )

        builder.add_node(
            PLANNER_NODE,
            SqlPlannerNode(
                metric_service=self._metric_service,
            ),
        )

        builder.add_node(
            SQL_GENERATOR_NODE,
            SqlGeneratorNode(query_runner=self._query_runner),
        )

        builder.add_node(
            SQL_VALIDATOR_NODE,
            SqlValidatorNode(
                query_runner=self._query_runner,
                metric_service=self._metric_service,
            ),
        )

        builder.add_node(
            QUERY_REWRITER_NODE,
            QueryRewriterNode(query_runner=self._query_runner),
        )

        builder.add_node(
            CREATE_OUTPUT_NODE,
            create_final_output,
        )

        builder.add_edge(START, PLANNER_NODE)
        builder.add_edge(PLANNER_NODE, SQL_GENERATOR_NODE)
        builder.add_edge(SQL_GENERATOR_NODE, SQL_VALIDATOR_NODE)
        builder.add_conditional_edges(
            SQL_VALIDATOR_NODE,
            sql_validation_condition,
            [QUERY_REWRITER_NODE, CREATE_OUTPUT_NODE],
        )
        builder.add_edge(QUERY_REWRITER_NODE, SQL_VALIDATOR_NODE)
        builder.add_edge(CREATE_OUTPUT_NODE, END)

        return builder


def sql_validation_condition(state: SqlExpectationState) -> str:
    """Determine whether to rewrite SQL or end based on validation results."""
    # If validation succeeded, go to create_output
    if state.success:
        return CREATE_OUTPUT_NODE
    else:
        return QUERY_REWRITER_NODE


def create_final_output(state: SqlExpectationState) -> SqlExpectationOutput:
    """Create the final output from the validated state."""
    return SqlExpectationOutput(
        sql=state.potential_sql or "",
        description=state.potential_description or "",
    )
