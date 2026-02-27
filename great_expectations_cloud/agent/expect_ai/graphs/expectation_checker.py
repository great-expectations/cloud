from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, Final

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig  # noqa: TC002
from langchain_openai import ChatOpenAI
from langgraph.constants import END, START
from langgraph.graph import StateGraph
from langgraph.graph.state import CompiledStateGraph  # noqa: TC002
from pydantic import BaseModel, Field
from pydantic import ValidationError as PydanticV2ValidationError
from pydantic.v1 import ValidationError as PydanticV1ValidationError

from great_expectations_cloud.agent.analytics import AgentAnalytics, RejectionReason
from great_expectations_cloud.agent.expect_ai.config import OPENAI_MODEL
from great_expectations_cloud.agent.expect_ai.exceptions import (
    InvalidExpectationTypeError,
    InvalidResponseTypeError,
)
from great_expectations_cloud.agent.expect_ai.expectations import (
    OpenAIGXExpectation,
    UnexpectedRowsExpectation,
)

if TYPE_CHECKING:
    from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner

LOGGER = logging.getLogger(__name__)
MAX_EXPECTATION_REWRITE_ATTEMPTS: Final = 3
_UNSET = "__unset__"


def _allow_null_overrides(a: str | None, b: str | None) -> str | None:
    """Hack to force langgraph's state management to allow
    setting fields to None.

    When using pydantic models, the internal state management appears to be
    ignoring fields set to None. Related: https://github.com/langchain-ai/langgraph/issues/2538.
    """
    return None if b == _UNSET else b


class ExpectationCheckerState(BaseModel):
    expectation: OpenAIGXExpectation | bool
    data_source_name: str
    data_asset_name: str
    attempts: int = 0
    success: bool | None = None
    error: Annotated[str | None, _allow_null_overrides] = None


class ExpectationCheckerInput(BaseModel):
    expectation: OpenAIGXExpectation
    data_source_name: str
    data_asset_name: str
    attempts: int = 0


class ExpectationCheckerOutput(BaseModel):
    attempts: int = 0
    success: bool | None = None
    error: str | None = None
    expectation: OpenAIGXExpectation


class QueryRewriterInput(BaseModel):
    expectation: OpenAIGXExpectation
    error: str | None = None
    data_source_name: str


class QueryRewriterOutput(BaseModel):
    expectation: OpenAIGXExpectation


class ExpectationChecker:
    def __init__(
        self,
        query_runner: QueryRunner,
        analytics: AgentAnalytics | None = None,
    ):
        self._query_runner = query_runner
        self._analytics = analytics or AgentAnalytics()

    def graph(
        self,
    ) -> CompiledStateGraph[
        ExpectationCheckerState, None, ExpectationCheckerInput, ExpectationCheckerOutput
    ]:
        builder = StateGraph(
            state_schema=ExpectationCheckerState,
            input_schema=ExpectationCheckerInput,
            output_schema=ExpectationCheckerOutput,
        )
        builder.add_node(
            "expectation_checker",
            ExpectationCheckerNode(sql_tools_manager=self._query_runner, analytics=self._analytics),
        )
        builder.add_node("query_rewriter", QueryRewriterNode(sql_tools_manager=self._query_runner))

        builder.add_conditional_edges(
            "expectation_checker", self._query_rewriter_or_end, ["query_rewriter", END]
        )
        builder.add_edge(START, "expectation_checker")
        builder.add_edge("query_rewriter", "expectation_checker")
        return builder.compile()

    @staticmethod
    def _query_rewriter_or_end(state: ExpectationCheckerState) -> str:
        """After an expectation is validated it can be output, otherwise it must be rewritten."""
        return "query_rewriter" if not state.success else END


class ExpectationCheckerNode:
    def __init__(self, sql_tools_manager: QueryRunner, analytics: AgentAnalytics):
        self._sql_tools_manager = sql_tools_manager
        self._analytics = analytics

    async def __call__(
        self, state: ExpectationCheckerState, config: RunnableConfig
    ) -> ExpectationCheckerOutput:
        expectation_type = "unknown"
        if isinstance(state.expectation, OpenAIGXExpectation):
            expectation_type = state.expectation.expectation_type

            try:
                state.expectation.get_gx_expectation()
            except (PydanticV1ValidationError, PydanticV2ValidationError) as e:
                self._analytics.emit_expectation_rejected(
                    expectation_type=expectation_type,
                    reason=RejectionReason.INVALID_PYDANTIC_CONSTRUCTION,
                )
                return ExpectationCheckerOutput(
                    success=True,
                    error=f"Failed pydantic validation: {e}",
                    attempts=state.attempts,
                    expectation=state.expectation,
                )
            except Exception as e:
                self._analytics.emit_expectation_rejected(
                    expectation_type=expectation_type, reason=RejectionReason.INVALID_CONSTRUCTION
                )
                return ExpectationCheckerOutput(
                    success=True,
                    error=f"Failed to instantiate: {e}",
                    attempts=state.attempts,
                    expectation=state.expectation,
                )

        if state.attempts >= MAX_EXPECTATION_REWRITE_ATTEMPTS:
            self._analytics.emit_expectation_rejected(
                expectation_type=expectation_type, reason=RejectionReason.INVALID_SQL
            )
            return ExpectationCheckerOutput(
                success=True,
                error=f"Query failed to compile after {MAX_EXPECTATION_REWRITE_ATTEMPTS} attempts.",
                attempts=state.attempts,
                expectation=state.expectation,
            )

        if not isinstance(state.expectation, UnexpectedRowsExpectation):
            self._analytics.emit_expectation_validated(expectation_type=expectation_type)
            return ExpectationCheckerOutput(
                success=True,
                error=None,
                attempts=0,
                expectation=state.expectation,
            )

        query_text = state.expectation.query.replace("{batch}", state.data_asset_name)
        (success, error) = self._sql_tools_manager.check_query_compiles(
            data_source_name=state.data_source_name, query_text=query_text
        )

        if success:
            self._analytics.emit_expectation_validated(expectation_type=expectation_type)

        error_or_unset = self._error_for_output(state, error)
        return ExpectationCheckerOutput(
            success=success,
            error=error_or_unset,
            attempts=state.attempts + 1,
            expectation=state.expectation,
        )

    def _error_for_output(self, state: ExpectationCheckerState, error: str | None) -> str | None:
        """Hack to get force langgraph's state management to allow us to overwrite non-None with None.

        TODO: Remove the need for this and _allow_null_overrides
        """
        if state.attempts == 0:
            return error
        elif error is None:
            return _UNSET
        else:
            return error


class QueryResponse(BaseModel):
    query: str = Field(..., description="The SQL query.")
    rationale: str = Field(..., description="The rationale for changes made to the query.")


class QueryRewriterNode:
    def __init__(self, sql_tools_manager: QueryRunner):
        self._sql_tools_manager = sql_tools_manager

    async def __call__(
        self, state: QueryRewriterInput, config: RunnableConfig
    ) -> QueryRewriterOutput:
        # this should never happen, but we need to make the type system happy
        if not isinstance(state.expectation, UnexpectedRowsExpectation):
            raise InvalidExpectationTypeError(type(state.expectation), UnexpectedRowsExpectation)

        structured_output_model = ChatOpenAI(
            model_name=OPENAI_MODEL,
            temperature=config["configurable"].get("temperature", 0.7),
            seed=config["configurable"].get("seed", None),
            request_timeout=60,
        ).with_structured_output(schema=QueryResponse, method="json_schema", strict=True)

        dialect = self._sql_tools_manager.get_dialect(data_source_name=state.data_source_name)
        system_prompt = (
            "You are an expert SQL developer proficient in debugging and fixing "
            + dialect
            + " SQL queries."
        )
        human_prompt = f"""
        The following query failed to compile:

        {state.expectation.query}

        with the error message:

        {state.error}

        The token {{batch}} is used as placeholder for the table name.

        Rewrite the query for the {dialect} dialect. If the query can be rewritten in multiple ways, only consider the most efficient and effective way to rewrite it.

        The rewritten query must meet these requirements:
            - it must be logically equivalent to the original query
            - it must return exactly the fields as the original query
        """
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=human_prompt),
        ]
        response = await structured_output_model.ainvoke(messages)
        if not isinstance(response, QueryResponse):
            raise InvalidResponseTypeError(type(response), QueryResponse)

        expectation = UnexpectedRowsExpectation(
            query=response.query,
            description=state.expectation.description,
        )

        return QueryRewriterOutput(
            expectation=expectation,
        )
