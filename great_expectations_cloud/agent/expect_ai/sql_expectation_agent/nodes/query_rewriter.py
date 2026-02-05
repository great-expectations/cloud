"""Query rewriter node for SQL expectation agent."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig  # noqa: TC002
from langchain_openai import ChatOpenAI
from openai import APIConnectionError, APITimeoutError
from pydantic import BaseModel, Field

from great_expectations_cloud.agent.expect_ai.config import OPENAI_MODEL
from great_expectations_cloud.agent.expect_ai.exceptions import InvalidResponseTypeError
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    SqlExpectationState,
    SqlQueryResponse,
)

if TYPE_CHECKING:
    from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner

logger = logging.getLogger(__name__)


class QueryResponse(BaseModel):
    query: str = Field(..., description="The SQL query.")
    rationale: str = Field(..., description="The rationale for changes made to the query.")


class QueryRewriterNode:
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    async def __call__(
        self, state: SqlExpectationState, config: RunnableConfig
    ) -> SqlQueryResponse:
        """Rewrite the SQL query based on the validation error."""

        if not state.potential_sql:
            logger.error("No SQL query to rewrite")
            return SqlQueryResponse(
                potential_sql="",
                potential_description=state.potential_description or "",
            )

        structured_output_model = ChatOpenAI(
            model_name=OPENAI_MODEL,
            temperature=config["configurable"].get("temperature", 0.7),
            seed=config["configurable"].get("seed", None),
            request_timeout=60,
        ).with_structured_output(schema=QueryResponse, method="json_schema", strict=True)

        dialect = self._query_runner.get_dialect(data_source_name=state.data_source_name)
        system_prompt = (
            "You are an expert SQL developer proficient in debugging and fixing "
            + dialect
            + " SQL queries."
        )
        human_prompt = f"""
        The following query failed to compile:

        {state.potential_sql}

        with the error message:

        {state.error}

        The token {{batch}} is used as placeholder for the table name. If it is not present in the query, remove the table name and replace it with `{{batch}}`

        Rewrite the query for the {dialect} dialect. If the query can be rewritten in multiple ways, only consider the most efficient and effective way to rewrite it.

        The rewritten query must meet these requirements:
            - it must be logically equivalent to the original query
            - it must return exactly the fields as the original query
            - it must contain the token {{batch}}

        """
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=human_prompt),
        ]

        response = await structured_output_model.with_retry(
            retry_if_exception_type=(APIConnectionError, APITimeoutError),
            stop_after_attempt=2,
        ).ainvoke(messages)

        if not isinstance(response, QueryResponse):
            raise InvalidResponseTypeError(type(response), QueryResponse)

        return SqlQueryResponse(
            potential_sql=response.query,
            potential_description=state.potential_description or "",
        )
