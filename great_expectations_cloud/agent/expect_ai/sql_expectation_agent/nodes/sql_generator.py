"""SQL generator node for SQL expectation agent."""

from __future__ import annotations

from typing import TYPE_CHECKING

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig  # noqa: TC002
from langchain_openai import ChatOpenAI
from openai import APIConnectionError, APITimeoutError

from great_expectations_cloud.agent.expect_ai.config import OPENAI_MODEL
from great_expectations_cloud.agent.expect_ai.exceptions import InvalidResponseTypeError
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    SqlAndDescriptionResponse,
    SqlExpectationState,
    SqlQueryResponse,
)

if TYPE_CHECKING:
    from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner


class SqlGeneratorNode:
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    async def __call__(
        self, state: SqlExpectationState, config: RunnableConfig
    ) -> SqlQueryResponse:
        """Generate SQL query and description for UnexpectedRowsExpectation."""
        # Create the system message for SQL generation
        dialect = self._query_runner.get_dialect(data_source_name=state.data_source_name)
        system_message = SystemMessage(
            content=f"You are a SQL coding assistant that generates SQL queries using {dialect} dialect. "
            "Each SQL query should return the rows that are unexpected given the user prompt. "
            "Do not add a semicolon to the end of the query. "
            "Do not use the table name directly - instead, use `{batch}` as a placeholder.\n\n"
            "You are also an expert on interpreting SQL queries. Given a SQL query, "
            "you will generate a description of the query that is less than 75 characters long. "
            "The description should be phrased in such a way that if the query returns any rows the description is false. "
            "The description should not include SQL syntax."
        )

        # Add example
        example_message = HumanMessage(
            content="Example:\n"
            "User Input: Every customer must have an email address.\n"
            "SQL: SELECT customer_id, customer_name FROM {batch} WHERE email IS NULL OR email = ''\n"
            "Description: Expect all customers to have an email address"
        )

        # Create the final prompt
        task_message = HumanMessage(
            content="Based on the conversation history and the user input, generate a SQL query "
            "that returns unexpected rows and provide an appropriate description."
        )

        messages = [
            system_message,
            example_message,
            *state.messages,
            task_message,
        ]

        response = await self.call_model(config=config, messages=messages)

        return SqlQueryResponse(
            potential_sql=response.sql,
            potential_description=response.description,
        )

    async def call_model(
        self, config: RunnableConfig, messages: list[BaseMessage]
    ) -> SqlAndDescriptionResponse:
        structured_output_model = ChatOpenAI(
            model_name=OPENAI_MODEL,
            temperature=config["configurable"].get("temperature", 0.7),
            seed=config["configurable"].get("seed", None),
            request_timeout=60,
        ).with_structured_output(
            schema=SqlAndDescriptionResponse, method="json_schema", strict=True
        )
        response = await structured_output_model.with_retry(
            retry_if_exception_type=(APIConnectionError, APITimeoutError),
            stop_after_attempt=2,
        ).ainvoke(messages)

        if not isinstance(response, SqlAndDescriptionResponse):
            raise InvalidResponseTypeError(type(response), SqlAndDescriptionResponse)
        return response
