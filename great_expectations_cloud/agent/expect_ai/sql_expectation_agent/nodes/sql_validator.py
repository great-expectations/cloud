"""SQL validator node for SQL expectation agent."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from langchain_core.runnables import RunnableConfig  # noqa: TC002

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Datasource
    from great_expectations.datasource.fluent.sql_datasource import TableAsset
    from great_expectations.execution_engine.sqlalchemy_execution_engine import (
        SqlAlchemyExecutionEngine,
    )

    from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
    from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
        SqlExpectationState,
    )
    from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner

logger = logging.getLogger(__name__)

# Maximum number of SQL rewrite attempts
MAX_SQL_REWRITE_ATTEMPTS = 3


class SqlValidatorNode:
    def __init__(self, query_runner: QueryRunner, metric_service: MetricService):
        self._query_runner = query_runner
        self._metric_service = metric_service

    async def __call__(self, state: SqlExpectationState, config: RunnableConfig) -> dict[str, Any]:
        """Validate the generated SQL and manage retry logic."""

        # Check if we've exceeded max attempts
        attempts = state.sql_validation_attempts
        if attempts >= MAX_SQL_REWRITE_ATTEMPTS:
            logger.error(f"SQL failed to compile after {MAX_SQL_REWRITE_ATTEMPTS} attempts")
            return {
                "success": True,  # Set to True to end the loop
                "sql_validation_attempts": attempts + 1,
                "error": f"Query failed to compile after {MAX_SQL_REWRITE_ATTEMPTS} attempts.",
            }

        # If we have potential SQL from either generator or rewriter, validate it
        sql_to_validate = state.potential_sql

        if not sql_to_validate:
            logger.error("No SQL to validate")
            return {
                "success": True,
                "sql_validation_attempts": attempts + 1,
                "error": "No SQL generated to validate",
            }

        # Validate the SQL query compiles
        data_source: Datasource[TableAsset, SqlAlchemyExecutionEngine] = (
            self._metric_service.get_data_source(state.data_source_name)
        )
        asset = data_source.get_asset(state.data_asset_name)
        sql_query = sql_to_validate.replace("{batch}", asset.table_name)
        compiles, error = self._query_runner.check_query_compiles(
            data_source_name=state.data_source_name,
            query_text=sql_query,
        )

        if compiles:
            logger.info("SQL query validated successfully")
            return {
                "success": True,
                "sql_validation_attempts": attempts + 1,
                "error": None,
            }
        else:
            logger.warning(f"SQL query failed validation: {error}")
            return {
                "success": False,
                "sql_validation_attempts": attempts + 1,
                "error": error,
            }
