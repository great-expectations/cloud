"""Planner node for SQL expectation agent."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from great_expectations.datasource.fluent.sql_datasource import (
    SQLDatasource,
    TableAsset,
)
from great_expectations.metrics.batch.batch_column_types import (
    BatchColumnTypes,
    BatchColumnTypesResult,
)
from great_expectations.metrics.batch.sample_values import (
    SampleValues,
    SampleValuesResult,
)
from great_expectations.metrics.metric_results import MetricErrorResult
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig  # noqa: TC002

from great_expectations_cloud.agent.expect_ai.exceptions import (
    InvalidAssetTypeError,
    InvalidDataSourceTypeError,
)
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    CoreMetrics,
    SqlExpectationInput,
    SqlExpectationState,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import (
        Batch,
        DataAsset,
        Datasource,
    )
    from great_expectations.execution_engine import ExecutionEngine

    from great_expectations_cloud.agent.expect_ai.metric_service import MetricService

logger = logging.getLogger(__name__)


class SqlPlannerNode:
    """
    Planner node for SQL expectation agent that returns SqlExpectationState.
    """

    SYSTEM_MESSAGE = """
    You are a knowledgeable data quality expert who specializes in recommending data quality tests for SQL tables and databases.
    """

    def __init__(self, metric_service: MetricService):
        self._metric_service = metric_service

    async def __call__(
        self, state: SqlExpectationInput, config: RunnableConfig
    ) -> SqlExpectationState:
        """Generate a plan for how to develop useful data quality tests for this data."""
        core_metrics = self.get_core_metrics(
            data_source_name=state.data_source_name,
            asset_name=state.data_asset_name,
            batch_definition_name=state.batch_definition_name,
        )
        messages = [
            SystemMessage(self.SYSTEM_MESSAGE),
            self.user_prompt_message(state),
            self.schema_message(core_metrics),
            self.sample_values_message(core_metrics),
        ]

        return SqlExpectationState(
            organization_id=state.organization_id,
            workspace_id=state.workspace_id,
            user_prompt=state.user_prompt,
            data_source_name=state.data_source_name,
            data_asset_name=state.data_asset_name,
            batch_definition_name=state.batch_definition_name,
            messages=messages,
            batch_definition=core_metrics.batch_definition,
        )

    def user_prompt_message(self, state: SqlExpectationInput) -> HumanMessage:
        """HumanMessage containing the user-provided prompt."""
        return HumanMessage(f"User Input: ```{state.user_prompt}```\n\n")

    def schema_message(self, core_metrics: CoreMetrics) -> HumanMessage:
        """HumanMessage containing the table schema, if any."""
        message_str = f"{core_metrics.sql_dialect}\n{core_metrics.table_name}\n"
        if isinstance(core_metrics.schema_result, BatchColumnTypesResult):
            message_str += (
                "Table schema in CSV format with header:\ncolumn_name,column_type\n"
                + "\n".join([f"{col.name},{col.type}" for col in core_metrics.schema_result.value])
            )
        elif isinstance(core_metrics.schema_result, MetricErrorResult):
            message_str += f"Could not compute column types: {core_metrics.schema_result.value.exception_message}"
        return HumanMessage(message_str)

    def sample_values_message(self, core_metrics: CoreMetrics) -> HumanMessage:
        """HumanMessage containing the table sample values, if any."""
        result = core_metrics.sample_values_result
        if isinstance(result, SampleValuesResult):
            message_str = "Table sample values in CSV format with header:\n\n"
            if not result.value.empty:
                message_str += result.value.to_csv(index=False, lineterminator="\n").rstrip("\n")
        elif isinstance(result, MetricErrorResult):
            message_str = f"Could not compute sample values: {result.value.exception_message}"
        else:
            # unreachable: CoreMetrics.sample_values_result must always be SampleValuesResult or MetricErrorResult
            message_str = ""
        return HumanMessage(message_str)

    def get_core_metrics(
        self,
        data_source_name: str,
        asset_name: str,
        batch_definition_name: str,
    ) -> CoreMetrics:
        """Use the MetricService to retrieve data required by the Planner."""
        data_source: Datasource[DataAsset[Any, Any], ExecutionEngine[Any]] = (
            self._metric_service.get_data_source(data_source_name)
        )
        if not isinstance(data_source, SQLDatasource):
            raise InvalidDataSourceTypeError(type(data_source), (SQLDatasource,))
        asset = data_source.get_asset(asset_name)
        if not isinstance(asset, TableAsset):
            raise InvalidAssetTypeError(type(asset), (TableAsset,))
        batch_definition = asset.get_batch_definition(batch_definition_name)
        batch: Batch = batch_definition.get_batch()
        schema_result, sample_values_result = batch.compute_metrics(
            [
                BatchColumnTypes(),
                SampleValues(),
            ]
        )
        engine = data_source.get_execution_engine()
        sql_dialect = f"SQL dialect: {engine.dialect.name}"
        table_name = f"Table name: {asset.table_name}"
        return CoreMetrics(
            batch_definition=batch_definition,
            sql_dialect=sql_dialect,
            table_name=table_name,
            schema_result=schema_result,  # type: ignore[arg-type]  # GX API is loosely typed here
            sample_values_result=sample_values_result,  # type: ignore[arg-type]  # GX API is loosely typed here
        )
