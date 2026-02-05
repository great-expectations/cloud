from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset
from great_expectations.metrics.batch.batch_column_types import (
    BatchColumnTypes,
    BatchColumnTypesResult,
)
from great_expectations.metrics.batch.sample_values import (
    SampleValues,
    SampleValuesResult,
)
from great_expectations.metrics.metric_results import MetricErrorResult
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig  # noqa: TC002

from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    GenerateExpectationsInput,
    GenerateExpectationsState,
)
from great_expectations_cloud.agent.expect_ai.exceptions import (
    InvalidAssetTypeError,
    InvalidDataSourceTypeError,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch, DataAsset, Datasource
    from great_expectations.execution_engine import ExecutionEngine

    from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
    from great_expectations_cloud.agent.expect_ai.tools.metrics import AgentToolsManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PlannerNode:
    """
    Node responsible for building the prompt that will be sent to the LLM for data quality test planning.

    This node extracts information about the data source, computes metrics about the data asset,
    and builds a prompt that contains relevant context for the LLM, including:
    - SQL dialect information (PostgreSQL, Snowflake, Redshift, Databricks, etc.)
    - Table structure
    - Sample data values

    The node supports all SQL-based datasources that implement the SQLDatasource interface.
    """

    def __init__(
        self,
        tools_manager: AgentToolsManager,
        metric_service: MetricService,
    ):
        self._tools_manager = tools_manager
        self._metric_service = metric_service

    async def __call__(self, state: GenerateExpectationsInput, config: RunnableConfig) -> GenerateExpectationsState:
        """Generate a plan for how to develop useful data quality tests for this data."""
        return self._update_state_with_core_metrics(state)

    def _update_state_with_core_metrics(self, state: GenerateExpectationsInput) -> GenerateExpectationsState:
        """
        Updates the state with core metrics about the data asset.

        This method:
        1. Gets the datasource and validates it's a SQL datasource
        2. Gets the asset and validates it's a table asset
        3. Extracts the SQL dialect from the datasource's execution engine
        4. Computes core metrics (schema and sample values)
        5. Builds a prompt with this information for the LLM

        Supports all SQL-based datasources (PostgreSQL, Snowflake, Redshift, Databricks, etc.)
        as long as they implement the SQLDatasource interface.

        Args:
            state: The input state containing datasource and asset information

        Returns:
            Updated state with messages containing datasource information and metrics

        Raises:
            InvalidDataSourceTypeError: If the datasource is not a SQLDatasource
            InvalidAssetTypeError: If the asset is not a TableAsset
        """
        data_source: Datasource[DataAsset[Any, Any], ExecutionEngine[Any]] = (
            self._metric_service.get_data_source(state.data_source_name)
        )
        if not isinstance(data_source, SQLDatasource):
            raise InvalidDataSourceTypeError(type(data_source), (SQLDatasource,))
        asset = data_source.get_asset(state.data_asset_name)
        if not isinstance(asset, TableAsset):
            raise InvalidAssetTypeError(type(asset), (TableAsset,))
        batch_definition = asset.get_batch_definition(state.batch_definition_name)
        batch: Batch = batch_definition.get_batch()
        schema_result, sample_values_result = batch.compute_metrics(
            [
                BatchColumnTypes(),
                SampleValues(),
            ]
        )

        logger.debug("Building initial task prompt")
        messages: list[HumanMessage] = []

        engine = data_source.get_execution_engine()
        sql_dialect = f"SQL dialect: {engine.dialect.name}"
        table_name = f"Table name: {asset.table_name}"

        if isinstance(schema_result, BatchColumnTypesResult):
            schema_csv_string = schema_metric_to_csv_string(schema_result)
            table_schema = f"Table schema in CSV format with header:\n{schema_csv_string}"
            messages.append(HumanMessage(content=f"{sql_dialect}\n{table_name}\n{table_schema}"))
        elif isinstance(schema_result, MetricErrorResult):
            messages.append(HumanMessage(content=f"{sql_dialect}\n{table_name}"))
            messages.append(
                HumanMessage(
                    content=f"Could not compute column types: {schema_result.value.exception_message}"
                )
            )

        if isinstance(sample_values_result, SampleValuesResult):
            sample_values_string = sample_values_metric_to_ai_readable_message(sample_values_result)
            messages.append(
                HumanMessage(
                    content=f"Table sample values in CSV format with header:\n\n{sample_values_string}"
                )
            )
        elif isinstance(sample_values_result, MetricErrorResult):
            messages.append(
                HumanMessage(
                    content=f"Could not compute sample values: {sample_values_result.value.exception_message}"
                )
            )

        return GenerateExpectationsState(
            organization_id=state.organization_id,
            data_source_name=state.data_source_name,
            data_asset_name=state.data_asset_name,
            batch_definition_name=state.batch_definition_name,
            batch_parameters=state.batch_parameters,
            existing_expectation_contexts=state.existing_expectation_contexts,
            messages=messages,
            batch_definition=batch_definition,
            potential_expectations=[],
            expectations=[],
        )


def schema_metric_to_csv_string(result: BatchColumnTypesResult) -> str:
    """Convert a BatchColumnTypesResult into a CSV string with column names and types.

    Args:
        result: The BatchColumnTypesResult containing column type information

    Returns:
        A CSV string with header row and data rows for each column
    """
    header = "column_name,column_type"
    rows = [f"{col.name},{col.type}" for col in result.value]
    return "\n".join([header, *rows])


def sample_values_metric_to_ai_readable_message(result: SampleValuesResult) -> str:
    """Convert a SampleValuesResult into a CSV string.

    Args:
        result: The SampleValuesResult containing sample data

    Returns:
        A CSV string with header row and data rows
    """
    if result.value.empty:
        return ""

    output: str = result.value.to_csv(index=False, lineterminator="\n")
    return output.rstrip("\n")
