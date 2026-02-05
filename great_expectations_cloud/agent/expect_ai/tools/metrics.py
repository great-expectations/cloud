from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pydantic
from great_expectations.core.batch_definition import BatchDefinition, PartitionerT
from great_expectations.metrics import (
    BatchColumnTypes,
    BatchRowCount,
    ColumnDescriptiveStats,
    ColumnDistinctValues,
    ColumnDistinctValuesCount,
    ColumnNullCount,
    ColumnSampleValues,
    ColumnValuesMatchRegexCount,
    ColumnValuesMatchRegexValues,
    ColumnValuesNotMatchRegexCount,
    ColumnValuesNotMatchRegexValues,
    SampleValues,
)
from langchain_core.tools import Tool

from great_expectations_cloud.agent.expect_ai.metric_service import MetricNotComputableError

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent.interfaces import (
        DataAsset,
        Datasource,
    )
    from great_expectations.execution_engine import ExecutionEngine

    from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import BatchParameters
    from great_expectations_cloud.agent.expect_ai.metric_service import MetricService


class NoArgs(pydantic.BaseModel):
    pass


class ColumnArgs(pydantic.BaseModel):
    column: str


# Note that we are hiding the count parameters and forcing the model to use
# the defaults. Default parameters are not allowed in tools.
class ColumnRegexValuesArgs(pydantic.BaseModel):
    column: str
    regex: str


class AgentToolsManager:
    def __init__(
        self,
        context: CloudDataContext,
        metric_service: MetricService,
        distinct_values_str_length_limit: int = 10000,
    ):
        self._context = context
        self._metric_service = metric_service
        self._data_sources: dict[str, Datasource[DataAsset[Any, Any], ExecutionEngine[Any]]] = {}
        self._tools: dict[str, list[Tool]] = {}
        self._distinct_values_str_length_limit = distinct_values_str_length_limit

    def get_tools(self, data_source_name: str) -> list[Tool]:
        if data_source_name not in self._tools:
            self._tools[data_source_name] = self._create_tools_with_core_metrics(data_source_name)

        return self._tools[data_source_name]

    def _create_tools_with_core_metrics(self, data_source_name: str) -> list[Tool]:
        metric_service = self._metric_service

        table_schema_metric_tool = Tool(
            name=BatchColumnTypes.__name__,
            description=BatchColumnTypes.__doc__ or "",
            args_schema=NoArgs,
            func=lambda batch_definition, batch_parameters: (
                metric_service.get_metric_value_or_error_text(
                    metric=BatchColumnTypes(),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            ),
        )

        table_row_count_metric_tool = Tool(
            name=BatchRowCount.__name__,
            description=BatchRowCount.__doc__ or "",
            args_schema=NoArgs,
            func=lambda batch_definition, batch_parameters: (
                metric_service.get_metric_value_or_error_text(
                    metric=BatchRowCount(),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            ),
        )

        sample_values_metric_tool = Tool(
            name=SampleValues.__name__,
            description=SampleValues.__doc__ or "",
            args_schema=NoArgs,
            func=lambda batch_definition, batch_parameters: (
                metric_service.get_metric_value_or_error_text(
                    metric=SampleValues(),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            ),
        )

        column_sample_values_metric_tool = Tool(
            name=ColumnSampleValues.__name__,
            description=ColumnSampleValues.__doc__ or "",
            args_schema=ColumnArgs,
            func=lambda batch_definition, batch_parameters, column: {
                ColumnSampleValues.__name__: metric_service.get_metric_value_or_error_text(
                    metric=ColumnSampleValues(column=column),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                ),
            },
        )

        column_null_count_metric_tool = Tool(
            name=ColumnNullCount.__name__,
            description=ColumnNullCount.__doc__ or "",
            args_schema=ColumnArgs,
            func=lambda batch_definition, batch_parameters, column: (
                metric_service.get_metric_value_or_error_text(
                    metric=ColumnNullCount(column=column),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            ),
        )

        column_distinct_values_count_metric_tool = Tool(
            name=ColumnDistinctValuesCount.__name__,
            description=ColumnDistinctValuesCount.__doc__ or "",
            args_schema=ColumnArgs,
            func=lambda batch_definition, batch_parameters, column: (
                metric_service.get_metric_value_or_error_text(
                    metric=ColumnDistinctValuesCount(column=column),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            ),
        )

        def _get_distinct_values_and_check_length(
            batch_definition: BatchDefinition[PartitionerT],
            batch_parameters: BatchParameters,
            column: str,
        ) -> dict[str, list[str] | str]:
            try:
                distinct_values = metric_service.get_metric_value(
                    metric=ColumnDistinctValues(column=column),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            except MetricNotComputableError as e:
                return {"ColumnDistinctValues": str(e)}
            # If the number of distinct values is too large, simply
            # including the list of values could consume all the
            # tokens available to the model.
            if len(str(distinct_values)) > self._distinct_values_str_length_limit:
                return {
                    "ColumnDistinctValues": f"Too many distinct values ({len(distinct_values)})"
                }
            return {"ColumnDistinctValues": distinct_values}

        column_distinct_values_metric_tool = Tool(
            name=ColumnDistinctValues.__name__,
            description=ColumnDistinctValues.__doc__ or "",
            args_schema=ColumnArgs,
            func=_get_distinct_values_and_check_length,
        )

        column_values_match_regex_values_metric_tool = Tool(
            name=ColumnValuesMatchRegexValues.__name__,
            description=ColumnValuesMatchRegexValues.__doc__ or "",
            args_schema=ColumnRegexValuesArgs,
            func=lambda batch_definition, batch_parameters, column, regex: {
                ColumnValuesMatchRegexValues.__name__: metric_service.get_metric_value_or_error_text(
                    metric=ColumnValuesMatchRegexValues(column=column, regex=regex),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                ),
            },
        )

        column_values_match_regex_count_metric_tool = Tool(
            name=ColumnValuesMatchRegexCount.__name__,
            description=ColumnValuesMatchRegexCount.__doc__ or "",
            args_schema=ColumnRegexValuesArgs,
            func=lambda batch_definition, batch_parameters, column, regex: (
                metric_service.get_metric_value_or_error_text(
                    metric=ColumnValuesMatchRegexCount(
                        column=column,
                        regex=regex,
                    ),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            ),
        )

        column_values_not_match_regex_values_metric_tool = Tool(
            name=ColumnValuesNotMatchRegexValues.__name__,
            description=ColumnValuesNotMatchRegexValues.__doc__ or "",
            args_schema=ColumnRegexValuesArgs,
            func=lambda batch_definition, batch_parameters, column, regex: {
                ColumnValuesNotMatchRegexValues.__name__: metric_service.get_metric_value_or_error_text(
                    metric=ColumnValuesNotMatchRegexValues(
                        column=column,
                        regex=regex,
                    ),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            },
        )

        column_values_not_match_regex_count_metric_tool = Tool(
            name=ColumnValuesNotMatchRegexCount.__name__,
            description=ColumnValuesNotMatchRegexCount.__doc__ or "",
            args_schema=ColumnRegexValuesArgs,
            func=lambda batch_definition, batch_parameters, column, regex: (
                metric_service.get_metric_value_or_error_text(
                    metric=ColumnValuesNotMatchRegexCount(
                        column=column,
                        regex=regex,
                    ),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            ),
        )

        column_descriptive_stats_metric_tool = Tool(
            name=ColumnDescriptiveStats.__name__,
            description=ColumnDescriptiveStats.__doc__ or "",
            args_schema=ColumnArgs,
            func=lambda batch_definition, batch_parameters, column: (
                metric_service.get_metric_value_or_error_text(
                    metric=ColumnDescriptiveStats(column=column),
                    batch_definition=self._ensure_core_batch_definition(batch_definition),
                    batch_parameters=batch_parameters,
                )
            ),
        )

        return [
            table_schema_metric_tool,
            table_row_count_metric_tool,
            sample_values_metric_tool,
            column_sample_values_metric_tool,
            column_null_count_metric_tool,
            column_distinct_values_count_metric_tool,
            column_distinct_values_metric_tool,
            column_values_match_regex_values_metric_tool,
            column_values_match_regex_count_metric_tool,
            column_values_not_match_regex_values_metric_tool,
            column_values_not_match_regex_count_metric_tool,
            column_descriptive_stats_metric_tool,
        ]

    def _ensure_core_batch_definition(
        self,
        batch_definition: BatchDefinition[PartitionerT],
    ) -> BatchDefinition[PartitionerT]:
        if not isinstance(batch_definition, BatchDefinition):
            raise ExpectedCoreBatchDefinitionError(batch_definition)
        return batch_definition


class ExpectedCoreBatchDefinitionError(ValueError):
    def __init__(self, batch_definition: Any):
        super().__init__(
            f"Expected a core batch definition, but got {batch_definition.__class__.__name__}"
        )
