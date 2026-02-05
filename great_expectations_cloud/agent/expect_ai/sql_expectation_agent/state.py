from __future__ import annotations

import os
from typing import Annotated, Any, TypedDict

from great_expectations.core.batch_definition import BatchDefinition, PartitionerT
from great_expectations.core.partitioners import ColumnPartitioner
from great_expectations.metrics.batch.batch_column_types import BatchColumnTypesResult
from great_expectations.metrics.batch.sample_values import SampleValuesResult
from great_expectations.metrics.metric_results import MetricErrorResult
from langchain_core.messages import BaseMessage
from langgraph.graph import add_messages
from pydantic import BaseModel, Field, field_serializer, field_validator
from pydantic.v1 import BaseModel as BaseModelV1

from great_expectations_cloud.agent.expect_ai.exceptions import InvalidBatchDefinitionError


def _get_organization_id_from_env() -> str:
    return os.getenv("GX_CLOUD_ORGANIZATION_ID", "")


def _get_workspace_id_from_env() -> str:
    return os.getenv("GX_CLOUD_WORKSPACE_ID", "")


def _allow_null_overrides(a: str | None, b: str | None) -> str | None:
    """Hack to force langgraph's state management to allow setting fields to None."""
    return None if b == "__unset__" else b


class SqlExpectationInput(BaseModel):
    """"""

    organization_id: str = Field(default_factory=_get_organization_id_from_env)
    workspace_id: str = Field(default_factory=_get_workspace_id_from_env)
    user_prompt: str
    data_source_name: str
    data_asset_name: str
    batch_definition_name: str


class SqlExpectationOutput(BaseModel):
    sql: str
    description: str


class SqlExpectationState(SqlExpectationInput):
    # TODO: Provide a non-Any type for batch_definition when Core uses pydantic v2 models.
    # This currently is required because of of mismatches between pydantic v1 and v2 models.
    batch_definition: Any
    messages: Annotated[list[BaseMessage], add_messages]
    potential_sql: str | None = None
    potential_description: str | None = None

    # SQL validation tracking fields
    success: bool | None = None
    error: Annotated[str | None, _allow_null_overrides] = None
    sql_validation_attempts: int = 0

    @field_validator("batch_definition", mode="before")
    @classmethod
    def parse_batch(cls, v: Any) -> BatchDefinition[PartitionerT]:
        if isinstance(v, BatchDefinition):
            return v
        raise InvalidBatchDefinitionError(type(v), BatchDefinition[PartitionerT])

    @field_serializer("batch_definition")
    def serialize_batch(
        self,
        v: BatchDefinition[PartitionerT],
        _info: Any,
    ) -> Any:
        if isinstance(v, BatchDefinition):
            return v.dict()
        raise InvalidBatchDefinitionError(type(v), BatchDefinition[PartitionerT])


class SqlQueryResponse(BaseModel):
    """Response model for SQL query generation and rewriting operations."""

    potential_sql: str
    potential_description: str


class SqlExpectationConfig(TypedDict):
    temperature: float
    seed: int | None
    thread_id: str


class SqlAndDescriptionResponse(BaseModel):
    sql: str = Field(..., description="A SQL query that returns the rows that are unexpected")
    description: str = Field(
        ...,
        description="A description of the query that is less than 75 characters long starting with 'Expect'",
    )


class CoreMetrics(BaseModelV1):
    # Pydantic doesn't allow using models from v1 and v2 in the same object.
    # GX still uses pydantic v1 and this class contains GX objects,
    # so it must be built from the v1 BaseModel.
    # This works because we aren't passing it through LangChain, which requires v2 objects.
    batch_definition: BatchDefinition[ColumnPartitioner]
    sql_dialect: str
    table_name: str
    schema_result: BatchColumnTypesResult | MetricErrorResult
    sample_values_result: SampleValuesResult | MetricErrorResult
