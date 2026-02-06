from __future__ import annotations

import os
from operator import add
from typing import Annotated, Any, TypedDict

from great_expectations.core.batch_definition import BatchDefinition, PartitionerT
from langchain_core.messages import BaseMessage
from langgraph.graph import add_messages
from pydantic import BaseModel, Field, StrictStr, field_serializer, field_validator

from great_expectations_cloud.agent.expect_ai.exceptions import InvalidBatchDefinitionError
from great_expectations_cloud.agent.expect_ai.expectations import (
    AddExpectationsResponse,
    OpenAIGXExpectation,
)


def reduce_add_expectations_response(
    left: AddExpectationsResponse, right: AddExpectationsResponse
) -> AddExpectationsResponse:
    # TODO: deduplicate
    return AddExpectationsResponse(
        rationale=left.rationale + "__" + right.rationale,
        expectations=left.expectations + right.expectations,
    )


class DataQualityPlanComponent(BaseModel):
    title: str
    plan_details: str = Field(
        description="A description of the potential risks to data quality and metrics relevant to defining tests that would catch them."
    )


class DataQualityPlan(BaseModel):
    components: Annotated[list[DataQualityPlanComponent], add]


def _get_organization_id_from_env() -> str:
    return os.getenv("GX_CLOUD_ORGANIZATION_ID", "")


def _get_workspace_id_from_env() -> str:
    return os.getenv("GX_CLOUD_WORKSPACE_ID", "")


BatchParameters = dict[StrictStr, str | int]


class ExistingExpectationContext(BaseModel):
    """Context information about an existing expectation to help prevent redundancy."""

    domain: str = Field(
        description="The domain of the expectation - either the column name or 'table'"
    )
    expectation_type: str = Field(description="The type of the expectation")
    description: str = Field(description="The description of the expectation")


class GenerateExpectationsInput(BaseModel):
    """The input should simply describe the asset for which we want to generate expectations.
    Because the model needs to connect to user data, we also provide sufficient information to connect to the data source."""

    organization_id: str = Field(default_factory=_get_organization_id_from_env)
    workspace_id: str = Field(default_factory=_get_workspace_id_from_env)
    data_source_name: str
    data_asset_name: str
    batch_definition_name: str
    batch_parameters: BatchParameters | None = None
    existing_expectation_contexts: list[ExistingExpectationContext] = Field(default_factory=list)


class ExpectationBuilderOutput(BaseModel):
    potential_expectations: Annotated[list[OpenAIGXExpectation], add]


class SubgraphInput(BaseModel):
    potential_expectations: Annotated[list[OpenAIGXExpectation], add]
    data_source_name: str
    data_asset_name: str


class GenerateExpectationsOutput(BaseModel):
    expectations: Annotated[list[OpenAIGXExpectation], add]


class GenerateExpectationsState(GenerateExpectationsInput):
    # TODO: Provide a non-Any type for batch_definition when Core uses pydantic v2 models.
    # This currently is required because of of mismatches between pydantic v1 and v2 models.
    batch_definition: Any
    messages: Annotated[list[BaseMessage], add_messages]
    data_quality_plan: DataQualityPlan | None = None
    potential_expectations: Annotated[list[OpenAIGXExpectation], add]
    expectations: Annotated[list[OpenAIGXExpectation], add]
    planned_tool_calls: int = 0
    executed_tool_calls: int = 0
    metric_batches_executed: int = 0
    total_turns: int = 0
    executed_tool_signatures: set[str] = Field(default_factory=set)

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


class ExpectationBuilderState(BaseModel):
    plan_development_messages: list[BaseMessage]
    plan_component: DataQualityPlanComponent
    data_source_name: str
    existing_expectation_contexts: list[ExistingExpectationContext] = Field(default_factory=list)


class GenerateExpectationsConfig(TypedDict):
    temperature: float
    seed: int | None
    thread_id: str
