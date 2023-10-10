import uuid
from typing import Literal, Optional, Sequence, Union
from uuid import UUID

from great_expectations.compatibility.pydantic import BaseModel, Extra, Field
from typing_extensions import Annotated


class AgentBaseModel(BaseModel):  # type: ignore[misc] # BaseSettings is has Any type
    class Config:
        extra: str = Extra.forbid


class EventBase(AgentBaseModel):
    type: str


class RunDataAssistantEvent(EventBase):
    type: str
    datasource_name: str
    data_asset_name: str
    expectation_suite_name: Optional[str] = None


class RunOnboardingDataAssistantEvent(RunDataAssistantEvent):
    type: Literal[
        "onboarding_data_assistant_request.received"
    ] = "onboarding_data_assistant_request.received"


class RunMissingnessDataAssistantEvent(RunDataAssistantEvent):
    type: Literal[
        "missingness_data_assistant_request.received"
    ] = "missingness_data_assistant_request.received"


class RunCheckpointEvent(EventBase):
    type: Literal["run_checkpoint_request"] = "run_checkpoint_request"
    checkpoint_id: uuid.UUID


class RunColumnDescriptiveMetricsEvent(EventBase):
    type: Literal[
        "column_descriptive_metrics_request.received"
    ] = "column_descriptive_metrics_request.received"
    datasource_name: str
    data_asset_name: str


class ListTableNamesEvent(EventBase):
    type: Literal["list_table_names_request.received"] = "list_table_names_request.received"
    datasource_name: str


class DraftDatasourceConfigEvent(EventBase):
    type: Literal["test_datasource_config"] = "test_datasource_config"
    config_id: UUID


class UnknownEvent(EventBase):
    type: Literal["unknown_event"] = "unknown_event"


Event = Annotated[
    Union[
        RunOnboardingDataAssistantEvent,
        RunMissingnessDataAssistantEvent,
        RunCheckpointEvent,
        RunColumnDescriptiveMetricsEvent,
        DraftDatasourceConfigEvent,
        ListTableNamesEvent,
        UnknownEvent,
    ],
    Field(discriminator="type"),
]


class CreatedResource(AgentBaseModel):
    resource_id: str
    type: str


class JobStarted(AgentBaseModel):
    status: Literal["started"] = "started"


class JobCompleted(AgentBaseModel):
    status: Literal["completed"] = "completed"
    success: bool
    created_resources: Sequence[CreatedResource] = []
    error_stack_trace: Union[str, None] = None


JobStatus = Union[JobStarted, JobCompleted]
