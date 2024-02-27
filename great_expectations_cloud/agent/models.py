from __future__ import annotations

import uuid
from typing import Any, Dict, Literal, Optional, Sequence, Set, Union, TYPE_CHECKING, ClassVar
from uuid import UUID

from great_expectations.compatibility.pydantic import BaseModel, Extra, Field
from typing_extensions import Annotated

from great_expectations_cloud.agent.actions.draft_datasource_config_action import DraftDatasourceConfigAction

if TYPE_CHECKING:
    from great_expectations_cloud.agent.actions import AgentAction, RunOnboardingDataAssistantAction, \
    RunCheckpointAction, ColumnDescriptiveMetricsAction, ListTableNamesAction, RunMissingnessDataAssistantAction


class AgentBaseModel(BaseModel):  # type: ignore[misc] # BaseSettings is has Any type
    class Config:
        extra: str = Extra.ignore


class EventBase(AgentBaseModel):
    type: str
    action: ClassVar[AgentAction]


class RunDataAssistantEvent(EventBase):
    type: str
    datasource_name: str
    data_asset_name: str
    expectation_suite_name: Optional[str] = None


class RunOnboardingDataAssistantEvent(RunDataAssistantEvent):
    type: Literal[
        "onboarding_data_assistant_request.received"
    ] = "onboarding_data_assistant_request.received"
    action = RunOnboardingDataAssistantAction


class RunMissingnessDataAssistantEvent(RunDataAssistantEvent):
    type: Literal[
        "missingness_data_assistant_request.received"
    ] = "missingness_data_assistant_request.received"
    action = RunMissingnessDataAssistantAction


class RunCheckpointEvent(EventBase):
    type: Literal["run_checkpoint_request"] = "run_checkpoint_request"
    datasource_names_to_asset_names: Dict[str, Set[str]]
    checkpoint_id: uuid.UUID
    splitter_options: Optional[Dict[str, Any]] = None
    action = RunCheckpointAction


class RunColumnDescriptiveMetricsEvent(EventBase):
    type: Literal[
        "column_descriptive_metrics_request.received"
    ] = "column_descriptive_metrics_request.received"
    datasource_name: str
    data_asset_name: str
    action = ColumnDescriptiveMetricsAction


class ListTableNamesEvent(EventBase):
    type: Literal["list_table_names_request.received"] = "list_table_names_request.received"
    datasource_name: str
    action = ListTableNamesAction


class DraftDatasourceConfigEvent(EventBase):
    type: Literal["test_datasource_config"] = "test_datasource_config"
    config_id: UUID
    action = DraftDatasourceConfigAction


class UnknownEvent(EventBase):
    """Noop, returns error"""

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
