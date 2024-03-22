from __future__ import annotations

import uuid
from typing import Any, Dict, Literal, Optional, Sequence, Set, Union
from uuid import UUID

from great_expectations.compatibility.pydantic import BaseModel, Extra, Field
from great_expectations.experimental.metric_repository.metrics import MetricTypes
from typing_extensions import Annotated

from great_expectations_cloud.agent.exceptions import GXCoreError


class AgentBaseModel(BaseModel):  # type: ignore[misc] # BaseSettings is has Any type
    class Config:
        # 2024-03-04: ZEL-501 Strictly enforce models for handling outdated APIs
        extra: str = Extra.forbid


class EventBase(AgentBaseModel):
    type: str


class RunDataAssistantEvent(EventBase):
    type: str
    datasource_name: str
    data_asset_name: str
    expectation_suite_name: Optional[str] = None


class RunOnboardingDataAssistantEvent(RunDataAssistantEvent):
    type: Literal["onboarding_data_assistant_request.received"] = (
        "onboarding_data_assistant_request.received"
    )


class RunMissingnessDataAssistantEvent(RunDataAssistantEvent):
    type: Literal["missingness_data_assistant_request.received"] = (
        "missingness_data_assistant_request.received"
    )


class RunCheckpointEvent(EventBase):
    type: Literal["run_checkpoint_request"] = "run_checkpoint_request"
    datasource_names_to_asset_names: Dict[str, Set[str]]
    checkpoint_id: uuid.UUID
    splitter_options: Optional[Dict[str, Any]] = None


class RunColumnDescriptiveMetricsEvent(EventBase):
    type: Literal["column_descriptive_metrics_request.received"] = (
        "column_descriptive_metrics_request.received"
    )
    datasource_name: str
    data_asset_name: str


class RunMetricsListEvent(EventBase):
    type: Literal["metrics_list_request.received"] = "metrics_list_request.received"
    datasource_name: str
    data_asset_name: str
    metric_names: Sequence[MetricTypes]


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
        RunMetricsListEvent,
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
    error_code: Union[str, None] = None
    error_params: Union[Dict[str, str], None] = None


JobStatus = Union[JobStarted, JobCompleted]


def build_failed_job_completed_status(error: BaseException) -> JobCompleted:
    if isinstance(error, GXCoreError):
        status = JobCompleted(
            success=False,
            error_stack_trace=str(error),
            error_code=error.error_code,
            error_params=error.get_error_params(),
        )
    else:
        status = JobCompleted(success=False, error_stack_trace=str(error))

    return status
