from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Annotated, Any, Literal, Optional, Union
from uuid import UUID

from great_expectations.expectations.metadata_types import DataQualityIssues
from great_expectations.experimental.metric_repository.metrics import MetricTypes
from pydantic.v1 import BaseModel, Extra, Field

from great_expectations_cloud.agent.exceptions import GXCoreError


def all_subclasses(cls: type) -> list[type]:
    """
    Recursively gather every subclass of `cls` (including nested ones).
    """
    direct = cls.__subclasses__()
    all_sub_cls: list[type] = []
    for C in direct:
        all_sub_cls.append(C)
        all_sub_cls.extend(all_subclasses(C))
    return all_sub_cls


class AgentBaseExtraForbid(BaseModel):
    class Config:
        # 2024-03-04: ZEL-501 Strictly enforce models for handling outdated APIs
        extra: str = Extra.forbid


class AgentBaseExtraIgnore(BaseModel):
    class Config:
        # Extra fields on Events are not strictly enforced
        extra: str = Extra.ignore


class EventBase(AgentBaseExtraIgnore):
    type: str
    organization_id: Optional[UUID] = None  # noqa: UP045


class ScheduledEventBase(EventBase):
    schedule_id: UUID


class RunDataAssistantEvent(EventBase):
    type: str
    datasource_name: str
    data_asset_name: str
    expectation_suite_name: Optional[str] = None  # noqa: UP045


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
    datasource_names_to_asset_names: dict[str, set[str]]
    checkpoint_id: uuid.UUID
    splitter_options: Optional[dict[str, Any]] = None  # noqa: UP045
    # TODO: Remove optional once fully migrated to greatexpectations v1
    checkpoint_name: Optional[str] = None  # noqa: UP045


class RunScheduledCheckpointEvent(ScheduledEventBase):
    type: Literal["run_scheduled_checkpoint.received"] = "run_scheduled_checkpoint.received"
    datasource_names_to_asset_names: dict[str, set[str]]
    checkpoint_id: uuid.UUID
    splitter_options: Optional[dict[str, Any]] = None  # noqa: UP045
    # TODO: Remove optional once fully migrated to greatexpectations v1
    checkpoint_name: Optional[str] = None  # noqa: UP045


class RunWindowCheckpointEvent(EventBase):
    type: Literal["run_window_checkpoint.received"] = "run_window_checkpoint.received"
    datasource_names_to_asset_names: dict[str, set[str]]
    checkpoint_id: uuid.UUID
    splitter_options: Optional[dict[str, Any]] = None  # noqa: UP045
    # TODO: Remove optional once fully migrated to greatexpectations v1
    checkpoint_name: Optional[str] = None  # noqa: UP045


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


class ListAssetNamesEvent(EventBase):
    type: Literal["list_table_names_request.received"] = "list_table_names_request.received"
    datasource_name: str


class DraftDatasourceConfigEvent(EventBase):
    type: Literal["test_datasource_config"] = "test_datasource_config"
    config_id: UUID


class GenerateDataQualityCheckExpectationsEvent(EventBase):
    type: Literal["generate_data_quality_check_expectations_request.received"] = (
        "generate_data_quality_check_expectations_request.received"
    )
    datasource_name: str
    data_assets: Sequence[str]
    selected_data_quality_issues: Sequence[DataQualityIssues] | None = None
    use_forecast: bool = False  # feature flag
    created_via: str | None = None


class RunRdAgentEvent(EventBase):
    type: Literal["rd_agent_action.received"] = "rd_agent_action.received"
    datasource_name: str
    data_asset_name: str
    batch_definition_name: str
    batch_parameters: Optional[dict[str, Any]] = None  # noqa: UP045
    use_core_metrics: bool = False


class UnknownEvent(AgentBaseExtraForbid):
    type: Literal["unknown_event"] = "unknown_event"


class MissingEventSubclasses(RuntimeError):
    def __init__(self) -> None:
        super().__init__("No valid Event subclasses found")


# Type alias for any event class that can be used in the dynamic system
EventType = type[Union[AgentBaseExtraForbid, AgentBaseExtraIgnore]]


#
# Dynamically build Event union from all subclasses of AgentBaseExtraForbid and AgentBaseExtraIgnore
#
def _build_event_union() -> tuple[type, ...]:
    """Build a discriminated Union of all Event subclasses dynamically."""
    # Collect all subclasses from both base classes
    forbid_subs = all_subclasses(AgentBaseExtraForbid)
    ignore_subs = all_subclasses(AgentBaseExtraIgnore)

    # Combine and filter to only include classes with a 'type' field and a discriminator value
    all_event_classes = []
    for cls in forbid_subs + ignore_subs:
        # Check if the class has a 'type' field and it's properly defined with a Literal type
        if hasattr(cls, "__fields__") and "type" in cls.__fields__:
            type_field = cls.__fields__["type"]
            # Check if it has a default value (discriminator value)
            if hasattr(type_field, "default") and type_field.default is not None:
                all_event_classes.append(cls)

    if not all_event_classes:
        raise MissingEventSubclasses()

    # Remove duplicates (preserves order for deterministic results)
    return tuple(dict.fromkeys(all_event_classes))


# Build the dynamic Event union
_event_classes = _build_event_union()

if TYPE_CHECKING:
    # For static type checking, provide a concrete union of known event types
    Event = Union[
        RunOnboardingDataAssistantEvent,
        RunMissingnessDataAssistantEvent,
        RunCheckpointEvent,
        RunScheduledCheckpointEvent,
        RunWindowCheckpointEvent,
        RunColumnDescriptiveMetricsEvent,
        RunMetricsListEvent,
        DraftDatasourceConfigEvent,
        ListAssetNamesEvent,
        GenerateDataQualityCheckExpectationsEvent,
        RunRdAgentEvent,
        UnknownEvent,
    ]
else:
    # At runtime, use the dynamic union
    Event = Annotated[Union[_event_classes], Field(discriminator="type")]


def reload_event_union() -> None:
    """Rebuild the Event union dynamically.

    Call this method after subclassing one of the EventBase models in order
    to make it available in the Event union."""
    global Event  # noqa: PLW0603
    reloaded_event_classes = _build_event_union()
    Event = Annotated[Union[reloaded_event_classes], Field(discriminator="type")]  # type: ignore[valid-type]


def get_event_union() -> Any:
    """Canonical way to access the Event union for non-typing use cases.

    The Event union can be dynamically extended when the Agent codebase is extended.
    Those subclasses will be defined after the Event model is defined in this module.
    This function allows callers to access the full union that includes those subclasses.

    Returns:
        A dynamically constructed discriminated Union type suitable for pydantic parsing.

    Type Bounds:
        All members of the returned Union are subclasses of either:
        - AgentBaseExtraForbid: Event classes with strict field validation
        - AgentBaseExtraIgnore: Event classes that ignore extra fields

        The actual return type is equivalent to:
        Annotated[Union[<all_event_subclasses>], Field(discriminator="type")]

        This cannot be statically typed due to the dynamic discovery of subclasses,
        so we use Any. At runtime, this represents a properly typed
        discriminated union of concrete event model classes.
    """
    reload_event_union()
    return Event


class CreatedResource(AgentBaseExtraForbid):
    resource_id: str
    type: str


class JobStarted(AgentBaseExtraForbid):
    status: Literal["started"] = "started"


class JobCompleted(AgentBaseExtraForbid):
    status: Literal["completed"] = "completed"
    success: bool
    created_resources: Sequence[CreatedResource] = []
    error_stack_trace: Union[str, None] = None
    error_code: Union[str, None] = None
    error_params: Union[dict[str, str], None] = None
    processed_by: Union[Literal["agent", "runner"], None] = None


JobStatus = Union[JobStarted, JobCompleted]


class UpdateJobStatusRequest(AgentBaseExtraForbid):
    data: JobStatus


class CreateScheduledJobAndSetJobStarted(AgentBaseExtraForbid):
    type: Literal["run_scheduled_checkpoint.received"] = "run_scheduled_checkpoint.received"
    correlation_id: UUID
    schedule_id: UUID
    checkpoint_id: UUID
    datasource_names_to_asset_names: dict[str, set[str]]
    splitter_options: Optional[dict[str, Any]] = None  # noqa: UP045
    # TODO: Remove optional once fully migrated to greatexpectations v1
    checkpoint_name: Optional[str] = None  # noqa: UP045


class CreateScheduledJobAndSetJobStartedRequest(AgentBaseExtraForbid):
    data: CreateScheduledJobAndSetJobStarted


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
