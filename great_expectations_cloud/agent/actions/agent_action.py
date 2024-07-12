from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, Sequence, TypeVar

from pydantic.v1 import BaseModel

from great_expectations_cloud.agent.models import CreatedResource, Event

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class ActionResult(BaseModel):
    id: str
    type: str
    created_resources: Sequence[CreatedResource]
    job_duration_milliseconds: float


_EventT = TypeVar("_EventT", bound=Event)


class AgentAction(Generic[_EventT]):
    def __init__(self, context: CloudDataContext):
        self._context = context

    @abstractmethod
    def run(self, event: _EventT, id: str) -> ActionResult: ...
