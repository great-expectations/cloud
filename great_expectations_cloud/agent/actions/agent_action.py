from abc import abstractmethod
from typing import Generic, Sequence, TypeVar

from great_expectations.compatibility.pydantic import BaseModel
from great_expectations.data_context import CloudDataContext

from great_expectations_cloud.agent.models import CreatedResource, Event


class ActionResult(BaseModel):  # type: ignore[misc] # BaseSettings is has Any type
    id: str
    type: str
    created_resources: Sequence[CreatedResource]


_EventT = TypeVar("_EventT", bound=Event)


class AgentAction(Generic[_EventT]):
    def __init__(self, context: CloudDataContext):
        self._context = context

    @abstractmethod
    def run(self, event: _EventT, id: str) -> ActionResult:
        ...
