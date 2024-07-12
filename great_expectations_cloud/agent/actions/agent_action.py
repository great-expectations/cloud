from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from great_expectations_cloud.agent.models import ActionResult, Event

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


_EventT = TypeVar("_EventT", bound=Event)


class AgentAction(Generic[_EventT]):
    def __init__(self, context: CloudDataContext):
        self._context = context

    @abstractmethod
    def run(self, event: _EventT, id: str) -> ActionResult: ...
