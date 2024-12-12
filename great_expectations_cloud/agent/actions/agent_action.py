from __future__ import annotations

import datetime
from abc import abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Generic, Optional, TypeVar
from uuid import UUID

from pydantic.v1 import BaseModel

from great_expectations_cloud.agent.models import CreatedResource, Event

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class ActionResult(BaseModel):
    id: str
    type: str
    created_resources: Sequence[CreatedResource]
    job_duration: Optional[datetime.timedelta] = None  # noqa: UP007  # Python 3.8 doesn't support `X | Y` for type annotation


_EventT = TypeVar("_EventT", bound=Event)


class AgentAction(Generic[_EventT]):
    def __init__(
        self, context: CloudDataContext, base_url: str, organization_id: UUID, auth_key: str
    ):
        self._context = context
        self._base_url = base_url
        self._organization_id = organization_id
        self._auth_key = auth_key

    @abstractmethod
    def run(self, event: _EventT, id: str) -> ActionResult: ...
