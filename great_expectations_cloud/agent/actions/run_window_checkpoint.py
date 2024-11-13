from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urljoin

from great_expectations.core.http import create_session
from great_expectations.exceptions import GXCloudError
from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.actions.run_checkpoint import run_checkpoint
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    RunWindowCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class RunWindowCheckpointAction(AgentAction[RunWindowCheckpointEvent]):
    @override
    def run(self, event: RunWindowCheckpointEvent, id: str) -> ActionResult:
        expectation_parameters_url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._organization_id}/checkpoints/{event.checkpoint_id}/expectation-parameters",
        )
        return run_window_checkpoint(
            self._context,
            event,
            id,
            auth_key=self._auth_key,
            url=expectation_parameters_url,
        )


register_event_action("1", RunWindowCheckpointEvent, RunWindowCheckpointAction)


def run_window_checkpoint(
    context: CloudDataContext,
    event: RunWindowCheckpointEvent,
    id: str,
    auth_key: str,
    url: str,
) -> ActionResult:
    with create_session(access_token=auth_key) as session:
        response = session.get(url=url)

    if not response.ok:
        raise GXCloudError(
            message=f"RunWindowCheckpointAction encountered an error while connecting to GX Cloud. "
            f"Unable to retrieve expectation_parameters for Checkpoint with ID={event.checkpoint_id}.",
            response=response,
        )
    data = response.json()
    try:
        expectation_parameters = data["data"]["expectation_parameters"]
    except KeyError as e:
        raise GXCloudError(
            message="Malformed response received from GX Cloud",
            response=response,
        ) from e

    return run_checkpoint(context, event, id, expectation_parameters=expectation_parameters)
