from __future__ import annotations

from great_expectations.core.http import create_session
from great_expectations.exceptions import GXCloudError
from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.actions.run_checkpoint import run_checkpoint_v0
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    RunWindowCheckpointEvent,
)


class RunWindowCheckpointAction(AgentAction[RunWindowCheckpointEvent]):
    # TODO: New actions need to be created that are compatible with GX v1 and registered for v1.
    #  This action is registered for v0, see register_event_action()

    @override
    def run(self, event: RunWindowCheckpointEvent, id: str) -> ActionResult:
        with create_session(access_token=self._auth_key) as session:
            expectation_parameters_for_checkpoint_url = f"{self._base_url}api/v1/organizations/"
            f"{self._organization_id}/checkpoints/{event.checkpoint_id}/expectation-parameters"
            response = session.get(url=expectation_parameters_for_checkpoint_url)

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

        # Note: In v0 expectation_parameters are called evaluation_parameters.
        return run_checkpoint_v0(
            self._context, event, id, evaluation_parameters=expectation_parameters
        )


register_event_action("0", RunWindowCheckpointEvent, RunWindowCheckpointAction)
