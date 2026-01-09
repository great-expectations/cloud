from __future__ import annotations

import logging
import socket
from typing import TYPE_CHECKING, Final
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
from great_expectations_cloud.agent.models import RunScheduledCheckpointEvent

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class RunScheduledCheckpointAction(AgentAction[RunScheduledCheckpointEvent]):
    @override
    def run(self, event: RunScheduledCheckpointEvent, id: str) -> ActionResult:
        expectation_parameters_url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._domain_context.organization_id}/workspaces/{self._domain_context.workspace_id}/checkpoints/{event.checkpoint_id}/expectation-parameters",
        )
        return run_scheduled_checkpoint(
            context=self._context,
            event=event,
            id=id,
            auth_key=self._auth_key,
            url=expectation_parameters_url,
        )


def run_scheduled_checkpoint(
    context: CloudDataContext, event: RunScheduledCheckpointEvent, id: str, auth_key: str, url: str
) -> ActionResult:
    """Run a scheduled checkpoint, fetching expectation parameters from GX Cloud first."""
    hostname = socket.gethostname()
    log_extra = {
        "correlation_id": id,
        "checkpoint_id": str(event.checkpoint_id),
        "schedule_id": str(event.schedule_id),
        "hostname": hostname,
    }

    LOGGER.debug("Fetching expectation parameters from GX Cloud", extra=log_extra)
    with create_session(access_token=auth_key) as session:
        response = session.get(url=url)

    if not response.ok:
        LOGGER.error(
            "Failed to fetch expectation parameters",
            extra={**log_extra, "response_status": response.status_code},
        )
        raise GXCloudError(
            message=f"RunScheduledCheckpointAction encountered an error while connecting to GX Cloud. "
            f"Unable to retrieve expectation_parameters for Checkpoint with ID={event.checkpoint_id}.",
            response=response,
        )
    data = response.json()
    LOGGER.debug("Expectation parameters fetched successfully", extra=log_extra)

    try:
        expectation_parameters = (
            data["data"]["expectation_parameters"]
            if len(data["data"]["expectation_parameters"]) > 0
            else None
        )
    except KeyError as e:
        LOGGER.exception("Malformed response from GX Cloud", extra=log_extra)
        raise GXCloudError(
            message="Malformed response received from GX Cloud",
            response=response,
        ) from e

    LOGGER.debug(
        "Proceeding to run checkpoint",
        extra={**log_extra, "has_expectation_parameters": expectation_parameters is not None},
    )
    return run_checkpoint(context, event, id, expectation_parameters=expectation_parameters)


register_event_action("1", RunScheduledCheckpointEvent, RunScheduledCheckpointAction)
