from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
    RunScheduledCheckpointEvent,
    RunWindowCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class RunCheckpointAction(AgentAction[RunCheckpointEvent]):
    @override
    def run(self, event: RunCheckpointEvent, id: str) -> ActionResult:
        return run_checkpoint_v1(self._context, event, id)


def run_checkpoint_v1(
    context: CloudDataContext,
    event: RunCheckpointEvent | RunScheduledCheckpointEvent | RunWindowCheckpointEvent,
    id: str,
    expectation_parameters: dict | None = None,
) -> ActionResult:
    """Note: the logic for this action is broken out into this function so that
    the same logic can be used for both RunCheckpointEvent and RunScheduledCheckpointEvent."""
    # TODO: move connection testing into OSS; there isn't really a reason it can't be done there (this TODO is copied from the v0 action, applies here as well).
    for datasource_name, data_asset_names in event.datasource_names_to_asset_names.items():
        datasource = context.get_datasource(datasource_name)
        datasource.test_connection(test_assets=False)  # raises `TestConnectionError` on failure
        for (
            data_asset_name
        ) in data_asset_names:  # only test connection for assets that are validated in checkpoint
            asset = datasource.get_asset(data_asset_name)
            asset.test_connection()  # raises `TestConnectionError` on failure

    if expectation_parameters is None:
        expectation_parameters = {}

    # get checkpoint
    if not event.checkpoint_name:
        message = "RunCheckpointAction encountered an error while running the checkpoint. The checkpoint_name is missing in the event. In order to run a checkpoint, the checkpoint_name must be provided."
        raise GXAgentError(message)

    checkpoint = context.checkpoints.get(event.checkpoint_name)

    if not checkpoint:
        message = f"RunCheckpointAction encountered an error while running the checkpoint. The checkpoint with name {event.checkpoint_name} does not exist."
        raise GXAgentError(message)

    # run checkpoint
    checkpoint_run_result = checkpoint.run(
        # TODO: If Batch parameters are not set, each Validation Definition will run on the default Batch determined by its Batch Definition. See https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/run_a_checkpoint
        # TODO: It is likely that we want to set batch_parameters
        expectation_parameters=expectation_parameters,
    )

    # TODO: The below is not validated with v1 and was copied over from the v0 implementation. It may need to be updated.
    validation_results = checkpoint_run_result.run_results
    created_resources = []
    for key in validation_results.keys():
        created_resource = CreatedResource(
            resource_id=validation_results[key]["actions_results"]["store_validation_result"]["id"],
            type="SuiteValidationResult",
        )
        created_resources.append(created_resource)

    return ActionResult(
        id=id,
        type=event.type,
        created_resources=created_resources,
    )


register_event_action("1", RunCheckpointEvent, RunCheckpointAction)
