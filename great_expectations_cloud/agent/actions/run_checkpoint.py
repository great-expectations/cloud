from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations.checkpoint.configurator import ActionDict
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
    RunScheduledCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class RunCheckpointAction(AgentAction[RunCheckpointEvent]):
    # TODO: New actions need to be created that are compatible with GX v1 and registered for v1.
    #  This action is registered for v0, see register_event_action()

    @override
    def run(self, event: RunCheckpointEvent, id: str) -> ActionResult:
        return run_checkpoint(self._context, event, id)


def run_checkpoint(
    context: CloudDataContext,
    event: RunCheckpointEvent | RunScheduledCheckpointEvent,
    id: str,
) -> ActionResult:
    """Note: the logic for this action is broken out into this function so that
    the same logic can be used for both RunCheckpointEvent and RunScheduledCheckpointEvent."""
    # TODO: move connection testing into OSS; there isn't really a reason it can't be done there
    for datasource_name, data_asset_names in event.datasource_names_to_asset_names.items():
        datasource = context.get_datasource(datasource_name)
        datasource.test_connection(test_assets=False)  # raises `TestConnectionError` on failure
        for (
            data_asset_name
        ) in data_asset_names:  # only test connection for assets that are validated in checkpoint
            asset = datasource.get_asset(data_asset_name)
            asset.test_connection()  # raises `TestConnectionError` on failure

    checkpoint = context.get_checkpoint(ge_cloud_id=str(event.checkpoint_id))
    has_store_checkpoint_action = _checkpoint_has_store_validation_results_action(checkpoint)

    checkpoint_run_result = checkpoint.run(
        batch_request={"options": event.splitter_options} if event.splitter_options else None,
        action_list=(
            None if has_store_checkpoint_action else [_create_store_validation_result_action()]
        ),
    )

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


def _checkpoint_has_store_validation_results_action(checkpoint: Checkpoint) -> bool:
    """
    Checks if the checkpoint has a StoreValidationResultAction in its action_list.

    v0.18 guarantees we have this action.
    v1.0 does not include this action.

    Since we can run checkpoints created in 1.0, we'll need to add this action if it's not present.
    """
    action_class_names = {a.get("action", {}).get("class_name") for a in checkpoint.action_list}
    return "StoreValidationResultAction" in action_class_names


def _create_store_validation_result_action() -> ActionDict:
    return {
        "name": "store_validation_result",
        "action": {
            "class_name": "StoreValidationResultAction",
        },
    }


register_event_action("0", RunCheckpointEvent, RunCheckpointAction)
