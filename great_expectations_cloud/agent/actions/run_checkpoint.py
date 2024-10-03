from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.event_handler import register_event_action
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
        return run_checkpoint(self._context, event, id)


class MissingCheckpointNameError(ValueError):
    """Property checkpoint_name is required but not present."""


def run_checkpoint(
    context: CloudDataContext,
    event: RunCheckpointEvent | RunScheduledCheckpointEvent | RunWindowCheckpointEvent,
    id: str,
) -> ActionResult:
    """Note: the logic for this action is broken out into this function so that
    the same logic can be used for both RunCheckpointEvent and RunScheduledCheckpointEvent."""
    # TODO: move connection testing into OSS; there isn't really a reason it can't be done there
    if not event.checkpoint_name:
        raise MissingCheckpointNameError
    test_connection_to_data_source_and_asset(context, event)
    checkpoint = context.checkpoints.get(name=event.checkpoint_name)
    checkpoint_run_result = checkpoint.run(
        batch_parameters=event.splitter_options,
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


def test_connection_to_data_source_and_asset(
    context: CloudDataContext,
    event: RunCheckpointEvent | RunScheduledCheckpointEvent | RunWindowCheckpointEvent,
) -> None:
    for datasource_name, data_asset_names in event.datasource_names_to_asset_names.items():
        datasource = context.get_datasource(datasource_name)
        datasource.test_connection(test_assets=False)  # raises `TestConnectionError` on failure
        for (
            data_asset_name
        ) in data_asset_names:  # only test connection for assets that are validated in checkpoint
            asset = datasource.get_asset(data_asset_name)
            asset.test_connection()  # raises `TestConnectionError` on failure


register_event_action("0", RunCheckpointEvent, RunCheckpointAction)
