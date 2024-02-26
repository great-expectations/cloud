from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import great_expectations as gx
from packaging.version import Version
from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.models import (
    CreatedResource,
    Event,
    RunCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

GX_VERSION = Version(gx.__version__)


# TODO: Placeholder for run_checkpoint registration / implementation. Move to a better place.
def lookup_runner(context: CloudDataContext) -> Callable:  # TODO: Return signature?
    """Lookup the correct runner implementation based on the context version."""

    if Version("0.18.0") <= GX_VERSION < Version("1.0.0"):
        return run_checkpoint_v0_18
    elif GX_VERSION >= Version("1.0.0"):
        return run_checkpoint_v1_0


def run_checkpoint(context: CloudDataContext, event: Event, id: str) -> ActionResult:
    # TODO: Should the lookup be cached?
    checkpoint_runner = lookup_runner(context)
    return checkpoint_runner(context, event, id)


def run_checkpoint_v0_18(context: CloudDataContext, event: Event, id: str) -> ActionResult:
    # TODO: Add try except block only if we plan to catch 0.18 exceptions
    for datasource_name, data_asset_names in event.datasource_names_to_asset_names.items():
        datasource = context.get_datasource(datasource_name)
        datasource.test_connection(test_assets=False)  # raises `TestConnectionError` on failure
        for (
            data_asset_name
        ) in data_asset_names:  # only test connection for assets that are validated in checkpoint
            asset = datasource.get_asset(data_asset_name)
            asset.test_connection()  # raises `TestConnectionError` on failure
    checkpoint_run_result = context.run_checkpoint(
        ge_cloud_id=event.checkpoint_id,
        batch_request={"options": event.splitter_options} if event.splitter_options else None,
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


def run_checkpoint_v1_0(context: CloudDataContext, event: Event, id: str) -> ActionResult:
    # try:
    #     # TODO: Implement and add try except block
    # except CheckpointError as e:  # Catch the exception from 0.18.x. Add more exception types if applicable.
    #     raise GXCoreError(
    #         detail=str(e),
    #         code="checkpoint-error"
    #     ) from e
    # except Exception as e:
    #     # Handle any other exception
    #     raise GXCoreError(
    #         detail=str(e),
    #         code="generic-unhandled-error"
    #     ) from e
    raise NotImplementedError


class RunCheckpointAction(AgentAction[RunCheckpointEvent]):
    @override
    def run(self, event: RunCheckpointEvent, id: str) -> ActionResult:
        return run_checkpoint(self._context, event, id)
