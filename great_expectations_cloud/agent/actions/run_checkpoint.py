from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Any
import logging

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
    from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

# from great_expectations_cloud.agent.actions.utils import verify_data_asset


class RunCheckpointAction(AgentAction[RunCheckpointEvent]):
    @override
    def run(self, event: RunCheckpointEvent, id: str) -> ActionResult:
        return run_checkpoint(self._context, event, id)


class MissingCheckpointNameError(ValueError):
    """Property checkpoint_name is required but not present."""


# using a dataclass because we don't want the pydantic behavior of copying objects
@dataclass
class DataSourceAssets:
    data_source: Datasource[Any, Any]
    assets_by_name: dict[str, DataAsset[Any, Any]]


def run_checkpoint(
    context: CloudDataContext,
    event: RunCheckpointEvent | RunScheduledCheckpointEvent | RunWindowCheckpointEvent,
    id: str,
    expectation_parameters: dict[str, Any] | None = None,
) -> ActionResult:
    # the checkpoint_name property on possible events is optional for backwards compatibility,
    # but this action requires it in order to run:
    if not event.checkpoint_name:
        raise MissingCheckpointNameError

    # # Check if we have the old event structure (your code) or should use the new approach
    # if hasattr(event, 'datasource_names_to_asset_names') and event.datasource_names_to_asset_names:
    #     # Use your original approach for backward compatibility
    #     for datasource_name, data_asset_names in event.datasource_names_to_asset_names.items():
    #         datasource = context.data_sources.get(name=datasource_name)
    #         datasource.test_connection(test_assets=False)  # raises `TestConnectionError` on failure
    #         for data_asset_name in data_asset_names:
    #             asset = datasource.get_asset(data_asset_name)
    #             verify_data_asset(LOGGER.info, datasource, asset)
    #             asset.test_connection()  # raises `TestConnectionError` on failure
    # else:
    # Use the new approach from Great Expectations
    checkpoint = context.checkpoints.get(name=event.checkpoint_name)
    
    # only GX-managed Checkpoints are currently validated here and they contain only one validation definition, but
    # the Checkpoint does allow for multiple validation definitions so we'll be defensive and ensure we only test each
    # source/asset once
    data_sources_assets_by_data_source_name: dict[str, DataSourceAssets] = {}
    for vd in checkpoint.validation_definitions:
        ds = vd.data_source
        ds_name = ds.name
        # create assets by name dict
        if ds_name not in data_sources_assets_by_data_source_name:
            data_sources_assets_by_data_source_name[ds_name] = DataSourceAssets(
                data_source=ds, assets_by_name={}
            )
        data_sources_assets_by_data_source_name[ds_name].assets_by_name[vd.asset.name] = vd.asset

    for data_sources_assets in data_sources_assets_by_data_source_name.values():
        data_source = data_sources_assets.data_source
        data_source.test_connection(test_assets=False)  # raises `TestConnectionError` on failure
        for data_asset in data_sources_assets.assets_by_name.values():
            # Add your verification if you want
            # verify_data_asset(LOGGER.info, data_source, data_asset)
            data_asset.test_connection()  # raises `TestConnectionError` on failure

    # Get checkpoint if we haven't already
    # if 'checkpoint' not in locals():
    #     checkpoint = context.checkpoints.get(name=event.checkpoint_name)

    # run checkpoint
    checkpoint_run_result = checkpoint.run(
        batch_parameters=event.splitter_options, expectation_parameters=expectation_parameters
    )

    validation_results = checkpoint_run_result.run_results
    created_resources = []
    for key in validation_results.keys():
        suite_validation_result = validation_results[key]
        if suite_validation_result.id is None:
            raise RuntimeError(f"SuiteValidationResult.id is None for key: {key}")  # noqa: TRY003
        created_resource = CreatedResource(
            resource_id=suite_validation_result.id,
            type="SuiteValidationResult",
        )
        created_resources.append(created_resource)

    return ActionResult(
        id=id,
        type=event.type,
        created_resources=created_resources,
    )


register_event_action("1", RunCheckpointEvent, RunCheckpointAction)