from __future__ import annotations

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
)


class RunCheckpointAction(AgentAction[RunCheckpointEvent]):
    @override
    def run(self, event: RunCheckpointEvent, id: str) -> ActionResult:
        # TODO: move connection testing into OSS; there isn't really a reason it can't be done there
        for datasource_name, data_asset_names in event.datasource_names_to_asset_names.items():
            datasource = self._context.get_datasource(datasource_name)
            datasource.test_connection(test_assets=False)  # raises `TestConnectionError` on failure
            for data_asset_name in (
                data_asset_names
            ):  # only test connection for assets that are validated in checkpoint
                asset = datasource.get_asset(data_asset_name)
                asset.test_connection()  # raises `TestConnectionError` on failure
        checkpoint_run_result = self._context.run_checkpoint(
            ge_cloud_id=event.checkpoint_id,
            batch_request={"options": event.splitter_options} if event.splitter_options else None,
        )

        validation_results = checkpoint_run_result.run_results
        created_resources = []
        for key in validation_results.keys():
            created_resource = CreatedResource(
                resource_id=validation_results[key]["actions_results"]["store_validation_result"][
                    "id"
                ],
                type="SuiteValidationResult",
            )
            created_resources.append(created_resource)

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=created_resources,
        )
