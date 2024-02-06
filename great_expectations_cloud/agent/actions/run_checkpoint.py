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
        checkpoint_run_result = self._context.run_checkpoint(
            ge_cloud_id=event.checkpoint_id,
            batch_request={"options": event.splitter_options} if event.splitter_options else None,
        )
        if event.datasource_name:
            datasource = self._context.get_datasource(event.datasource_name)
            datasource.test_connection(test_assets=True)  # raises `TestConnectionError` on failure

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
