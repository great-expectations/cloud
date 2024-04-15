from __future__ import annotations

from uuid import UUID

import pytest

from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction
from great_expectations_cloud.agent.models import (
    CreatedResource,
)

pytestmark = pytest.mark.unit


def test_run_checkpoint_action_without_splitter_options_returns_action_result(
    empty_cloud_data_context, checkpoint_event_without_splitter_options, checkpoint_id
):
    cloud_data_context = empty_cloud_data_context
    action = RunCheckpointAction(context=cloud_data_context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint = empty_cloud_data_context.run_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    checkpoint.run_results = {
        "GXCloudIdentifier::validation_result::78ebf58e-bdb5-4d79-88d5-79bae19bf7d0": {
            "actions_results": {
                "store_validation_result": {"id": "78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"}
            }
        }
    }
    action_result = action.run(event=checkpoint_event_without_splitter_options, id=id)

    cloud_data_context.run_checkpoint.assert_called_with(
        ge_cloud_id=UUID(checkpoint_id), batch_request=None
    )
    assert action_result.type == checkpoint_event_without_splitter_options.type
    assert action_result.id == id
    assert action_result.created_resources == [
        CreatedResource(
            resource_id="78ebf58e-bdb5-4d79-88d5-79bae19bf7d0",
            type="SuiteValidationResult",
        ),
    ]
