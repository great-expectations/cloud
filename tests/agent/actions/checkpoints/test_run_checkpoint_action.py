from unittest.mock import MagicMock
from uuid import UUID

import pytest
from great_expectations.data_context import CloudDataContext

from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
)

pytestmark = pytest.mark.unit


@pytest.fixture(scope="function")
def context():
    return MagicMock(autospec=CloudDataContext)


@pytest.fixture
def checkpoint_id():
    return "5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"


@pytest.fixture
def checkpoint_event_without_splitter_options(checkpoint_id):
    return RunCheckpointEvent(
        type="run_checkpoint_request",
        checkpoint_id=checkpoint_id,
    )


@pytest.fixture
def checkpoint_event_with_splitter_options(checkpoint_id):
    return RunCheckpointEvent(
        type="run_checkpoint_request",
        checkpoint_id=checkpoint_id,
        splitter_options={"year": 2023, "month": 11, "day": 30},
    )


def test_run_checkpoint_action_without_splitter_options_returns_action_result(
    context, checkpoint_event_without_splitter_options, checkpoint_id
):
    action = RunCheckpointAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint = context.run_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    checkpoint.run_results = {
        "GXCloudIdentifier::validation_result::78ebf58e-bdb5-4d79-88d5-79bae19bf7d0": {
            "actions_results": {
                "store_validation_result": {"id": "78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"}
            }
        }
    }
    action_result = action.run(event=checkpoint_event_without_splitter_options, id=id)

    context.run_checkpoint.assert_called_with(ge_cloud_id=UUID(checkpoint_id), batch_request=None)
    assert action_result.type == checkpoint_event_without_splitter_options.type
    assert action_result.id == id
    assert action_result.created_resources == [
        CreatedResource(
            resource_id="78ebf58e-bdb5-4d79-88d5-79bae19bf7d0",
            type="SuiteValidationResult",
        ),
    ]


def test_run_checkpoint_action_with_splitter_options_returns_action_result(
    context, checkpoint_event_with_splitter_options, checkpoint_id
):
    action = RunCheckpointAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint = context.run_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    checkpoint.run_results = {
        "GXCloudIdentifier::validation_result::78ebf58e-bdb5-4d79-88d5-79bae19bf7d0": {
            "actions_results": {
                "store_validation_result": {"id": "78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"}
            }
        }
    }
    action_result = action.run(event=checkpoint_event_with_splitter_options, id=id)

    context.run_checkpoint.assert_called_with(
        ge_cloud_id=UUID(checkpoint_id),
        batch_request={"options": {"day": 30, "month": 11, "year": 2023}},
    )
    assert action_result.type == checkpoint_event_with_splitter_options.type
    assert action_result.id == id
    assert action_result.created_resources == [
        CreatedResource(
            resource_id="78ebf58e-bdb5-4d79-88d5-79bae19bf7d0",
            type="SuiteValidationResult",
        ),
    ]
