from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pytest
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent.interfaces import TestConnectionError

from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction
from great_expectations_cloud.agent.actions.run_scheduled_checkpoint import (
    RunScheduledCheckpointAction,
)
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
    RunScheduledCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint.configurator import ActionDict
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit

CHECKPOINT_ID = UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8")

default_checkpoint_action: ActionDict = {
    "name": "store_validation_result",
    "action": {
        "class_name": "StoreValidationResultAction",
    },
}

another_checkpoint_action: ActionDict = {
    "name": "another_action",
    "action": {"class_name": "MyCustomAction"},
}

run_checkpoint_action_class_and_event = (
    RunCheckpointAction,
    RunCheckpointEvent(
        type="run_checkpoint_request",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id=CHECKPOINT_ID,
        organization_id=uuid.uuid4(),
    ),
)
run_scheduled_checkpoint_action_class_and_event = (
    RunScheduledCheckpointAction,
    RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        schedule_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        organization_id=uuid.uuid4(),
    ),
)


@pytest.mark.parametrize(
    ("checkpoint_actions", "expected_checkpoint_actions"),
    [
        ([default_checkpoint_action], None),
        ([default_checkpoint_action, another_checkpoint_action], None),
        ([], [default_checkpoint_action]),
        ([another_checkpoint_action], [default_checkpoint_action]),
    ],
)
@pytest.mark.parametrize(
    "splitter_options, batch_request",
    [
        (
            {"year": 2023, "month": 11, "day": 30},
            {"options": {"day": 30, "month": 11, "year": 2023}},
        ),
        ({}, None),
        (None, None),
    ],
)
@pytest.mark.parametrize(
    "action_class,event",
    [run_checkpoint_action_class_and_event, run_scheduled_checkpoint_action_class_and_event],
)
def test_run_checkpoint_action_with_and_without_splitter_options_returns_action_result(
    mock_context,
    action_class,
    event,
    splitter_options,
    batch_request,
    checkpoint_actions: list[dict[str, Any]],
    expected_checkpoint_actions: list[ActionDict] | None,
    mocker: MockerFixture,
):
    mock_checkpoint = mocker.MagicMock(
        spec=Checkpoint,
        id=str(CHECKPOINT_ID),
        action_list=checkpoint_actions,
    )
    mock_context.get_checkpoint.return_value = mock_checkpoint

    action = action_class(
        context=mock_context, base_url="", organization_id=uuid.uuid4(), auth_key=""
    )
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint_result = mock_checkpoint.run.return_value
    checkpoint_result_id_str = "5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"
    checkpoint_result.ge_cloud_id = checkpoint_result_id_str
    checkpoint_result.run_results = {
        "GXCloudIdentifier::validation_result::78ebf58e-bdb5-4d79-88d5-79bae19bf7d0": {
            "actions_results": {
                "store_validation_result": {"id": "78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"}
            }
        }
    }

    event.splitter_options = splitter_options
    action_result = action.run(event=event, id=id)

    mock_context.get_checkpoint.assert_called_once_with(ge_cloud_id=str(CHECKPOINT_ID))
    mock_checkpoint.run.assert_called_once_with(
        batch_request=batch_request,
        action_list=expected_checkpoint_actions,
    )

    assert action_result.type == event.type
    assert action_result.id == id
    assert action_result.created_resources == [
        CreatedResource(
            resource_id="78ebf58e-bdb5-4d79-88d5-79bae19bf7d0",
            type="SuiteValidationResult",
        ),
    ]


@pytest.mark.parametrize(
    "action_class,event",
    [run_checkpoint_action_class_and_event, run_scheduled_checkpoint_action_class_and_event],
)
def test_run_checkpoint_action_raises_on_test_connection_failure(
    mock_context,
    mocker: MockerFixture,
    action_class,
    event,
):
    mock_datasource = mocker.Mock(spec=Datasource)
    mock_context.get_datasource.return_value = mock_datasource
    mock_datasource.test_connection.side_effect = TestConnectionError()

    action = action_class(
        context=mock_context, base_url="", organization_id=uuid.uuid4(), auth_key=""
    )

    with pytest.raises(TestConnectionError):
        action.run(
            event=event,
            id="test-id",
        )
