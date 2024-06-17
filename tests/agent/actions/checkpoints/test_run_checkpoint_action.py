from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import pytest
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
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit

run_checkpoint_action_class_and_event = (
    RunCheckpointAction,
    RunCheckpointEvent(
        type="run_checkpoint_request",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id="5f3814d6-a2e2-40f9-ba75-87ddf485c3a8",
    ),
)
run_scheduled_checkpoint_action_class_and_event = (
    RunScheduledCheckpointAction,
    RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id="5f3814d6-a2e2-40f9-ba75-87ddf485c3a8",
        schedule_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
    ),
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
    mock_context, action_class, event, splitter_options, batch_request
):
    action = action_class(context=mock_context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint = mock_context.run_checkpoint.return_value
    checkpoint_id_str = "5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"
    checkpoint.ge_cloud_id = checkpoint_id_str
    checkpoint.run_results = {
        "GXCloudIdentifier::validation_result::78ebf58e-bdb5-4d79-88d5-79bae19bf7d0": {
            "actions_results": {
                "store_validation_result": {"id": "78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"}
            }
        }
    }
    event.splitter_options = splitter_options
    action_result = action.run(event=event, id=id)

    mock_context.run_checkpoint.assert_called_with(
        ge_cloud_id=UUID(checkpoint_id_str), batch_request=batch_request
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

    action = action_class(context=mock_context)

    with pytest.raises(TestConnectionError):
        action.run(
            event=event,
            id="test-id",
        )
