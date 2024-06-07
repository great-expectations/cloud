from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent.interfaces import TestConnectionError

from great_expectations_cloud.agent.actions.run_scheduled_checkpoint import (
    RunScheduledCheckpointAction,
)
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunScheduledCheckpointEvent,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


@pytest.fixture
def checkpoint_id():
    return "5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"


@pytest.fixture
def datasource_names_to_asset_names():
    return {"Data Source 1": {"Data Asset A", "Data Asset B"}}


@pytest.fixture
def checkpoint_event_without_splitter_options(checkpoint_id, datasource_names_to_asset_names):
    return RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        checkpoint_id=checkpoint_id,
        datasource_names_to_asset_names=datasource_names_to_asset_names,
        schedule_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
    )


@pytest.fixture
def checkpoint_event_with_splitter_options(checkpoint_id, datasource_names_to_asset_names):
    return RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        checkpoint_id=checkpoint_id,
        datasource_names_to_asset_names=datasource_names_to_asset_names,
        splitter_options={"year": 2023, "month": 11, "day": 30},
        schedule_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
    )


def test_run_checkpoint_action_without_splitter_options_returns_action_result(
    mock_context, checkpoint_event_without_splitter_options, checkpoint_id
):
    action = RunScheduledCheckpointAction(context=mock_context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint = mock_context.run_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    checkpoint.run_results = {
        "GXCloudIdentifier::validation_result::78ebf58e-bdb5-4d79-88d5-79bae19bf7d0": {
            "actions_results": {
                "store_validation_result": {"id": "78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"}
            }
        }
    }
    action_result = action.run(event=checkpoint_event_without_splitter_options, id=id)

    mock_context.run_checkpoint.assert_called_with(
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


def test_run_checkpoint_action_with_splitter_options_returns_action_result(
    mock_context, checkpoint_event_with_splitter_options, checkpoint_id
):
    action = RunScheduledCheckpointAction(context=mock_context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint = mock_context.run_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    checkpoint.run_results = {
        "GXCloudIdentifier::validation_result::78ebf58e-bdb5-4d79-88d5-79bae19bf7d0": {
            "actions_results": {
                "store_validation_result": {"id": "78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"}
            }
        }
    }
    action_result = action.run(event=checkpoint_event_with_splitter_options, id=id)

    mock_context.run_checkpoint.assert_called_with(
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


def test_run_checkpoint_action_raises_on_test_connection_failure(
    mock_context, checkpoint_id, datasource_names_to_asset_names, mocker: MockerFixture
):
    mock_datasource = mocker.Mock(spec=Datasource)
    mock_context.get_datasource.return_value = mock_datasource
    mock_datasource.test_connection.side_effect = TestConnectionError()

    action = RunScheduledCheckpointAction(context=mock_context)

    with pytest.raises(TestConnectionError):
        action.run(
            event=RunScheduledCheckpointEvent(
                type="run_scheduled_checkpoint.received",
                datasource_names_to_asset_names=datasource_names_to_asset_names,
                checkpoint_id=checkpoint_id,
                schedule_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
            ),
            id="test-id",
        )
