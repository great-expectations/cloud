from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from unittest.mock import create_autospec
from uuid import UUID

import pytest
import responses
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier
from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent.interfaces import TestConnectionError

from great_expectations_cloud.agent.actions import RunWindowCheckpointAction
from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction
from great_expectations_cloud.agent.actions.run_scheduled_checkpoint import (
    RunScheduledCheckpointAction,
)
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
    RunScheduledCheckpointEvent,
    RunWindowCheckpointEvent,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


@pytest.fixture
def cloud_base_url() -> str:
    return "http://localhost:5000"


run_checkpoint_action_class_and_event = (
    RunCheckpointAction,
    RunCheckpointEvent(
        type="run_checkpoint_request",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        checkpoint_name="Checkpoint Z",
        organization_id=uuid.uuid4(),
    ),
)
run_scheduled_checkpoint_action_class_and_event = (
    RunScheduledCheckpointAction,
    RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        checkpoint_name="Checkpoint Z",
        schedule_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        organization_id=uuid.uuid4(),
    ),
)
run_window_checkpoint_action_class_and_event = (
    RunWindowCheckpointAction,
    RunWindowCheckpointEvent(
        type="run_window_checkpoint.received",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        organization_id=uuid.uuid4(),
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
    [
        run_checkpoint_action_class_and_event,
        run_scheduled_checkpoint_action_class_and_event,
        # run_window_checkpoint_action_class_and_event,
    ],
)
def test_run_checkpoint_action_with_and_without_splitter_options_returns_action_result(
    mock_context, action_class, event, splitter_options, batch_request, cloud_base_url
):
    # ARRANGE
    org_id = "12345678-1234-5678-1234-567812345678"
    action = action_class(
        context=mock_context, base_url=cloud_base_url, organization_id=org_id, auth_key=""
    )
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint = mock_context.checkpoints.get.return_value
    identifier = create_autospec(ValidationResultIdentifier)
    result = ExpectationSuiteValidationResult(
        success=True, results=[], suite_name="abc", id="78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"
    )
    checkpoint.run.return_value.run_results = {identifier: result}

    event.splitter_options = splitter_options
    if event.type == "run_window_checkpoint.received":
        # Test errs with and without this mock for the window checkpoint
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                f"{cloud_base_url}/api/v1/organizations/{org_id}/checkpoints/{event.checkpoint_id}/expectation-parameters",
                json={"data": {"expectation_parameters": {"batch_request": batch_request}}},
            )

    # ACT
    action_result = action.run(event=event, id=id)

    # ASSERT
    checkpoint.run.assert_called_with(
        batch_parameters=splitter_options, expectation_parameters=None
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
    [
        run_checkpoint_action_class_and_event,
        run_scheduled_checkpoint_action_class_and_event,
        # run_window_checkpoint_action_class_and_event,
    ],
)
def test_run_checkpoint_action_raises_on_test_connection_failure(
    mock_context,
    mocker: MockerFixture,
    action_class,
    event,
    cloud_base_url,
):
    mock_datasource = mocker.Mock(spec=Datasource)
    mock_context.data_sources.get.return_value = mock_datasource
    mock_datasource.test_connection.side_effect = TestConnectionError()
    org_id = uuid.uuid4()
    action = action_class(
        context=mock_context, base_url=cloud_base_url, organization_id=org_id, auth_key=""
    )
    if event.type == "run_window_checkpoint.received":
        # Test errs with and without this mock for the window checkpoint
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                f"{cloud_base_url}/api/v1/organizations/{org_id}/checkpoints/{event.checkpoint_id}/expectation-parameters",
                json={"data": {"expectation_parameters": {}}},
            )

    with pytest.raises(TestConnectionError):
        action.run(
            event=event,
            id="test-id",
        )
