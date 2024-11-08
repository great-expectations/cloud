from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from unittest.mock import ANY, create_autospec
from uuid import UUID

import pytest
import responses
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier
from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.exceptions import GXCloudError

from great_expectations_cloud.agent.actions import RunWindowCheckpointAction
from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction
from great_expectations_cloud.agent.actions.run_scheduled_checkpoint import (
    RunScheduledCheckpointAction,
)
from great_expectations_cloud.agent.config import GxAgentEnvVars
from great_expectations_cloud.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
    RunScheduledCheckpointEvent,
    RunWindowCheckpointEvent,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


fixture_id = "81f4e105-e37d-4168-85a0-2526943f9956"


@pytest.fixture
def org_id():
    return fixture_id


@pytest.fixture
def token():
    return "MTg0NDkyYmYtNTBiOS00ZDc1LTk3MmMtYjQ0M2NhZDA2NjJk"


@pytest.fixture
def set_required_env_vars(monkeypatch, org_id, token) -> None:
    env_vars = {
        "GX_CLOUD_BASE_URL": "http://test-base-url",
        "GX_CLOUD_ORGANIZATION_ID": org_id,
        "GX_CLOUD_ACCESS_TOKEN": token,
    }
    for key, val in env_vars.items():
        monkeypatch.setenv(name=key, value=val)


run_checkpoint_action_class_and_event = (
    RunCheckpointAction,
    RunCheckpointEvent(
        type="run_checkpoint_request",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        checkpoint_name="Checkpoint Z",
        organization_id=UUID(fixture_id),
    ),
    None,
)
run_scheduled_checkpoint_action_class_and_event = (
    RunScheduledCheckpointAction,
    RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        checkpoint_name="Checkpoint Z",
        schedule_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        organization_id=UUID(fixture_id),
    ),
    "great_expectations_cloud.agent.actions.run_scheduled_checkpoint.run_checkpoint",
)
run_window_checkpoint_action_class_and_event = (
    RunWindowCheckpointAction,
    RunWindowCheckpointEvent(
        type="run_window_checkpoint.received",
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        checkpoint_id=UUID("5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"),
        organization_id=UUID(fixture_id),
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
    "action_class,event,mock_patch",
    [
        run_checkpoint_action_class_and_event,
        run_scheduled_checkpoint_action_class_and_event,
        # run_window_checkpoint_action_class_and_event,
    ],
)
@responses.activate
def test_run_checkpoint_action_with_and_without_splitter_options_returns_action_result(
    mock_context,
    mocker: MockerFixture,
    action_class,
    event,
    mock_patch,
    splitter_options,
    batch_request,
    org_id,
    set_required_env_vars,
):
    # ARRANGE
    env_vars = GxAgentEnvVars()
    action = action_class(
        context=mock_context,
        base_url=env_vars.gx_cloud_base_url,
        organization_id=UUID(org_id),
        auth_key="",
    )
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    if mock_patch is not None:
        mock_checkpoint = mocker.patch(mock_patch)
    else:
        mock_checkpoint = mock_context.checkpoints.get.return_value
        identifier = create_autospec(ValidationResultIdentifier)
        result = ExpectationSuiteValidationResult(
            success=True, results=[], suite_name="abc", id="78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"
        )
        mock_checkpoint.run.return_value.run_results = {identifier: result}

    event.splitter_options = splitter_options

    if event.type == "run_scheduled_checkpoint.received":
        url = f"{env_vars.gx_cloud_base_url}/api/v1/organizations/{env_vars.gx_cloud_organization_id}/checkpoints/{event.checkpoint_id}/expectation-parameters"
        responses.get(
            url,
            json={"data": {"expectation_parameters": {"something_min": 0, "something_max": 1}}},
        )

    # ACT
    action_result = action.run(event=event, id=id)

    # ASSERT
    if mock_patch is not None:
        mock_checkpoint.assert_called_with(
            ANY, event, ANY, expectation_parameters={"something_min": 0, "something_max": 1}
        )
    else:
        mock_checkpoint.run.assert_called_with(
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
    "action_class,event,mock_patch",
    [
        run_checkpoint_action_class_and_event,
        run_scheduled_checkpoint_action_class_and_event,
        # run_window_checkpoint_action_class_and_event,
    ],
)
@responses.activate
def test_run_checkpoint_action_raises_on_test_connection_failure(
    mock_context, mocker: MockerFixture, action_class, event, mock_patch, set_required_env_vars
):
    env_vars = GxAgentEnvVars()
    mock_datasource = mocker.Mock(spec=Datasource)
    mock_context.data_sources.get.return_value = mock_datasource
    mock_datasource.test_connection.side_effect = TestConnectionError()
    org_id = uuid.uuid4()
    action = action_class(
        context=mock_context,
        base_url=env_vars.gx_cloud_base_url,
        organization_id=org_id,
        auth_key="",
    )
    # Test errs with and without this mock for the window checkpoint
    if event.type == "run_scheduled_checkpoint.received":
        responses.get(
            url=f"{env_vars.gx_cloud_base_url}/api/v1/organizations/{org_id}/checkpoints/{event.checkpoint_id}/expectation-parameters",
            json={"data": {"expectation_parameters": {}}},
        )

    with pytest.raises(TestConnectionError):
        action.run(
            event=event,
            id="test-id",
        )


@pytest.mark.parametrize(
    "action_class,event,mock_patch",
    [
        run_scheduled_checkpoint_action_class_and_event,
    ],
)
@responses.activate
def test_run_checkpoint_action_raises_on_gx_cloud_error(
    mock_context, mocker: MockerFixture, action_class, event, mock_patch, set_required_env_vars
):
    env_vars = GxAgentEnvVars()
    mock_datasource = mocker.Mock(spec=Datasource)
    mock_context.data_sources.get.return_value = mock_datasource
    mock_datasource.test_connection.side_effect = TestConnectionError()
    org_id = uuid.uuid4()
    action = action_class(
        context=mock_context,
        base_url=env_vars.gx_cloud_base_url,
        organization_id=org_id,
        auth_key="",
    )
    # Test errs with and without this mock for the window checkpoint
    if event.type == "run_scheduled_checkpoint.received":
        responses.add(
            method=responses.GET,
            url=f"{env_vars.gx_cloud_base_url}/api/v1/organizations/{org_id}/checkpoints/{event.checkpoint_id}/expectation-parameters",
            status=503,
        )

    with pytest.raises(GXCloudError):
        action.run(
            event=event,
            id="test-id",
        )


@pytest.mark.parametrize(
    "action_class,event,mock_patch",
    [
        run_scheduled_checkpoint_action_class_and_event,
    ],
)
@responses.activate
def test_run_checkpoint_action_raises_on_key_error(
    mock_context, mocker: MockerFixture, action_class, event, mock_patch, set_required_env_vars
):
    env_vars = GxAgentEnvVars()
    mock_datasource = mocker.Mock(spec=Datasource)
    mock_context.data_sources.get.return_value = mock_datasource
    mock_datasource.test_connection.side_effect = TestConnectionError()
    org_id = uuid.uuid4()
    action = action_class(
        context=mock_context,
        base_url=env_vars.gx_cloud_base_url,
        organization_id=org_id,
        auth_key="",
    )
    # Test errs with and without this mock for the window checkpoint
    if event.type == "run_scheduled_checkpoint.received":
        responses.add(
            method=responses.GET,
            url=f"{env_vars.gx_cloud_base_url}/api/v1/organizations/{org_id}/checkpoints/{event.checkpoint_id}/expectation-parameters",
            status=200,
            json={"data": {}},
        )

    with pytest.raises(GXCloudError):
        action.run(
            event=event,
            id="test-id",
        )
