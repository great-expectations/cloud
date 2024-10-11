from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Literal
from unittest.mock import ANY
from uuid import UUID

import pytest
import responses

from great_expectations_cloud.agent.actions.run_scheduled_window_checkpoint import (
    RunScheduledWindowCheckpointAction,
)
from great_expectations_cloud.agent.config import GxAgentEnvVars
from great_expectations_cloud.agent.models import (
    RunScheduledWindowCheckpointEvent,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.fixture
def org_id():
    return "81f4e105-e37d-4168-85a0-2526943f9956"


@pytest.fixture
def token():
    return "MTg0NDkyYmYtNTBiOS00ZDc1LTk3MmMtYjQ0M2NhZDA2NjJk"


@pytest.fixture
def set_required_env_vars(monkeypatch, org_id, token) -> None:
    env_vars = {
        "GX_CLOUD_BASE_URL": "https://test-base-url",
        "GX_CLOUD_ORGANIZATION_ID": org_id,
        "GX_CLOUD_ACCESS_TOKEN": token,
    }
    for key, val in env_vars.items():
        monkeypatch.setenv(name=key, value=val)


def build_get_draft_config_payload(
    config: dict[str, Any], id: UUID
) -> dict[Literal["data"], dict[str, str | UUID | dict[str, Any]]]:
    return {
        "data": {
            "type": "draft_config",
            "id": str(id),
            "config": config,
        }
    }


@responses.activate
def test_run_scheduled_window_checkpoint(
    mock_context, mocker: MockerFixture, set_required_env_vars: None, org_id
):
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    env_vars = GxAgentEnvVars()
    action = RunScheduledWindowCheckpointAction(
        context=mock_context,
        base_url="https://test-base-url",
        auth_key="",
        organization_id=org_id,
    )

    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    checkpoint_id = uuid.uuid4()
    event = RunScheduledWindowCheckpointEvent(
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        organization_id=UUID(env_vars.gx_cloud_organization_id),
        checkpoint_id=checkpoint_id,
        schedule_id=uuid.uuid4(),
    )
    expected_url: str = (
        f"{env_vars.gx_cloud_base_url}/api/v1/organizations/{env_vars.gx_cloud_organization_id}"
        f"/checkpoints/{checkpoint_id}/expectation-parameters"
    )
    # 'https://test-base-url/api/v1/organizations/81f4e105-e37d-4168-85a0-2526943f9956/checkpoints/120ba4c5-2004-4222-9d34-c59e6059a6f7/expectation-parameters'
    expectation_parameters = {"param_name_min": 4.0, "param_name_max": 6.0}
    responses.get(
        url=expected_url,
        json={"data": {"expectation_parameters": expectation_parameters}},
    )
    mock_run_checkpoint = mocker.patch(
        "great_expectations_cloud.agent.actions.run_window_checkpoint.run_checkpoint",
    )
    _ = action.run(event=event, id=str(job_id))

    mock_run_checkpoint.assert_called_with(
        ANY, event, ANY, expectation_parameters=expectation_parameters
    )
