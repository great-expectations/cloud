from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from unittest.mock import ANY
from uuid import UUID

import pytest

from great_expectations_cloud.agent.actions import RunWindowCheckpointAction
from great_expectations_cloud.agent.analytics import AgentAnalytics
from great_expectations_cloud.agent.config import GxAgentEnvVars
from great_expectations_cloud.agent.models import DomainContext, RunWindowCheckpointEvent

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


def test_run_window_checkpoint(
    mock_context, mocker: MockerFixture, set_required_env_vars: None, org_id
):
    org_id = UUID("81f4e105-e37d-4168-85a0-2526943f9956")
    env_vars = GxAgentEnvVars()
    workspace_id = uuid.uuid4()
    action = RunWindowCheckpointAction(
        context=mock_context,
        base_url="https://test-base-url",
        auth_key="",
        domain_context=DomainContext(organization_id=org_id, workspace_id=workspace_id),
        analytics=AgentAnalytics(),
    )

    job_id = UUID("87657a8e-f65e-4e64-b21f-e83a54738b75")
    checkpoint_id = uuid.uuid4()
    event = RunWindowCheckpointEvent(
        datasource_names_to_asset_names={"Data Source 1": {"Data Asset A", "Data Asset B"}},
        organization_id=UUID(env_vars.gx_cloud_organization_id),
        checkpoint_id=checkpoint_id,
        workspace_id=workspace_id,
    )
    mock_run_checkpoint = mocker.patch(
        "great_expectations_cloud.agent.actions.run_window_checkpoint.run_checkpoint",
    )
    _ = action.run(event=event, id=str(job_id))

    mock_run_checkpoint.assert_called_with(ANY, event, ANY)
