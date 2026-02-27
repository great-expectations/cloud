from __future__ import annotations

import uuid

import pytest

from great_expectations_cloud.agent.actions.unknown import UnknownEventAction
from great_expectations_cloud.agent.agent_warnings import GXAgentUserWarning
from great_expectations_cloud.agent.analytics import AgentAnalytics
from great_expectations_cloud.agent.models import DomainContext, UnknownEvent


def test_unknown_throws_warning(mocker):
    event = UnknownEvent()
    action = UnknownEventAction(
        context=mocker.Mock(),
        base_url="",
        auth_key="",
        domain_context=DomainContext(organization_id=uuid.uuid4(), workspace_id=uuid.uuid4()),
        analytics=AgentAnalytics(),
    )
    with pytest.warns(GXAgentUserWarning):
        action.run(event=event, id="lala")
