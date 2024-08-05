from __future__ import annotations

import uuid

import pytest

from great_expectations_cloud.agent.actions.unknown import UnknownEventAction
from great_expectations_cloud.agent.agent_warnings import GXAgentUserWarning
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.models import UnknownEvent


def test_unknown_throws_warning(mocker):
    event = UnknownEvent()
    action = UnknownEventAction(
        context=mocker.Mock(), base_url="", auth_key="", organization_id=uuid.uuid4()
    )
    with pytest.warns(GXAgentUserWarning):
        action.run(event=event, id="lala")


def test_organization_id_of_event_needs_to_match_context(
    mocker, org_id: str, org_id_different_from_context: str
):
    event = UnknownEvent(organization_id=uuid.UUID(org_id_different_from_context))
    action = UnknownEventAction(
        context=mocker.Mock(), base_url="", auth_key="", organization_id=uuid.UUID(org_id)
    )
    with pytest.raises(GXAgentError):
        action.run(event=event, id="lala")
