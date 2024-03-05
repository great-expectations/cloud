from __future__ import annotations

from unittest.mock import Mock

import pytest

from great_expectations_cloud.agent.actions.unknown import UnknownEventAction
from great_expectations_cloud.agent.models import UnknownEvent
from great_expectations_cloud.agent.warnings import GXAgentUserWarning


def test_unknown_throws_warning():
    event = UnknownEvent()
    action = UnknownEventAction(context=Mock())
    with pytest.warns(GXAgentUserWarning):
        action.run(event=event, id="lala")
