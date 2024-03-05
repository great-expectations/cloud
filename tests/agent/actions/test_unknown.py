from __future__ import annotations

from unittest.mock import Mock

import pytest

from great_expectations_cloud.agent.actions.unknown import UnknownEventAction
from great_expectations_cloud.agent.models import UnknownEvent


def test_unknown_throws_warning():
    event = UnknownEvent()
    action = UnknownEventAction(context=Mock())
    with pytest.warns(Warning):
        action.run(event=event, id="lala")
