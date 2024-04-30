from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from great_expectations_cloud.agent.actions import DraftDatasourceConfigAction
from great_expectations_cloud.agent.exceptions import GXCoreError
from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


def test_running_draft_datasource_config_action(
    context: CloudDataContext,
):
    # Arrange
    # Draft config is loaded in mercury seed data

    # Act
    action = DraftDatasourceConfigAction(context=context)

    draft_datasource_id_for_connect_successfully = (
        "2512c2d8-a212-4295-b01b-2bb2ac066f04"  # local_mercury_db
    )
    draft_datasource_config_event = DraftDatasourceConfigEvent(
        type="test_datasource_config",
        config_id=draft_datasource_id_for_connect_successfully,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    result = action.run(event=draft_datasource_config_event, id=event_id)

    # Assert
    # Check that the action was successful e.g. that we can connect to the datasource
    assert result
    assert result.id == event_id
    assert result.type == draft_datasource_config_event.type
    assert result.created_resources == []


def test_running_draft_datasource_config_action_fails_for_unreachable_datasource(
    context: CloudDataContext,
):
    # Arrange
    # Draft config is loaded in mercury seed data

    # Act
    action = DraftDatasourceConfigAction(context=context)
    datasource_id_for_connect_successfully = (
        "e47a5059-a6bb-4de7-9286-6ea600a0c53a"  # local_mercury_db_bad_password
    )
    draft_datasource_config_event = DraftDatasourceConfigEvent(
        type="test_datasource_config",
        config_id=datasource_id_for_connect_successfully,
    )

    # Check that the action was unsuccessful and an error was raised.
    with pytest.raises(GXCoreError):
        action.run(event=draft_datasource_config_event, id="test_id")
