from __future__ import annotations

import pytest

from great_expectations_cloud.agent.actions import DraftDatasourceConfigAction
from great_expectations_cloud.agent.exceptions import GXCoreError
from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent


def test_running_draft_datasource_config_action(
    context,
    cloud_base_url: str,
    cloud_organization_id: str,
    cloud_access_token: str,
):
    action = DraftDatasourceConfigAction(context=context)
    # local mercury db:
    datasource_id_for_connect_successfully = "2ccfea7f-3f91-47f2-804e-2106aa07ef24"
    draft_datasource_config_event = DraftDatasourceConfigEvent(
        type="test_datasource_config",
        config_id=datasource_id_for_connect_successfully,
    )

    result = action.run(event=draft_datasource_config_event, id="test_id")

    # Check that the action was successful e.g. that we can connect to the datasource
    assert result


def test_running_draft_datasource_config_action_fails_for_unreachable_datasource(
    context,
    cloud_base_url: str,
    cloud_organization_id: str,
    cloud_access_token: str,
):
    action = DraftDatasourceConfigAction(context=context)
    # my_pandas_filesystem_ds:
    datasource_id_for_connect_successfully = "d0c05404-a69e-4beb-af57-e98c24480143"
    draft_datasource_config_event = DraftDatasourceConfigEvent(
        type="test_datasource_config",
        config_id=datasource_id_for_connect_successfully,
    )

    # Check that the action was unsuccessful and an error was raised.
    with pytest.raises(GXCoreError):
        action.run(event=draft_datasource_config_event, id="test_id")
