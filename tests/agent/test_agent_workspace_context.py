from __future__ import annotations

import uuid

import pytest

from great_expectations_cloud.agent import GXAgent
from great_expectations_cloud.agent.message_service.subscriber import EventContext
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    RunCheckpointEvent,
)

pytestmark = pytest.mark.unit


def _make_event_context(event, correlation_id: str) -> EventContext:
    async def _noop():
        return None

    return EventContext(
        event=event,
        correlation_id=correlation_id,
        processed_successfully=lambda: None,
        processed_with_failures=lambda: None,
        redeliver_message=_noop,
    )


def test_agent_does_not_initialize_context_on_init(mocker, monkeypatch):
    # Provide required env vars for agent config
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", str(uuid.uuid4()))
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "dummy")
    monkeypatch.setenv("GX_CLOUD_BASE_URL", "http://localhost:5000/")
    # Arrange
    get_context_spy = mocker.patch("great_expectations_cloud.agent.agent.get_context")
    # Mock HTTP session used by _create_config to avoid real POST
    mock_session = mocker.MagicMock()
    mock_response = mocker.Mock()
    mock_response.ok = True
    mock_response.json.return_value = {
        "queue": "gx-agent",
        "connection_string": "amqp://guest:guest@localhost:5672/",
    }
    mock_session.post.return_value = mock_response
    # Support context manager usage elsewhere in agent for status updates
    mock_session.__enter__.return_value = mock_session
    mock_session.__exit__.return_value = None
    mock_patch_response = mocker.Mock()
    mock_patch_response.raise_for_status = mocker.Mock()
    mock_session.patch.return_value = mock_patch_response
    mocker.patch("great_expectations_cloud.agent.agent.create_session", return_value=mock_session)

    # Act
    _ = GXAgent()

    # Assert
    get_context_spy.assert_not_called()


def test_get_data_context_builds_context_per_event(mocker, monkeypatch):
    # Provide required env vars for agent config
    monkeypatch.setenv("GX_CLOUD_ORGANIZATION_ID", str(uuid.uuid4()))
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "dummy")
    monkeypatch.setenv("GX_CLOUD_BASE_URL", "http://localhost:5000/")
    # Arrange
    get_context_spy = mocker.patch("great_expectations_cloud.agent.agent.get_context")
    # Mock HTTP session used by _create_config to avoid real POST
    mock_session = mocker.MagicMock()
    mock_response = mocker.Mock()
    mock_response.ok = True
    mock_response.json.return_value = {
        "queue": "gx-agent",
        "connection_string": "amqp://guest:guest@localhost:5672/",
    }
    mock_session.post.return_value = mock_response
    # Support context manager usage elsewhere in agent for status updates
    mock_session.__enter__.return_value = mock_session
    mock_session.__exit__.return_value = None
    mock_patch_response = mocker.Mock()
    mock_patch_response.raise_for_status = mocker.Mock()
    mock_session.patch.return_value = mock_patch_response
    mocker.patch("great_expectations_cloud.agent.agent.create_session", return_value=mock_session)

    agent = GXAgent()

    # Two events with distinct workspace_ids
    org_id = uuid.uuid4()
    ws_id_1 = uuid.uuid4()
    ws_id_2 = uuid.uuid4()
    correlation_ids = [str(uuid.uuid4()), str(uuid.uuid4())]

    # Act: call get_data_context twice, once per event
    agent.get_data_context(
        _make_event_context(
            DraftDatasourceConfigEvent(
                type="test_datasource_config",
                config_id=uuid.uuid4(),
                organization_id=org_id,
                workspace_id=ws_id_1,
            ),
            correlation_ids[0],
        )
    )
    agent.get_data_context(
        _make_event_context(
            RunCheckpointEvent(
                type="run_checkpoint_request",
                datasource_names_to_asset_names={},
                checkpoint_id=uuid.uuid4(),
                organization_id=org_id,
                workspace_id=ws_id_2,
            ),
            correlation_ids[1],
        )
    )

    # Assert - one call per event
    assert get_context_spy.call_count == 2
