from __future__ import annotations

import uuid
from typing import Any
from uuid import UUID

import orjson
import pytest

from great_expectations_cloud.agent.event_handler import EventHandler

pytestmark = pytest.mark.unit


def _event_payload_with_workspace_id() -> dict[str, Any]:
    return {
        "type": "onboarding_data_assistant_request.received",
        "datasource_name": "ds",
        "data_asset_name": "asset",
        "organization_id": str(uuid.uuid4()),
        "workspace_id": str(uuid.uuid4()),
    }


def test_parse_event_includes_workspace_id():
    # Arrange
    payload = _event_payload_with_workspace_id()
    serialized = orjson.dumps(payload)

    # Act
    event = EventHandler.parse_event_from(serialized)

    # Assert
    # Event should be parsed into a concrete event with workspace_id
    assert getattr(event, "workspace_id", None) == UUID(payload["workspace_id"])  # mypy: union safe


def test_parse_event_missing_workspace_id_yields_unknown():
    # Arrange
    payload = _event_payload_with_workspace_id()
    payload.pop("workspace_id")
    serialized = orjson.dumps(payload)

    # Act
    event = EventHandler.parse_event_from(serialized)

    # Assert
    # Without workspace_id the event should be treated as unknown/invalid
    assert event.type == "unknown_event"
