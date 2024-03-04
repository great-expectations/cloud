from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from great_expectations_cloud.agent.event_handler import EventHandler, UnknownEventError
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    RunCheckpointEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
    UnknownEvent,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


def test_event_handler_raises_for_unknown_event(mock_context):
    event = UnknownEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    handler = EventHandler(context=mock_context)

    with pytest.raises(UnknownEventError):
        handler.handle_event(event=event, id=correlation_id)


def test_event_handler_handles_run_missingness_data_assistant_event(
    mock_context, mocker: MockerFixture
):
    action = mocker.patch(
        "great_expectations_cloud.agent.event_handler.RunMissingnessDataAssistantAction"
    )
    event = RunMissingnessDataAssistantEvent(
        datasource_name="test-datasource",
        data_asset_name="test-data-asset",
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    handler = EventHandler(context=mock_context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=mock_context)
    action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_run_onboarding_data_assistant_event(
    mock_context, mocker: MockerFixture
):
    action = mocker.patch(
        "great_expectations_cloud.agent.event_handler.RunOnboardingDataAssistantAction"
    )
    event = RunOnboardingDataAssistantEvent(
        datasource_name="test-datasource", data_asset_name="test-data-asset"
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    handler = EventHandler(context=mock_context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=mock_context)
    action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_run_checkpoint_event(mock_context, mocker: MockerFixture):
    action = mocker.patch("great_expectations_cloud.agent.event_handler.RunCheckpointAction")
    event = RunCheckpointEvent(
        checkpoint_id="3ecd140b-1dd5-41f4-bdb1-c8009d4f1940",
        datasource_names_to_asset_names={"Data Source name": {"Asset name"}},
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    handler = EventHandler(context=mock_context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=mock_context)
    action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_draft_config_event(mock_context, mocker: MockerFixture):
    action = mocker.patch(
        "great_expectations_cloud.agent.event_handler.DraftDatasourceConfigAction"
    )
    event = DraftDatasourceConfigEvent(config_id=uuid4())
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    handler = EventHandler(context=mock_context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=mock_context)
    action.return_value.run.assert_called_with(event=event, id=correlation_id)
