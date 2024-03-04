from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from great_expectations.data_context import CloudDataContext

from great_expectations_cloud.agent.actions import (
    ColumnDescriptiveMetricsAction,
    ListTableNamesAction,
    RunCheckpointAction,
    RunMissingnessDataAssistantAction,
    RunOnboardingDataAssistantAction,
)
from great_expectations_cloud.agent.actions.draft_datasource_config_action import (
    DraftDatasourceConfigAction,
)
from great_expectations_cloud.agent.actions.unknown import UnknownEventAction
from great_expectations_cloud.agent.event_handler import EventHandler
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    ListTableNamesEvent,
    RunCheckpointEvent,
    RunColumnDescriptiveMetricsEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
    UnknownEvent,
)

pytestmark = pytest.mark.unit


def test_event_handler_unknown_event():
    event = UnknownEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)
    result = handler.handle_event(event=event, id=correlation_id)
    assert result.type == "unknown_event"


@pytest.mark.parametrize(
    "event_class,action_class",
    {
        RunOnboardingDataAssistantEvent: RunOnboardingDataAssistantAction,
        RunMissingnessDataAssistantEvent: RunMissingnessDataAssistantAction,
        ListTableNamesEvent: ListTableNamesAction,
        RunCheckpointEvent: RunCheckpointAction,
        RunColumnDescriptiveMetricsEvent: ColumnDescriptiveMetricsAction,
        DraftDatasourceConfigEvent: DraftDatasourceConfigAction,
    }.items(),
)
def test_get_action(event_class, action_class):
    event = MagicMock(spec=event_class)
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)
    action_class_actual = handler.get_event_action(event)
    assert isinstance(action_class_actual, action_class)


def test_event_action_unknown():
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)
    action = handler.get_event_action(UnknownEvent())
    assert isinstance(action, UnknownEventAction)


def test_event_handler_handles_run_missingness_data_assistant_event(mocker):
    mock_action=MagicMock(autospec=RunMissingnessDataAssistantAction)
    # Override with mock
    EventHandler._EVENT_TO_ACTION_MAP[RunMissingnessDataAssistantEvent] = mock_action

    event = RunMissingnessDataAssistantEvent(
        datasource_name="test-datasource",
        data_asset_name="test-data-asset",
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    mock_action.assert_called_with(context=context)
    mock_action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_run_onboarding_data_assistant_event(mocker):
    mock_action = MagicMock(autospec=RunOnboardingDataAssistantAction)
    # Override with mock
    EventHandler._EVENT_TO_ACTION_MAP[RunOnboardingDataAssistantEvent] = mock_action
    event = RunOnboardingDataAssistantEvent(
        datasource_name="test-datasource", data_asset_name="test-data-asset"
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    mock_action.assert_called_with(context=context)
    mock_action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_run_checkpoint_event(mocker):
    mock_action = MagicMock(autospec=RunCheckpointEvent)
    # Override with mock
    EventHandler._EVENT_TO_ACTION_MAP[RunCheckpointEvent] = mock_action
    event = RunCheckpointEvent(
        checkpoint_id="3ecd140b-1dd5-41f4-bdb1-c8009d4f1940",
        datasource_names_to_asset_names={"Data Source name": {"Asset name"}},
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    mock_action.assert_called_with(context=context)
    mock_action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_draft_config_event(mocker):
    mock_action = MagicMock(autospec=DraftDatasourceConfigAction)
    # Override with mock
    EventHandler._EVENT_TO_ACTION_MAP[DraftDatasourceConfigEvent] = mock_action
    event = DraftDatasourceConfigEvent(config_id=uuid4())
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    mock_action.assert_called_with(context=context)
    mock_action.return_value.run.assert_called_with(event=event, id=correlation_id)
