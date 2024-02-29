from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from great_expectations.data_context import CloudDataContext

from great_expectations_cloud.agent.actions import RunOnboardingDataAssistantAction, RunMissingnessDataAssistantAction, \
    ListTableNamesAction, RunCheckpointAction, ColumnDescriptiveMetricsAction
from great_expectations_cloud.agent.actions.draft_datasource_config_action import DraftDatasourceConfigAction
from great_expectations_cloud.agent.actions.unknown_event import UnknownEventAction
from great_expectations_cloud.agent.event_handler import EventHandler
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    RunCheckpointEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
    UnknownEvent, RunColumnDescriptiveMetricsEvent, ListTableNamesEvent,
)

pytestmark = pytest.mark.unit

# TOD Fix for entire flow
def test_event_handler_raises_for_unknown_event():
    event = UnknownEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    # with pytest.raises(UnknownEventError):
    #     handler.handle_event(event=event, id=correlation_id)

@pytest.mark.parametrize("event_class,action_class",EventHandler._EVENT_TO_ACTION_MAP.items())
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


def test_malformed_event():
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)
    event = MagicMock()
    # with pytest.raises(TypeError):
        # on message error creates unknown event

def test_event_handler_handles_run_missingness_data_assistant_event(mocker):
    action = mocker.patch(
        "great_expectations_cloud.agent.event_handler.RunMissingnessDataAssistantAction"
    )
    event = RunMissingnessDataAssistantEvent(
        datasource_name="test-datasource",
        data_asset_name="test-data-asset",
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=context)
    action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_run_onboarding_data_assistant_event(mocker):
    action = mocker.patch(
        "great_expectations_cloud.agent.event_handler.RunOnboardingDataAssistantAction"
    )
    event = RunOnboardingDataAssistantEvent(
        datasource_name="test-datasource", data_asset_name="test-data-asset"
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=context)
    action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_run_checkpoint_event(mocker):
    action = mocker.patch("great_expectations_cloud.agent.event_handler.RunCheckpointAction")
    event = RunCheckpointEvent(
        checkpoint_id="3ecd140b-1dd5-41f4-bdb1-c8009d4f1940",
        datasource_names_to_asset_names={"Data Source name": {"Asset name"}},
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=context)
    action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_draft_config_event(mocker):
    action = mocker.patch(
        "great_expectations_cloud.agent.event_handler.DraftDatasourceConfigAction"
    )
    event = DraftDatasourceConfigEvent(config_id=uuid4())
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=context)
    action.return_value.run.assert_called_with(event=event, id=correlation_id)
