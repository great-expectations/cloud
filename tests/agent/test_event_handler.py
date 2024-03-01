from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from great_expectations.data_context import CloudDataContext

from great_expectations_cloud.agent.actions import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.event_handler import (
    _EVENT_ACTION_MAP,
    EventHandler,
    UnknownEventError,
    register_event_action,
)
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    Event,
    RunCheckpointEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
    UnknownEvent,
)

pytestmark = pytest.mark.unit


def test_event_handler_raises_for_unknown_event():
    event = UnknownEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    with pytest.raises(UnknownEventError):
        handler.handle_event(event=event, id=correlation_id)


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


class TestEventHandlerRegistry:
    def test_register_event_action(self):
        class DummyEvent:
            pass

        class DummyAction(AgentAction):
            def run(self, event: Event, id: str) -> ActionResult:
                pass

        register_event_action("0", DummyEvent, DummyAction)
        assert _EVENT_ACTION_MAP["0"][DummyEvent.__name__] == DummyAction

        del _EVENT_ACTION_MAP["0"][DummyEvent.__name__]  # Cleanup

    def test_register_event_action_already_registered(self):
        class DummyEvent:
            pass

        class DummyAction(AgentAction):
            def run(self, event: Event, id: str) -> ActionResult:
                pass

        register_event_action("0", DummyEvent, DummyAction)
        with pytest.raises(ValueError):
            register_event_action("0", DummyEvent, DummyAction)

        del _EVENT_ACTION_MAP["0"][DummyEvent.__name__]  # Cleanup


def test_event_handler_registry_more_tests():
    # TODO: Add more tests
    raise NotImplementedError  # Reminder to add more tests
