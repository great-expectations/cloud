from __future__ import annotations

from typing import Literal
from unittest import mock
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from great_expectations.data_context import CloudDataContext

from great_expectations_cloud.agent.actions import (
    ActionResult,
    AgentAction,
    ColumnDescriptiveMetricsAction,
    ListTableNamesAction,
    RunCheckpointAction,
    RunMissingnessDataAssistantAction,
    RunOnboardingDataAssistantAction,
)
from great_expectations_cloud.agent.actions.draft_datasource_config_action import (
    DraftDatasourceConfigAction,
)
from great_expectations_cloud.agent.event_handler import (
    _EVENT_ACTION_MAP,
    EventHandler,
    NoVersionImplementationError,
    UnknownEventError,
    _get_major_version,
    register_event_action,
)
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    Event,
    EventBase,
    ListTableNamesEvent,
    RunCheckpointEvent,
    RunColumnDescriptiveMetricsEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
    UnknownEvent,
)

pytestmark = pytest.mark.unit


class TestEventHandler:
    def test_event_handler_raises_for_unknown_event(self):
        event = UnknownEvent()
        correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
        context = MagicMock(autospec=CloudDataContext)
        handler = EventHandler(context=context)

        with pytest.raises(UnknownEventError):
            handler.handle_event(event=event, id=correlation_id)

    @pytest.mark.parametrize(
        "event_name, event, action_type",
        [
            (
                "RunMissingnessDataAssistantEvent",
                RunMissingnessDataAssistantEvent(
                    datasource_name="test-datasource",
                    data_asset_name="test-data-asset",
                ),
                RunMissingnessDataAssistantAction,
            ),
            (
                "RunOnboardingDataAssistantEvent",
                RunOnboardingDataAssistantEvent(
                    datasource_name="test-datasource", data_asset_name="test-data-asset"
                ),
                RunOnboardingDataAssistantAction,
            ),
            (
                "RunCheckpointEvent",
                RunCheckpointEvent(
                    checkpoint_id="3ecd140b-1dd5-41f4-bdb1-c8009d4f1940",
                    datasource_names_to_asset_names={"Data Source name": {"Asset name"}},
                ),
                RunCheckpointAction,
            ),
            (
                "DraftDatasourceConfigEvent",
                DraftDatasourceConfigEvent(config_id=uuid4()),
                DraftDatasourceConfigAction,
            ),
            (
                "ListTableNamesEvent",
                ListTableNamesEvent(datasource_name="test-datasource"),
                ListTableNamesAction,
            ),
            (
                "RunColumnDescriptiveMetricsEvent",
                RunColumnDescriptiveMetricsEvent(
                    datasource_name="test-datasource", data_asset_name="test-data-asset"
                ),
                ColumnDescriptiveMetricsAction,
            ),
        ],
    )
    def test_event_handler_handles_all_events(
        self, mocker, event_name: str, event: Event, action_type: type[AgentAction]
    ):
        action = MagicMock(autospec=action_type)
        mocker.patch("great_expectations_cloud.agent.event_handler._GX_MAJOR_VERSION", "1")

        with mock.patch.dict(_EVENT_ACTION_MAP, {"1": {event_name: action}}):
            correlation_id = str(uuid4())
            context = MagicMock(autospec=CloudDataContext)
            handler = EventHandler(context=context)

            handler.handle_event(event=event, id=correlation_id)

            action.assert_called_with(context=context)
            action().run.assert_called_with(event=event, id=correlation_id)

    def test_event_handler_raises_on_no_version_implementation(self, mocker):
        gx_major_version = mocker.patch(
            "great_expectations_cloud.agent.event_handler._GX_MAJOR_VERSION"
        )
        gx_major_version.return_value = "NOT_A_REAL_VERSION"

        context = MagicMock(autospec=CloudDataContext)
        handler = EventHandler(context=context)

        with pytest.raises(NoVersionImplementationError):
            handler.get_event_action(DummyEvent)


class DummyEvent(EventBase):
    type: Literal["event_name.received"] = "event_name.received"


class DummyAction(AgentAction):
    def run(self, event: Event, id: str) -> ActionResult:
        pass


class TestEventHandlerRegistry:
    def test_register_event_action(self):
        register_event_action("0", DummyEvent, DummyAction)
        assert _EVENT_ACTION_MAP["0"][DummyEvent.__name__] == DummyAction

        del _EVENT_ACTION_MAP["0"][DummyEvent.__name__]  # Cleanup

    def test_register_event_action_already_registered(self):
        register_event_action("0", DummyEvent, DummyAction)
        with pytest.raises(ValueError):
            register_event_action("0", DummyEvent, DummyAction)

        del _EVENT_ACTION_MAP["0"][DummyEvent.__name__]  # Cleanup

    def test_event_handler_gets_correct_event_action(self):
        register_event_action("0", DummyEvent, DummyAction)
        context = MagicMock(autospec=CloudDataContext)
        handler = EventHandler(context=context)

        assert type(handler.get_event_action(DummyEvent)) == DummyAction

        del _EVENT_ACTION_MAP["0"][DummyEvent.__name__]

    @pytest.mark.parametrize(
        "version, expected",
        [
            ("0.0.1", "0"),
            ("1.0.1", "1"),
            ("2.0.1", "2"),
            ("1.1alpha1", "1"),
            ("10.0.1", "10"),
        ],
    )
    def test__get_major_version(self, version: str, expected: str):
        assert _get_major_version(version) == expected

    # Note: Version coerces to LegacyVersion rather than raising an error, so we don't test for invalid versions.
