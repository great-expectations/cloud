from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import orjson
import packaging.version
import pytest
from great_expectations.experimental.metric_repository.metrics import (
    MetricTypes,
)

from great_expectations_cloud.agent.actions import (
    AgentAction,
    DraftDatasourceConfigAction,
    ListAssetNamesAction,
    MetricListAction,
    RunCheckpointAction,
)
from great_expectations_cloud.agent.agent_warnings import GXAgentUserWarning
from great_expectations_cloud.agent.event_handler import (
    _EVENT_ACTION_MAP,
    EventAlreadyRegisteredError,
    EventHandler,
    NoVersionImplementationError,
    _get_major_version,
    register_event_action,
)
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.models import (
    DraftDatasourceConfigEvent,
    Event,
    ListAssetNamesEvent,
    RunCheckpointEvent,
    RunMetricsListEvent,
    RunOnboardingDataAssistantEvent,
    UnknownEvent,
)
from tests.agent.conftest import DummyAction, DummyEvent

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


@pytest.fixture
def org_id_different_from_event() -> str:
    return "f67dd790-fe65-4f8a-bbc8-2c11f20e34b8"


@pytest.fixture()
def example_event():
    return RunOnboardingDataAssistantEvent(
        type="onboarding_data_assistant_request.received",
        datasource_name="abc",
        data_asset_name="boo",
        organization_id=uuid4(),
    )


class TestEventHandler:
    def test_event_handler_unknown_event(self, mock_context):
        event = UnknownEvent()
        correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
        handler = EventHandler(context=mock_context)
        with pytest.warns(GXAgentUserWarning):
            result = handler.handle_event(
                event=event, id=correlation_id, base_url="", auth_key="", organization_id=uuid4()
            )
        assert result.type == "unknown_event"

    @pytest.mark.parametrize(
        "event_name, event, action_type",
        [
            (
                "RunCheckpointEvent",
                RunCheckpointEvent(
                    checkpoint_id=UUID("3ecd140b-1dd5-41f4-bdb1-c8009d4f1940"),
                    datasource_names_to_asset_names={"Data Source name": {"Asset name"}},
                    organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                ),
                RunCheckpointAction,
            ),
            (
                "DraftDatasourceConfigEvent",
                DraftDatasourceConfigEvent(
                    config_id=uuid4(),
                    organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                ),
                DraftDatasourceConfigAction,
            ),
            (
                "ListAssetNamesEvent",
                ListAssetNamesEvent(
                    datasource_name="test-datasource",
                    organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                ),
                ListAssetNamesAction,
            ),
            (
                "RunMetricsListEvent",
                RunMetricsListEvent(
                    datasource_name="test-datasource",
                    data_asset_name="test-data-asset",
                    metric_names=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
                    organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                ),
                MetricListAction,
            ),
        ],
    )
    def test_event_handler_handles_all_events(
        self,
        mock_context,
        mocker: MockerFixture,
        event_name: str,
        event: Event,
        action_type: type[AgentAction[Any]],
    ):
        action = mocker.MagicMock(autospec=action_type)
        mocker.patch("great_expectations_cloud.agent.event_handler._GX_MAJOR_VERSION", "1")

        mocker.patch.dict(_EVENT_ACTION_MAP, {"1": {event_name: action}}, clear=True)
        correlation_id = str(uuid4())
        handler = EventHandler(context=mock_context)

        fake_org_id = UUID("00000000-0000-0000-0000-000000000000")

        handler.handle_event(
            event=event, id=correlation_id, base_url="", auth_key="", organization_id=fake_org_id
        )

        action.assert_called_with(
            context=mock_context, base_url="", organization_id=fake_org_id, auth_key=""
        )
        action().run.assert_called_with(event=event, id=correlation_id)

    @pytest.mark.parametrize(
        "event_name, event, action_type",
        [
            (
                "RunCheckpointEvent",
                RunCheckpointEvent(
                    checkpoint_id=UUID("3ecd140b-1dd5-41f4-bdb1-c8009d4f1940"),
                    datasource_names_to_asset_names={"Data Source name": {"Asset name"}},
                    organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                ),
                RunCheckpointAction,
            ),
            (
                "DraftDatasourceConfigEvent",
                DraftDatasourceConfigEvent(
                    config_id=uuid4(),
                    organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                ),
                DraftDatasourceConfigAction,
            ),
            (
                "ListTableNamesEvent",
                ListAssetNamesEvent(
                    datasource_name="test-datasource",
                    organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                ),
                ListAssetNamesAction,
            ),
            (
                "RunMetricsListEvent",
                RunMetricsListEvent(
                    datasource_name="test-datasource",
                    data_asset_name="test-data-asset",
                    metric_names=[MetricTypes.TABLE_COLUMN_TYPES, MetricTypes.TABLE_COLUMNS],
                    organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                ),
                MetricListAction,
            ),
        ],
    )
    def test_event_handler_checks_org_id_in_context_matches_event(
        self,
        mock_context,
        mocker: MockerFixture,
        event_name: str,
        event: Event,
        action_type: type[AgentAction[Any]],
        org_id_different_from_event: str,
    ):
        action = mocker.MagicMock(autospec=action_type)
        mocker.patch("great_expectations_cloud.agent.event_handler._GX_MAJOR_VERSION", "1")

        mocker.patch.dict(_EVENT_ACTION_MAP, {"1": {event_name: action}}, clear=True)
        correlation_id = str(uuid4())
        handler = EventHandler(context=mock_context)

        with pytest.raises(GXAgentError):
            handler.handle_event(
                event=event,
                id=correlation_id,
                base_url="",
                auth_key="",
                organization_id=uuid.UUID(org_id_different_from_event),
            )

    def test_event_handler_raises_on_no_version_implementation(
        self, mock_context, mocker: MockerFixture
    ):
        gx_major_version = mocker.patch(
            "great_expectations_cloud.agent.event_handler._GX_MAJOR_VERSION"
        )
        gx_major_version.return_value = "NOT_A_REAL_VERSION"

        handler = EventHandler(context=mock_context)

        with pytest.raises(NoVersionImplementationError):
            handler.get_event_action(DummyEvent, base_url="", auth_key="", organization_id=uuid4())  # type: ignore[arg-type]  # Dummy event only used in testing


class TestEventHandlerRegistry:
    @pytest.mark.parametrize(
        "version",
        [
            "0",
            "1234567890",  # Version that doesn't exist already in the map
        ],
    )
    def test_register_event_action(self, mocker: MockerFixture, version: str):
        mocker.patch.dict(_EVENT_ACTION_MAP, {}, clear=True)
        register_event_action(version, DummyEvent, DummyAction)  # type: ignore[arg-type]
        assert _EVENT_ACTION_MAP[version][DummyEvent.__name__] == DummyAction

    def test_register_event_action_already_registered(self, mocker: MockerFixture):
        mocker.patch.dict(_EVENT_ACTION_MAP, {}, clear=True)
        register_event_action("0", DummyEvent, DummyAction)  # type: ignore[arg-type]
        with pytest.raises(EventAlreadyRegisteredError):
            register_event_action("0", DummyEvent, DummyAction)  # type: ignore[arg-type]

    def test_event_handler_gets_correct_event_action(self, mocker: MockerFixture, mock_context):
        mocker.patch.dict(_EVENT_ACTION_MAP, {}, clear=True)
        DummyEvent.organization_id = uuid.UUID("00000000-0000-0000-0000-000000000000")
        register_event_action("1", DummyEvent, DummyAction)  # type: ignore[arg-type]
        handler = EventHandler(context=mock_context)
        assert isinstance(
            handler.get_event_action(
                DummyEvent,  # type: ignore[arg-type]  # Dummy event only used in testing
                base_url="",
                auth_key="",
                organization_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
            ),
            DummyAction,
        )

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

    def test__get_major_version_raises_on_invalid_version(self):
        with pytest.raises(packaging.version.InvalidVersion):
            _get_major_version("invalid_version")


def test_parse_event_extra_field_is_ignored(example_event):
    event_dict = example_event.dict()
    event_dict["new_field"] = "surprise!"
    serialized_bytes = orjson.dumps(event_dict)
    event = EventHandler.parse_event_from(serialized_bytes)

    # Extra field is ignored, which allows request to still be received - ZELDA-770
    assert event.type == "onboarding_data_assistant_request.received"


def test_parse_event_missing_required_field(example_event):
    event_dict = example_event.dict(exclude={"datasource_name"})
    serialized_bytes = orjson.dumps(event_dict)
    event = EventHandler.parse_event_from(serialized_bytes)

    assert event.type == "unknown_event"


def test_parse_event_invalid_json(example_event):
    event_dict = example_event.dict()
    invalid_json_addition = "}}}}"
    serialized_bytes = (orjson.dumps(event_dict).decode() + invalid_json_addition).encode("utf-8")
    event = EventHandler.parse_event_from(serialized_bytes)

    assert event.type == "unknown_event"


def test_parse_event(example_event):
    serialized_bytes = example_event.json().encode("utf-8")
    event = EventHandler.parse_event_from(serialized_bytes)

    assert event.type == "onboarding_data_assistant_request.received"
