from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID

import pytest
from pydantic.v1 import ValidationError, parse_raw_as

from great_expectations_cloud.agent.models import (
    AgentBaseExtraForbid,
    AgentBaseExtraIgnore,
    Event,
    MissingEventSubclasses,
    RunCheckpointEvent,
    UnknownEvent,
    _build_event_union,
    all_subclasses,
)

pytestmark = pytest.mark.unit


class TestDynamicEventUnion:
    """Test the dynamic Event union system."""

    def test_all_subclasses_function(self):
        """Test that all_subclasses correctly discovers nested subclasses."""

        # Create a test class hierarchy
        class BaseTest(AgentBaseExtraIgnore):
            pass

        class ChildTest(BaseTest):
            pass

        class GrandchildTest(ChildTest):
            pass

        # Test that all_subclasses finds all descendants
        subclasses = all_subclasses(BaseTest)
        subclass_names = [cls.__name__ for cls in subclasses]

        assert "ChildTest" in subclass_names
        assert "GrandchildTest" in subclass_names
        assert (
            len([cls for cls in subclasses if cls.__name__ in ["ChildTest", "GrandchildTest"]]) == 2
        )

    def test_build_event_union_discovers_existing_events(self):
        """Test that _build_event_union discovers all existing event classes."""

        event_classes = _build_event_union()
        event_class_names = [cls.__name__ for cls in event_classes]

        # Should include core event classes
        assert "UnknownEvent" in event_class_names
        assert "RunCheckpointEvent" in event_class_names
        assert "ListAssetNamesEvent" in event_class_names

        # Should have a reasonable number of events (at least 10)
        assert len(event_classes) >= 10

    def test_build_event_union_includes_both_base_types(self):
        """Test that _build_event_union includes subclasses of both base classes."""

        event_classes = _build_event_union()

        # Check that we have classes from both base types
        forbid_classes = [cls for cls in event_classes if issubclass(cls, AgentBaseExtraForbid)]
        ignore_classes = [cls for cls in event_classes if issubclass(cls, AgentBaseExtraIgnore)]

        assert len(forbid_classes) > 0, "Should have at least one AgentBaseExtraForbid subclass"
        assert len(ignore_classes) > 0, "Should have at least one AgentBaseExtraIgnore subclass"

        # UnknownEvent should be in forbid_classes
        forbid_names = [cls.__name__ for cls in forbid_classes]
        assert "UnknownEvent" in forbid_names

    def test_build_event_union_filters_classes_without_type_field(self):
        """Test that classes without proper type fields are filtered out."""

        # Create a class without a type field
        class InvalidEvent(AgentBaseExtraIgnore):
            some_field: str

        # Create a class with type field but no default
        class AlsoInvalidEvent(AgentBaseExtraIgnore):
            type: str
            some_field: str

        event_classes = _build_event_union()
        event_class_names = [cls.__name__ for cls in event_classes]

        # These invalid classes should not be included
        assert "InvalidEvent" not in event_class_names
        assert "AlsoInvalidEvent" not in event_class_names

    def test_build_event_union_includes_classes_with_proper_type_field(self):
        """Test that classes with proper type fields are included."""

        # Create a valid event class
        class ValidTestEvent(AgentBaseExtraIgnore):
            type: Literal["valid_test_event"] = "valid_test_event"
            test_field: str = "test"

        event_classes = _build_event_union()
        event_class_names = [cls.__name__ for cls in event_classes]

        # Our valid class should be included
        assert "ValidTestEvent" in event_class_names

    def test_event_union_removes_duplicates(self):
        """Test that the event union doesn't include duplicate classes."""

        event_classes = _build_event_union()
        event_class_names = [cls.__name__ for cls in event_classes]

        # Check for duplicates
        assert len(event_class_names) == len(set(event_class_names)), (
            "Event classes should be unique"
        )

    def test_event_parsing_with_dynamic_union(self):
        """Test that the dynamic Event union can parse various event types."""

        # Test parsing UnknownEvent
        unknown_data = {"type": "unknown_event"}
        unknown_event: Any = parse_raw_as(Event, json.dumps(unknown_data))  # type: ignore[arg-type]  # MyPy sees plain Union, runtime uses Pydantic discriminated union
        assert isinstance(unknown_event, UnknownEvent)
        assert unknown_event.type == "unknown_event"

        # Test parsing RunCheckpointEvent
        checkpoint_data = {
            "type": "run_checkpoint_request",
            "datasource_names_to_asset_names": {"ds1": ["asset1"]},
            "checkpoint_id": "12345678-1234-1234-1234-123456789012",
        }
        checkpoint_event: Any = parse_raw_as(Event, json.dumps(checkpoint_data))  # type: ignore[arg-type]  # MyPy sees plain Union, runtime uses Pydantic discriminated union
        assert isinstance(checkpoint_event, RunCheckpointEvent)
        assert checkpoint_event.type == "run_checkpoint_request"
        assert checkpoint_event.checkpoint_id == UUID("12345678-1234-1234-1234-123456789012")

    def test_dynamic_event_union_extensibility(self):
        """Test that new event classes can be added and discovered."""

        # Define new event classes
        class NewTestEventForbid(AgentBaseExtraForbid):
            type: Literal["new_test_event_forbid"] = "new_test_event_forbid"
            custom_data: str = "test"

        class NewTestEventIgnore(AgentBaseExtraIgnore):
            type: Literal["new_test_event_ignore"] = "new_test_event_ignore"
            another_field: int = 42

        # Rebuild the event union to include new classes
        event_classes = _build_event_union()
        event_class_names = [cls.__name__ for cls in event_classes]

        # New classes should be discovered
        assert "NewTestEventForbid" in event_class_names
        assert "NewTestEventIgnore" in event_class_names

        # Check that they have the right base classes
        forbid_classes = [cls for cls in event_classes if issubclass(cls, AgentBaseExtraForbid)]
        ignore_classes = [cls for cls in event_classes if issubclass(cls, AgentBaseExtraIgnore)]

        forbid_names = [cls.__name__ for cls in forbid_classes]
        ignore_names = [cls.__name__ for cls in ignore_classes]

        assert "NewTestEventForbid" in forbid_names
        assert "NewTestEventIgnore" in ignore_names

    def test_backward_compatibility(self):
        """Test that existing functionality is preserved."""

        # Test that we can still parse the original event types
        original_events = [
            ("unknown_event", UnknownEvent),
            ("run_checkpoint_request", RunCheckpointEvent),
        ]

        for event_type, expected_class in original_events:
            if event_type == "run_checkpoint_request":
                data = {
                    "type": event_type,
                    "datasource_names_to_asset_names": {"ds": ["asset"]},
                    "checkpoint_id": "12345678-1234-1234-1234-123456789012",
                }
            else:
                data = {"type": event_type}

            event: Any = parse_raw_as(Event, json.dumps(data))  # type: ignore[arg-type]  # MyPy sees plain Union, runtime uses Pydantic discriminated union
            assert isinstance(event, expected_class)
            assert event.type == event_type

    def test_event_union_error_handling(self):
        """Test error handling in the dynamic event union system."""

        # Test that invalid event data raises appropriate errors
        with pytest.raises(ValidationError):  # Should raise validation error
            parse_raw_as(Event, json.dumps({"type": "nonexistent_event_type"}))  # type: ignore[arg-type]  # MyPy sees plain Union, runtime uses Pydantic discriminated union

        # Test that malformed JSON raises errors
        with pytest.raises((ValidationError, ValueError)):
            parse_raw_as(Event, "invalid json")  # type: ignore[arg-type]  # MyPy sees plain Union, runtime uses Pydantic discriminated union

    def test_missing_event_subclasses_error(self, mocker):
        """Test that MissingEventSubclasses is raised when no valid event classes are found."""

        # Mock all_subclasses to return empty lists (no subclasses found)
        mocker.patch("great_expectations_cloud.agent.models.all_subclasses", return_value=[])

        # This should raise MissingEventSubclasses since no valid classes are found
        with pytest.raises(MissingEventSubclasses):
            _build_event_union()
