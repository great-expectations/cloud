from __future__ import annotations

from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction


def test_running_checkpoint_action(context, checkpoint_event):
    action = RunCheckpointAction(context=context)
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    action_result = action.run(event=checkpoint_event, id=event_id)
    assert action_result.type == checkpoint_event.type
    assert action_result.id == event_id
