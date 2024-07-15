from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import ActionResult, AgentAction
from great_expectations_cloud.agent.actions.data_assistants.utils import (
    DataAssistantType,
    build_action_result,
    build_batch_request,
)
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.models import RunOnboardingDataAssistantEvent

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


class RunOnboardingDataAssistantAction(AgentAction[RunOnboardingDataAssistantEvent]):
    def __init__(self, context: CloudDataContext):
        super().__init__(context=context)
        self._data_assistant = self._context.assistants.onboarding

    @override
    def run(
        self,
        event: RunOnboardingDataAssistantEvent,
        id: str,
    ) -> ActionResult:
        batch_request = build_batch_request(context=self._context, event=event)

        data_assistant_result = self._data_assistant.run(
            batch_request=batch_request,
        )
        return build_action_result(
            context=self._context,
            data_assistant_name=DataAssistantType.ONBOARDING,
            event=event,
            data_assistant_result=data_assistant_result,
            id=id,
        )


register_event_action("0", RunOnboardingDataAssistantEvent, RunOnboardingDataAssistantAction)
