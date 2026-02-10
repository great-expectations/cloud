from __future__ import annotations

import asyncio
import logging
from http import HTTPStatus
from typing import TYPE_CHECKING, Final
from urllib.parse import urljoin
from uuid import UUID

from great_expectations.core.http import create_session
from pydantic import BaseModel
from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.actions.utils import ensure_openai_credentials
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.agent import (
    SqlExpectationAgent,
)
from great_expectations_cloud.agent.expect_ai.sql_expectation_agent.state import (
    SqlExpectationInput,
)
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner
from great_expectations_cloud.agent.models import (
    CreatedResource,
    GenerateSqlExpectationEvent,
)
from great_expectations_cloud.agent.services.expectation_draft_config_service import (
    ExpectationDraftConfigService,
)

if TYPE_CHECKING:
    from great_expectations.expectations import UnexpectedRowsExpectation


logger = logging.getLogger(__name__)

CREATED_VIA_PROMPT: Final[str] = "expect_ai_sql_generation"


class PromptMetadataResponse(BaseModel):
    id: UUID
    user_prompt: str
    data_source_name: str
    asset_name: str
    batch_definition_name: str


class GenerateSqlExpectationAction(AgentAction[GenerateSqlExpectationEvent]):
    @override
    def run(self, event: GenerateSqlExpectationEvent, id: str) -> ActionResult:
        ensure_openai_credentials()

        prompt_metadata = self._get_prompt_metadata(event.expectation_prompt_id)
        metric_service = MetricService(context=self._context)
        query_runner = QueryRunner(context=self._context)

        agent = SqlExpectationAgent(
            query_runner=query_runner,
            metric_service=metric_service,
        )

        sql_input = SqlExpectationInput(
            organization_id=str(self._domain_context.organization_id),
            workspace_id=str(self._domain_context.workspace_id),
            user_prompt=prompt_metadata.user_prompt,
            data_source_name=prompt_metadata.data_source_name,
            data_asset_name=prompt_metadata.asset_name,
            batch_definition_name=prompt_metadata.batch_definition_name,
        )

        expectation = asyncio.run(agent.arun(input=sql_input))

        created_resource = self._create_expectation_draft_config(
            data_source_name=prompt_metadata.data_source_name,
            data_asset_name=prompt_metadata.asset_name,
            expectation=expectation,
            event_id=id,
        )

        return ActionResult(id=id, type=event.type, created_resources=[created_resource])

    def _get_prompt_metadata(self, expectation_prompt_id: UUID) -> PromptMetadataResponse:
        url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._domain_context.organization_id!s}/workspaces/{self._domain_context.workspace_id!s}/expectations/prompt-metadata/{expectation_prompt_id}",
        )

        with create_session(access_token=self._auth_key) as session:
            response = session.get(url=url)
            if response.status_code != HTTPStatus.OK:
                logger.error(
                    f"Failed to retrieve prompt metadata for expectation_prompt_id "
                    f"{expectation_prompt_id}. Status code: {response.status_code}"
                )
                msg = f"Failed to retrieve prompt metadata for expectation_prompt_id {expectation_prompt_id}. Status code: {response.status_code}."
                if response.text:
                    msg += f" {response.text}"
                raise GXAgentError(msg)

            logger.info(
                f"Retrieved prompt metadata for expectation_prompt_id {expectation_prompt_id}"
            )
            return PromptMetadataResponse(**response.json())

    def _create_expectation_draft_config(
        self,
        data_source_name: str,
        data_asset_name: str,
        expectation: UnexpectedRowsExpectation,
        event_id: str,
    ) -> CreatedResource:

        draft_config_service = ExpectationDraftConfigService(
            context=self._context,
            created_via=CREATED_VIA_PROMPT,
        )

        created_resource = draft_config_service.create_single_expectation_draft_config(
            data_source_name=data_source_name,
            data_asset_name=data_asset_name,
            expectation=expectation,
            event_id=event_id,
        )

        logger.info(
            f"Successfully created expectation draft config with id: {created_resource.resource_id}"
        )

        return created_resource


register_event_action("1", GenerateSqlExpectationEvent, GenerateSqlExpectationAction)
