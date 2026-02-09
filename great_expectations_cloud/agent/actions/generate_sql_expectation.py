from __future__ import annotations

import asyncio
import logging
import os
from http import HTTPStatus
from typing import TYPE_CHECKING, Final
from urllib.parse import urljoin
from uuid import UUID

from great_expectations.core.http import create_session
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset
from pydantic import BaseModel
from typing_extensions import override

from great_expectations_cloud.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations_cloud.agent.config import GxAgentEnvVars
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.exceptions import GXAgentError, MercuryError
from great_expectations_cloud.agent.expect_ai.exceptions import (
    InvalidAssetTypeError,
    InvalidDataSourceTypeError,
)
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

if TYPE_CHECKING:
    from great_expectations.expectations import UnexpectedRowsExpectation


LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

# Constants
CREATED_VIA_PROMPT: Final[str] = "expect_ai_sql_generation"


# Models
class PromptMetadataResponse(BaseModel):
    """Response model for prompt metadata from Mercury API."""

    user_prompt: str
    data_source_name: str
    asset_name: str
    batch_definition_name: str


class GenerateSqlExpectationAction(AgentAction[GenerateSqlExpectationEvent]):
    @override
    def run(self, event: GenerateSqlExpectationEvent, id: str) -> ActionResult:
        if not self._has_openai_credentials():
            msg = "OpenAI credentials not configured. Set OPENAI_API_KEY environment variable to enable ExpectAI."
            raise GXAgentError(msg)
        # Get prompt metadata from Mercury API
        if not event.expectation_prompt_id:
            msg = "GenerateSqlExpectationEvent is missing required field: expectation_prompt_id"
            raise GXAgentError(msg)
        prompt_metadata = self._get_prompt_metadata(event.expectation_prompt_id)

        metric_service = MetricService(context=self._context)
        query_runner = QueryRunner(context=self._context)

        agent = SqlExpectationAgent(
            query_runner=query_runner,
            metric_service=metric_service,
        )

        # Create SqlExpectationInput from prompt metadata
        sql_input = SqlExpectationInput(
            organization_id=str(self._domain_context.organization_id),
            workspace_id=str(self._domain_context.workspace_id),
            user_prompt=prompt_metadata.user_prompt,
            data_source_name=prompt_metadata.data_source_name,
            data_asset_name=prompt_metadata.asset_name,
            batch_definition_name=prompt_metadata.batch_definition_name,
        )

        # Create UnexpectedRowsExpectation from prompt
        expectation = asyncio.run(agent.arun(input=sql_input))

        created_resource = self._create_expectation_draft_config(
            data_source_name=prompt_metadata.data_source_name,
            data_asset_name=prompt_metadata.asset_name,
            expectation=expectation,
            event_id=id,
        )

        return ActionResult(id=id, type=event.type, created_resources=[created_resource])

    def _get_prompt_metadata(self, expectation_prompt_id: UUID) -> PromptMetadataResponse:
        """Retrieve prompt metadata from Mercury API.

        Makes an HTTP GET request to the Mercury API to fetch metadata for the given
        expectation prompt ID, including user prompt and data source details.

        Args:
            expectation_prompt_id: The UUID of the expectation prompt to retrieve metadata for.

        Returns:
            PromptMetadataResponse containing the prompt metadata including user prompt,
            data source name, asset name and batch definition name.

        Raises:
            MercuryError: If the API request returns a non-200 status code.
        """
        url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._domain_context.organization_id!s}/workspaces/{self._domain_context.workspace_id!s}/expectations/prompt-metadata/{expectation_prompt_id}",
        )

        with create_session(access_token=self._auth_key) as session:
            response = session.get(url=url)
            if response.status_code != HTTPStatus.OK:
                LOGGER.error(
                    f"Failed to retrieve prompt metadata for expectation_prompt_id "
                    f"{expectation_prompt_id}. Status code: {response.status_code}"
                )
                raise MercuryError(
                    message=f"Failed to retrieve prompt metadata for expectation_prompt_id {expectation_prompt_id}: {response.text}",
                    status_code=response.status_code,
                )

            LOGGER.info(
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
        """Create expectation draft config via Mercury API.

        Args:
            data_source_name: Name of the data source containing the data asset.
            data_asset_name: Name of the data asset to validate.
            expectation: The UnexpectedRowsExpectation to create a draft config for.
            event_id: Event ID to include in the Agent-Job-Id header for tracing.

        Returns:
            CreatedResource containing the draft config ID and ExpectationDraftConfig type.
        """
        # Get asset to validate types and get asset_id
        datasource = self._context.data_sources.get(data_source_name)
        if not isinstance(datasource, SQLDatasource):
            raise InvalidDataSourceTypeError(type(datasource), (SQLDatasource,))
        asset = datasource.get_asset(data_asset_name)
        if not isinstance(asset, TableAsset):
            raise InvalidAssetTypeError(type(asset), (TableAsset,))

        # Prepare expectation object with metadata
        expectation_config = expectation.configuration.to_json_dict()
        expectation_config["autogenerated"] = True
        expectation_config["created_via"] = CREATED_VIA_PROMPT

        # Backend expects `expectation_type` instead of `type`
        if "type" in expectation_config:
            expectation_type = expectation_config.pop("type")
            expectation_config["expectation_type"] = expectation_type

        draft_config_payload = {
            "data": [
                {
                    "asset_id": str(asset.id),
                    "draft_expectation": expectation_config,
                    "organization_id": str(self._domain_context.organization_id),
                }
            ]
        }

        url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._domain_context.organization_id}/workspaces/{self._domain_context.workspace_id}/expectation-draft-configs",
        )

        headers = {"Agent-Job-Id": event_id}
        with create_session(access_token=self._auth_key) as session:
            response = session.post(url=url, json=draft_config_payload, headers=headers)

        if response.status_code != HTTPStatus.CREATED:
            raise MercuryError(
                message=f"Failed to create expectation draft config: {response.text}",
                status_code=response.status_code,
            )

        draft_config_id = UUID(response.json()["data"][0]["id"])
        LOGGER.info(f"Successfully created expectation draft config with id: {draft_config_id}")

        return CreatedResource(resource_id=str(draft_config_id), type="ExpectationDraftConfig")

    def _has_openai_credentials(self) -> bool:
        """Check if OpenAI API key is configured."""
        try:
            env_vars = GxAgentEnvVars()
        except Exception:
            # If we can't load env vars, check environment directly as fallback
            return os.getenv("OPENAI_API_KEY") is not None
        else:
            return env_vars.expect_ai_enabled


register_event_action("1", GenerateSqlExpectationEvent, GenerateSqlExpectationAction)
