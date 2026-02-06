from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

from great_expectations.metrics import BatchColumnTypes, BatchRowCount
from great_expectations.metrics.batch.batch_column_types import BatchColumnTypesResult
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.actions.expectation_pruner import ExpectationPruner
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import AssetReviewAgent
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    GenerateExpectationsInput,
)
from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
from great_expectations_cloud.agent.expect_ai.tools.metrics import AgentToolsManager
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner
from great_expectations_cloud.agent.models import (
    DomainContext,
    GenerateExpectationsEvent,
    RunRdAgentEvent,
)
from great_expectations_cloud.agent.services.expectation_draft_config_service import (
    ExpectationDraftConfigService,
)
from great_expectations_cloud.agent.services.expectation_service import (
    ExpectationService,
    ListExpectationsError,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

MAX_PRUNED_EXPECTATIONS = 10

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GenerateExpectationsAction(AgentAction[GenerateExpectationsEvent | RunRdAgentEvent]):
    def __init__(
        self,
        context: CloudDataContext,
        base_url: str,
        domain_context: DomainContext,
        auth_key: str,
    ):
        super().__init__(
            context=context, base_url=base_url, domain_context=domain_context, auth_key=auth_key
        )

    @override
    def run(self, event: GenerateExpectationsEvent | RunRdAgentEvent, id: str) -> ActionResult:
        # Check for OpenAI credentials
        if not self._has_openai_credentials():
            msg = (
                "OpenAI credentials not configured. "
                "Set OPENAI_API_KEY environment variable to enable ExpectAI."
            )
            raise GXAgentError(msg)

        # Do not proceed with generating Expectations if the Data Asset is empty
        if self._batch_contains_no_rows(event):
            error_message = "Could not generate Expectations because the Data Asset has no records. Ensure the table or view connected to your Data Asset has records and try again."
            raise RuntimeError(error_message)

        # Initialize services
        metric_service = MetricService(context=self._context)
        tools_manager = AgentToolsManager(
            context=self._context,
            metric_service=metric_service,
        )
        query_runner = QueryRunner(context=self._context)

        expectation_service = ExpectationService(
            context=self._context,
            base_url=self._base_url,
            auth_key=self._auth_key,
        )

        # Fetch existing expectations (used as context for the AI to avoid duplicates)
        try:
            existing_expectation_contexts = (
                expectation_service.get_existing_expectations_by_data_asset(
                    data_source_name=event.datasource_name,
                    data_asset_name=event.data_asset_name,
                    organization_id=self._domain_context.organization_id,
                    workspace_id=self._domain_context.workspace_id,
                )
            )
        except ListExpectationsError:
            logger.exception(
                "list_expectations.failed", extra={"data_asset_name": event.data_asset_name}
            )
            existing_expectation_contexts = []
        except Exception:
            logger.exception(
                "list_expectations.unexpected_error",
                extra={"data_asset_name": event.data_asset_name},
            )
            existing_expectation_contexts = []

        # Generate expectations using AssetReviewAgent
        agent = AssetReviewAgent(
            tools_manager=tools_manager,
            query_runner=query_runner,
            metric_service=metric_service,
        )

        generate_expectations_input = GenerateExpectationsInput(
            organization_id=str(self._domain_context.organization_id),
            workspace_id=str(event.workspace_id),
            data_source_name=event.datasource_name,
            data_asset_name=event.data_asset_name,
            batch_definition_name=event.batch_definition_name,
            batch_parameters=event.batch_parameters,
            existing_expectation_contexts=existing_expectation_contexts,
        )

        expectation_suite = asyncio.run(
            agent.arun(generate_expectations_input=generate_expectations_input)
        )
        expectations = expectation_suite.expectations

        # Get column names for pruning invalid columns
        batch_definition = (
            self._context.data_sources.get(event.datasource_name)
            .get_asset(event.data_asset_name)
            .get_batch_definition(event.batch_definition_name)
        )

        column_types_result = metric_service.get_metric_result(
            batch_definition=batch_definition,
            metric=BatchColumnTypes(),
            batch_parameters=event.batch_parameters,
        )

        # Prune expectations
        expectation_pruner = ExpectationPruner(max_expectations=MAX_PRUNED_EXPECTATIONS)

        if isinstance(column_types_result, BatchColumnTypesResult):
            valid_columns = {col.name for col in column_types_result.value}
            expectations = expectation_pruner.prune_invalid_columns(
                expectations=expectations,
                valid_columns=valid_columns,
            )

        expectations = expectation_pruner.prune_expectations(expectations)

        # Create draft configs
        draft_config_service = ExpectationDraftConfigService(
            context=self._context,
            base_url=self._base_url,
            auth_key=self._auth_key,
            organization_id=self._domain_context.organization_id,
            workspace_id=self._domain_context.workspace_id,
        )

        created_resources = draft_config_service.create_expectation_draft_configs(
            data_source_name=event.datasource_name,
            data_asset_name=event.data_asset_name,
            expectations=expectations,
            event_id=id,
        )

        return ActionResult(id=id, type=event.type, created_resources=created_resources)

    def _has_openai_credentials(self) -> bool:
        """Check if OpenAI credentials are configured."""
        return os.getenv("OPENAI_API_KEY") is not None

    def _batch_contains_no_rows(self, event: GenerateExpectationsEvent | RunRdAgentEvent) -> bool:
        """Check if the batch has no rows."""
        batch_definition = (
            self._context.data_sources.get(event.datasource_name)
            .get_asset(event.data_asset_name)
            .get_batch_definition(event.batch_definition_name)
        )

        metric_service = MetricService(context=self._context)
        row_count_result = metric_service.get_metric_result(
            batch_definition=batch_definition,
            metric=BatchRowCount(),
            batch_parameters=event.batch_parameters,
        )

        return row_count_result.value == 0


# Register both event types for transition period
register_event_action("1", GenerateExpectationsEvent, GenerateExpectationsAction)
register_event_action("1", RunRdAgentEvent, GenerateExpectationsAction)
