from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from http import HTTPStatus
from typing import TYPE_CHECKING
from urllib.parse import urljoin
from uuid import UUID

from great_expectations.core.http import create_session
from great_expectations.metrics import BatchRowCount
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.actions.utils import ensure_openai_credentials
from great_expectations_cloud.agent.dd_metrics import ExpectAIAnalytics
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import (
    AssetReviewAgent,
)
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    GenerateExpectationsInput,
)
from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner
from great_expectations_cloud.agent.models import (
    CreatedResource,
    DomainContext,
    GenerateExpectationsEvent,
)
from great_expectations_cloud.agent.services.expectation_draft_config_service import (
    CreatedResourceTypes,
    ExpectationDraftConfigService,
)
from great_expectations_cloud.agent.services.expectation_service import (
    ExpectationService,
    ListExpectationsError,
)

if TYPE_CHECKING:
    import great_expectations.expectations as gxe
    from great_expectations.data_context import CloudDataContext


MAX_PRUNED_EXPECTATIONS = 10

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


CREATED_VIA_EXPECT_AI = "expect_ai"


class ExpectAIAgentError(Exception):
    def __init__(self, org_id: str, asset_id: str, message: str):
        super().__init__(f"Error for (org: {org_id}, asset: {asset_id}): {message}")


class GenerateExpectationsAction(AgentAction[GenerateExpectationsEvent]):
    def __init__(
        self,
        context: CloudDataContext,
        base_url: str,
        domain_context: DomainContext,
        auth_key: str,
        analytics: ExpectAIAnalytics | None = None,
    ):
        super().__init__(
            context=context, base_url=base_url, domain_context=domain_context, auth_key=auth_key
        )
        self._analytics = analytics or ExpectAIAnalytics()

    @override
    def run(self, event: GenerateExpectationsEvent, id: str) -> ActionResult:
        # Import here to avoid circular import
        from great_expectations_cloud.agent.expect_ai.tools.metrics import (  # noqa: PLC0415
            AgentToolsManager,
        )

        ensure_openai_credentials()

        metric_service = MetricService(context=self._context)
        tools_manager = AgentToolsManager(
            context=self._context,
            metric_service=metric_service,
        )
        query_runner = QueryRunner(context=self._context)
        agent = AssetReviewAgent(
            tools_manager=tools_manager,
            query_runner=query_runner,
            metric_service=metric_service,
            analytics=self._analytics,
        )
        expectation_service = ExpectationService(context=self._context)

        # Do not proceed with generating Expectations if the Data Asset is empty
        if self._batch_contains_no_rows(event):
            error_message = "Could not generate Expectations because the Data Asset has no records. Ensure the table or view connected to your Data Asset has records and try again."
            raise RuntimeError(error_message)

        try:
            existing_expectation_contexts = (
                expectation_service.get_existing_expectations_by_data_asset(
                    data_source_name=event.datasource_name,
                    data_asset_name=event.data_asset_name,
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

        generate_expectations_input = GenerateExpectationsInput(
            organization_id=str(self._domain_context.organization_id),
            workspace_id=str(event.workspace_id),
            data_source_name=event.datasource_name,
            data_asset_name=event.data_asset_name,
            batch_definition_name=event.batch_definition_name,
            batch_parameters=event.batch_parameters,
            existing_expectation_contexts=existing_expectation_contexts,
        )
        asset_review_result = asyncio.run(
            agent.arun(generate_expectations_input=generate_expectations_input)
        )

        expectation_pruner = ExpectationPruner(max_expectations=MAX_PRUNED_EXPECTATIONS)
        expectations = asset_review_result.expectation_suite.expectations
        if len(asset_review_result.metrics.column_names) > 0:
            expectations = expectation_pruner.prune_invalid_columns(
                expectations=expectations,
                valid_columns=asset_review_result.metrics.column_names,
            )
        expectations = expectation_pruner.prune_expectations(expectations)

        return self._create_expectation_draft_configs(id=id, event=event, expectations=expectations)

    def _batch_contains_no_rows(self, event: GenerateExpectationsEvent) -> bool:
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

    def _create_expectation_draft_configs(
        self, id: str, event: GenerateExpectationsEvent, expectations: list[gxe.Expectation]
    ) -> ActionResult:
        draft_config_service = ExpectationDraftConfigService(
            context=self._context,
            created_via=CREATED_VIA_EXPECT_AI,
        )

        created_resources = draft_config_service.create_expectation_draft_configs(
            data_source_name=event.datasource_name,
            data_asset_name=event.data_asset_name,
            expectations=expectations,
            event_id=id,
        )

        return ActionResult(id=id, type=event.type, created_resources=created_resources)

    def _create_gx_managed_expectations(
        self, id: str, event: GenerateExpectationsEvent, expectations: list[gxe.Expectation]
    ) -> ActionResult:
        data_source = self._context.data_sources.get(event.datasource_name)
        asset = data_source.get_asset(event.data_asset_name)

        expectation_ids = self._add_expectations_to_managed_asset(
            asset_id=str(asset.id),
            expectations=expectations,
        )
        created_resources = [
            CreatedResource(resource_id=str(expectation_id), type=CreatedResourceTypes.EXPECTATION)
            for expectation_id in expectation_ids
        ]
        return ActionResult(
            id=id,
            type=event.type,
            created_resources=created_resources,
        )

    def _add_expectations_to_managed_asset(
        self, asset_id: str, expectations: list[gxe.Expectation]
    ) -> list[UUID]:
        url = urljoin(
            base=self._base_url,
            url=self._url_path(asset_id=asset_id),
        )

        payload = []
        for expectation in expectations:
            expectation_payload = expectation.configuration.to_json_dict()
            expectation_payload["autogenerated"] = True
            expectation_payload["created_via"] = CREATED_VIA_EXPECT_AI

            # Backend expects `expectation_type` instead of `type`:
            expectation_payload["expectation_type"] = expectation_payload.pop("type")
            payload.append(expectation_payload)

        with create_session(access_token=self._auth_key) as session:
            response = session.post(url=url, json=payload)
            if response.status_code != HTTPStatus.CREATED:
                raise ExpectAIAgentError(
                    org_id=str(self._domain_context.organization_id),
                    asset_id=asset_id,
                    message=f"Add expectations failed with http status code {response.status_code}",
                )
        return [UUID(data["id"]) for data in response.json()["data"]]

    def _url_path(self, asset_id: str) -> str:
        return f"/api/v1/organizations/{self._domain_context.organization_id!s}/workspaces/{self._domain_context.workspace_id}/expectations/{asset_id}"


register_event_action("1", GenerateExpectationsEvent, GenerateExpectationsAction)


class ExpectationPruner:
    """Expectation list pruner.

    NOTE: This should likely be treated as temporary.
    The goal is to limit the number of expectations.
    We will likely push this type of reasoning into echos in the future.
    """

    def __init__(self, max_expectations: int = 10) -> None:
        self._max_expectations = max_expectations

    def prune_expectations(self, expectations: list[gxe.Expectation]) -> list[gxe.Expectation]:
        """Apply a few heuristics to cut down on the number of expectations.

        The idea here is we go through multiple rounds of filtering, resulting in a smaller list.
        """
        # only one per type per col
        expectations = self._limit_per_type_and_column(expectations)

        # limit the number per column to 2
        expectations = self._limit_per_column(expectations, max_for_col=2)

        # limit the number per column to 1 if we still have a lot
        if len(expectations) > self._max_expectations:
            expectations = self._limit_per_column(expectations, max_for_col=1)

        # cap the number of expectations
        return expectations[: self._max_expectations]

    def _limit_per_column(
        self,
        expectations: list[gxe.Expectation],
        max_for_col: int = 1,
    ) -> list[gxe.Expectation]:
        expectations_for_col_so_far = defaultdict[str, int](int)
        output: list[gxe.Expectation] = []
        for exp in expectations:
            if column := getattr(exp, "column", None):
                if expectations_for_col_so_far[column] < max_for_col:
                    output += [exp]
                    expectations_for_col_so_far[column] += 1
            else:
                output += [exp]
        return output

    def _limit_per_type_and_column(
        self,
        expectations: list[gxe.Expectation],
    ) -> list[gxe.Expectation]:
        so_far = defaultdict[tuple[str, str], int](int)
        output: list[gxe.Expectation] = []
        for exp in expectations:
            if column := getattr(exp, "column", None):
                key = (exp.expectation_type, column)
                if so_far[key] < 1:
                    output += [exp]
                    so_far[key] += 1
            else:
                output += [exp]
        return output

    def prune_invalid_columns(
        self,
        expectations: list[gxe.Expectation],
        valid_columns: set[str],
    ) -> list[gxe.Expectation]:
        """Prune expectations that reference columns not in the provided set.

        Args:
            expectations: List of expectations to filter
            valid_columns: Set of valid column names. Expectations referencing
                columns not in this set will be removed.

        Returns:
            Filtered list of expectations that only reference valid columns.
        """
        output: list[gxe.Expectation] = []
        for exp in expectations:
            # Check for single column attribute
            if column := getattr(exp, "column", None):
                if column not in valid_columns:
                    continue

            # Check for column_list attribute
            if column_list := getattr(exp, "column_list", None):
                if any(col not in valid_columns for col in column_list):
                    continue

            # Keep expectations that don't have column/column_list or have all valid columns
            output.append(exp)
        return output
