from __future__ import annotations

import logging
from itertools import islice
from typing import TYPE_CHECKING, Final
from uuid import UUID

import great_expectations.expectations as gx_expectations
from great_expectations.exceptions import DataContextError
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.cloud_data_store import (
    CloudDataStore,
)
from great_expectations.experimental.metric_repository.metric_list_metric_retriever import (
    MetricListMetricRetriever,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from great_expectations.experimental.metric_repository.metrics import MetricRun, MetricTypes
from typing_extensions import override

from great_expectations_cloud.agent.actions import ActionResult, AgentAction
from great_expectations_cloud.agent.event_handler import register_event_action
from great_expectations_cloud.agent.exceptions import GXAgentError
from great_expectations_cloud.agent.models import (
    CreatedResource,
    GenerateSchemaChangeExpectationsEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import DataAsset

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class PartialSchemaChangeExpectationError(GXAgentError):
    def __init__(self, asset_to_error_map: dict[str, Exception], assets_attempted: int):
        self.MAX_ERRORS_TO_DISPLAY = 5
        self.asset_to_error_map = asset_to_error_map
        num_assets_with_errors = len(self.asset_to_error_map)
        self.message = f"Failed to generate schema change expectations for {num_assets_with_errors} of the {assets_attempted} assets."
        super().__init__(self._build_error_message())

    def _build_error_message(self) -> str:
        if len(self.asset_to_error_map) > self.MAX_ERRORS_TO_DISPLAY:
            # Get the first MAX_ERRORS_TO_DISPLAY errors to display:
            errors_to_display = dict(
                islice(self.asset_to_error_map.items(), self.MAX_ERRORS_TO_DISPLAY)
            )
            num_errors_not_displayed = len(self.asset_to_error_map) - self.MAX_ERRORS_TO_DISPLAY

            num_error_display_msg = f"Only displaying the first {self.MAX_ERRORS_TO_DISPLAY} errors. There {'is' if num_errors_not_displayed == 1 else 'are'} {num_errors_not_displayed} additional error{'' if num_errors_not_displayed == 1 else 's'}. "
        else:
            errors_to_display = self.asset_to_error_map
            num_error_display_msg = ""
        errors = " ----- \n".join(
            f"Asset: {asset_name} Error: {error}" for asset_name, error in errors_to_display.items()
        )
        return f"{self.message}\n{num_error_display_msg}Errors:\n----- {errors}"


class GenerateSchemaChangeExpectationsAction(AgentAction[GenerateSchemaChangeExpectationsEvent]):
    def __init__(  # noqa: PLR0913  # Refactor opportunity
        self,
        context: CloudDataContext,
        base_url: str,
        organization_id: UUID,
        auth_key: str,
        metric_repository: MetricRepository | None = None,
        batch_inspector: BatchInspector | None = None,
    ):
        super().__init__(
            context=context, base_url=base_url, organization_id=organization_id, auth_key=auth_key
        )
        self._metric_repository = metric_repository or MetricRepository(
            data_store=CloudDataStore(self._context)
        )
        self._batch_inspector = batch_inspector or BatchInspector(
            context, [MetricListMetricRetriever(self._context)]
        )

    @override
    def run(self, event: GenerateSchemaChangeExpectationsEvent, id: str) -> ActionResult:
        created_resources: list[CreatedResource] = []
        asset_to_error_map: dict[str, Exception] = {}
        for asset_name in event.data_assets:
            try:
                data_asset = self._retrieve_asset_from_asset_name(event, asset_name)
                metric_run, metric_run_id = self._get_metrics(data_asset)
                # this logic will change with ZELDA-1154 and we will be pulling this in from a mapping
                expectation_suite_name = f"{data_asset.name} Suite"
                expectation = self._add_schema_change_expectation(
                    metric_run, expectation_suite_name
                )
                created_resources.append(
                    CreatedResource(resource_id=str(metric_run_id), type="MetricRun")
                )
                created_resources.append(
                    CreatedResource(resource_id=expectation.id, type="Expectation")
                )
            except Exception as e:
                asset_to_error_map[asset_name] = e

        if asset_to_error_map:
            raise PartialSchemaChangeExpectationError(
                asset_to_error_map=asset_to_error_map,
                assets_attempted=len(event.data_assets),
            )

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=created_resources,
        )

    def _retrieve_asset_from_asset_name(
        self, event: GenerateSchemaChangeExpectationsEvent, asset_name: str
    ) -> DataAsset:
        try:
            datasource = self._context.data_sources.get(event.datasource_name)
            data_asset = datasource.get_asset(asset_name)
            data_asset.test_connection()  # raises `TestConnectionError` on failure

        except Exception as e:
            # TODO - see if this can be made more specific
            raise RuntimeError(f"Failed to retrieve asset: {e}") from e  # noqa: TRY003 # want to keep this informative for now

        return data_asset

    def _get_metrics(self, data_asset: DataAsset) -> tuple[MetricRun, UUID]:
        batch_request = data_asset.build_batch_request()
        metric_run = self._batch_inspector.compute_metric_list_run(
            data_asset_id=data_asset.id,
            batch_request=batch_request,
            metric_list=[MetricTypes.TABLE_COLUMNS, MetricTypes.TABLE_COLUMN_TYPES],
        )
        metric_run_id = self._metric_repository.add_metric_run(metric_run)
        # Note: This exception is raised after the metric run is added to the repository so that
        # the user can still access any computed metrics even if one of the metrics fails.
        self._raise_on_any_metric_exception(metric_run)

        return metric_run, metric_run_id

    def _add_schema_change_expectation(
        self, metric_run: MetricRun, expectation_suite_name: str
    ) -> gx_expectations.Expectation:
        try:
            expectation_suite = self._context.suites.get(name=expectation_suite_name)
        except DataContextError as e:
            raise RuntimeError(  # noqa: TRY003 # want to keep this informative for now
                f"Expectation Suite with name {expectation_suite_name} was not found."
            ) from e

        try:
            expectation = expectation_suite.add_expectation(
                expectation=gx_expectations.ExpectTableColumnsToMatchSet(
                    column_set=metric_run.metrics[0].value
                )
            )
            expectation_suite.save()
        except Exception as e:
            raise RuntimeError(f"Failed to add expectation to suite: {e}") from e  # noqa: TRY003 # want to keep this informative for now
        return expectation

    def _raise_on_any_metric_exception(self, metric_run: MetricRun) -> None:
        if any(metric.exception for metric in metric_run.metrics):
            raise RuntimeError(  # noqa: TRY003 # one off error
                "One or more metrics failed to compute."
            )


register_event_action(
    "1", GenerateSchemaChangeExpectationsEvent, GenerateSchemaChangeExpectationsAction
)
