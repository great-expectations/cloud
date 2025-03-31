from __future__ import annotations

import logging
from http import HTTPStatus
from typing import TYPE_CHECKING, Final
from urllib.parse import urljoin
from uuid import UUID

import great_expectations.expectations as gx_expectations
from great_expectations.core.http import create_session
from great_expectations.expectations.metadata_types import DataQualityIssues
from great_expectations.expectations.window import Offset, Window
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
    GenerateDataQualityCheckExpectationsEvent,
)
from great_expectations_cloud.agent.utils import (
    TriangularInterpolationOptions,
    param_safe_unique_id,
    triangular_interpolation,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import DataAsset

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class PartialGenerateDataQualityCheckExpectationError(GXAgentError):
    def __init__(self, assets_with_errors: list[str], assets_attempted: int):
        message_header = f"Unable to autogenerate expectations for {len(assets_with_errors)} of {assets_attempted} Data Assets."
        errors = ", ".join(assets_with_errors)
        message_footer = "Check your connection details, delete and recreate these Data Assets."
        message = f"{message_header}\n\u2022 {errors}\n{message_footer}"
        super().__init__(message)


class GenerateDataQualityCheckExpectationsAction(
    AgentAction[GenerateDataQualityCheckExpectationsEvent]
):
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
    def run(self, event: GenerateDataQualityCheckExpectationsEvent, id: str) -> ActionResult:
        created_resources: list[CreatedResource] = []
        assets_with_errors: list[str] = []
        for asset_name in event.data_assets:
            try:
                data_asset = self._retrieve_asset_from_asset_name(event, asset_name)

                metric_run, metric_run_id = self._get_metrics(data_asset)
                created_resources.append(
                    CreatedResource(resource_id=str(metric_run_id), type="MetricRun")
                )
                if event.selected_data_quality_issues:
                    if DataQualityIssues.VOLUME in event.selected_data_quality_issues:
                        volume_change_expectation_id = self._add_volume_change_expectation(
                            asset_id=data_asset.id
                        )
                        created_resources.append(
                            CreatedResource(
                                resource_id=str(volume_change_expectation_id), type="Expectation"
                            )
                        )

                    if DataQualityIssues.SCHEMA in event.selected_data_quality_issues:
                        schema_change_expectation_id = self._add_schema_change_expectation(
                            metric_run=metric_run, asset_id=data_asset.id
                        )
                        created_resources.append(
                            CreatedResource(
                                resource_id=str(schema_change_expectation_id), type="Expectation"
                            )
                        )

                    if DataQualityIssues.COMPLETENESS in event.selected_data_quality_issues:
                        completeness_change_expectation_ids = (
                            self._add_completeness_change_expectations(
                                metric_run=metric_run, asset_id=data_asset.id
                            )
                        )

                        for exp_id in completeness_change_expectation_ids:
                            created_resources.append(
                                CreatedResource(resource_id=str(exp_id), type="Expectation")
                            )

            except Exception as e:
                LOGGER.exception("Failed to generate expectations for %s: %s", asset_name, str(e))  # noqa: TRY401
                assets_with_errors.append(asset_name)

        if assets_with_errors:
            raise PartialGenerateDataQualityCheckExpectationError(
                assets_with_errors=assets_with_errors,
                assets_attempted=len(event.data_assets),
            )

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=created_resources,
        )

    def _retrieve_asset_from_asset_name(
        self, event: GenerateDataQualityCheckExpectationsEvent, asset_name: str
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
            metric_list=[
                MetricTypes.TABLE_COLUMNS,
                MetricTypes.TABLE_COLUMN_TYPES,
                MetricTypes.COLUMN_NULL_COUNT,
                MetricTypes.TABLE_ROW_COUNT,
            ],
        )
        metric_run_id = self._metric_repository.add_metric_run(metric_run)
        # Note: This exception is raised after the metric run is added to the repository so that
        # the user can still access any computed metrics even if one of the metrics fails.
        self._raise_on_any_metric_exception(metric_run)

        return metric_run, metric_run_id

    def _add_volume_change_expectation(self, asset_id: UUID) -> UUID:
        unique_id = param_safe_unique_id(16)
        expectation = gx_expectations.ExpectTableRowCountToBeBetween(
            windows=[
                Window(
                    constraint_fn="mean",
                    parameter_name=f"{unique_id}_min_value_min",
                    range=1,
                    offset=Offset(positive=0.0, negative=0.0),
                    strict=True,
                )
            ],
            strict_min=True,
            min_value={"$PARAMETER": f"{unique_id}_min_value_min"},
            max_value=None,
        )
        expectation_id = self._create_expectation_for_asset(
            expectation=expectation, asset_id=asset_id
        )
        return expectation_id

    def _add_schema_change_expectation(self, metric_run: MetricRun, asset_id: UUID) -> UUID:
        # Find the TABLE_COLUMNS metric by type instead of assuming it's at position 0
        table_columns_metric = next(
            (
                metric
                for metric in metric_run.metrics
                if metric.metric_name == MetricTypes.TABLE_COLUMNS
            ),
            None,
        )
        if not table_columns_metric:
            raise RuntimeError("missing TABLE_COLUMNS metric")  # noqa: TRY003

        expectation = gx_expectations.ExpectTableColumnsToMatchSet(
            column_set=table_columns_metric.value
        )
        expectation_id = self._create_expectation_for_asset(
            expectation=expectation, asset_id=asset_id
        )
        return expectation_id

    def _add_completeness_change_expectations(
        self, metric_run: MetricRun, asset_id: UUID
    ) -> list[UUID]:
        table_row_count = next(
            metric
            for metric in metric_run.metrics
            if metric.metric_name == MetricTypes.TABLE_ROW_COUNT
        )
        expectation_ids = []

        if not table_row_count:
            raise RuntimeError("missing TABLE_ROW_COUNT metric")  # noqa: TRY003

        column_null_values_metric = [
            metric
            for metric in metric_run.metrics
            if metric.metric_name == MetricTypes.COLUMN_NULL_COUNT
        ]

        if not column_null_values_metric or len(column_null_values_metric) == 0:
            raise RuntimeError("missing COLUMN_NULL_COUNT metrics")  # noqa: TRY003

        for column in column_null_values_metric:
            column_name = column.column
            null_count = column.value
            row_count = table_row_count.value
            if null_count == 0:
                expectation = gx_expectations.ExpectColumnValuesToNotBeNull(
                    column=column_name, mostly=1
                )
                expectation_id = self._create_expectation_for_asset(
                    expectation=expectation, asset_id=asset_id
                )
                expectation_ids.append(expectation_id)
            elif null_count == row_count:
                expectation = gx_expectations.ExpectColumnValuesToBeNull(
                    column=column_name, mostly=1
                )
                expectation_id = self._create_expectation_for_asset(
                    expectation=expectation, asset_id=asset_id
                )
                expectation_ids.append(expectation_id)
            else:
                # Create two separate expectations when null count is neither 0 nor 100%
                unique_id_null = param_safe_unique_id(16)
                unique_id_not_null = param_safe_unique_id(16)

                options = TriangularInterpolationOptions(
                    input_range=(0.0, float(row_count)), output_range=(0, 0.1), round_precision=5
                )
                interpolated_offset = max(
                    0.0001, round(triangular_interpolation(null_count, options), 5)
                )

                # For the null expectation (sets lower bound on nulls)
                null_expectation = gx_expectations.ExpectColumnValuesToBeNull(
                    windows=[
                        Window(
                            constraint_fn="mean",
                            parameter_name=f"{unique_id_null}_null_value_min",
                            range=5,
                            offset=Offset(
                                positive=interpolated_offset, negative=interpolated_offset
                            ),
                            strict=False,
                        )
                    ],
                    column=column_name,
                    mostly={"$PARAMETER": f"{unique_id_null}_null_value_min"},
                )

                # For the not-null expectation (sets upper bound on nulls by requiring not-nulls)
                not_null_expectation = gx_expectations.ExpectColumnValuesToNotBeNull(
                    windows=[
                        Window(
                            constraint_fn="mean",
                            parameter_name=f"{unique_id_not_null}_not_null_value_min",
                            range=5,
                            offset=Offset(
                                positive=interpolated_offset, negative=interpolated_offset
                            ),
                            strict=False,
                        )
                    ],
                    column=column_name,
                    mostly={"$PARAMETER": f"{unique_id_not_null}_not_null_value_min"},
                )

                null_expectation_id = self._create_expectation_for_asset(
                    expectation=null_expectation, asset_id=asset_id
                )
                expectation_ids.append(null_expectation_id)

                not_null_expectation_id = self._create_expectation_for_asset(
                    expectation=not_null_expectation, asset_id=asset_id
                )
                expectation_ids.append(not_null_expectation_id)

        return expectation_ids

    def _create_expectation_for_asset(
        self, expectation: gx_expectations.Expectation, asset_id: UUID
    ) -> UUID:
        url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._organization_id}/expectations/{asset_id}",
        )

        expectation_payload = expectation.configuration.to_json_dict()
        expectation_payload["autogenerated"] = True
        expectation_payload["created_via"] = "asset_creation"

        # Backend expects `expectation_type` instead of `type`:
        expectation_type = expectation_payload.pop("type")
        expectation_payload["expectation_type"] = expectation_type

        with create_session(access_token=self._auth_key) as session:
            response = session.post(url=url, json=expectation_payload)

        if response.status_code != HTTPStatus.CREATED:
            message = f"Failed to add autogenerated expectation: {expectation_type}"
            raise GXAgentError(message)
        return UUID(response.json()["data"]["id"])

    def _raise_on_any_metric_exception(self, metric_run: MetricRun) -> None:
        if any(metric.exception for metric in metric_run.metrics):
            raise RuntimeError(  # noqa: TRY003 # one off error
                "One or more metrics failed to compute."
            )


register_event_action(
    "1", GenerateDataQualityCheckExpectationsEvent, GenerateDataQualityCheckExpectationsAction
)
