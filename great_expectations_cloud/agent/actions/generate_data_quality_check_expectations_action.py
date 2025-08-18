from __future__ import annotations

import logging
from collections.abc import Sequence
from enum import Enum
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urljoin
from uuid import UUID

import great_expectations.expectations as gx_expectations
from great_expectations.core.http import create_session
from great_expectations.exceptions import GXCloudError, InvalidExpectationConfigurationError
from great_expectations.expectations.metadata_types import (
    DataQualityIssues,
)
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
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    MetricRun,
    MetricTypes,
)
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
    from great_expectations.core.suite_parameters import (
        SuiteParameterDict,
    )
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import DataAsset

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class ExpectationConstraintFunction(str, Enum):
    """Expectation constraint functions."""

    FORECAST = "forecast"
    MEAN = "mean"


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
        selected_dqis: Sequence[DataQualityIssues] = event.selected_data_quality_issues or []
        created_via: str | None = event.created_via or None
        for asset_name in event.data_assets:
            try:
                data_asset = self._retrieve_asset_from_asset_name(event, asset_name)

                metric_run, metric_run_id = self._get_metrics(data_asset)
                created_resources.append(
                    CreatedResource(resource_id=str(metric_run_id), type="MetricRun")
                )

                if selected_dqis:
                    pre_existing_anomaly_detection_coverage = (
                        self._get_current_anomaly_detection_coverage(data_asset.id)
                    )

                    if self._should_add_volume_change_detection_coverage(
                        selected_data_quality_issues=selected_dqis,
                        pre_existing_anomaly_detection_coverage=pre_existing_anomaly_detection_coverage,
                    ):
                        volume_change_expectation_id = self._add_volume_change_expectation(
                            asset_id=data_asset.id,
                            use_forecast=event.use_forecast,
                            created_via=created_via,
                        )
                        created_resources.append(
                            CreatedResource(
                                resource_id=str(volume_change_expectation_id), type="Expectation"
                            )
                        )

                    if self._should_add_schema_change_detection_coverage(
                        selected_data_quality_issues=selected_dqis,
                        pre_existing_anomaly_detection_coverage=pre_existing_anomaly_detection_coverage,
                    ):
                        schema_change_expectation_id = self._add_schema_change_expectation(
                            metric_run=metric_run, asset_id=data_asset.id, created_via=created_via
                        )
                        created_resources.append(
                            CreatedResource(
                                resource_id=str(schema_change_expectation_id), type="Expectation"
                            )
                        )

                    if DataQualityIssues.COMPLETENESS in selected_dqis:
                        pre_existing_completeness_change_expectations = (
                            pre_existing_anomaly_detection_coverage.get(
                                DataQualityIssues.COMPLETENESS, []
                            )
                        )
                        completeness_change_expectation_ids = self._add_completeness_change_expectations(
                            metric_run=metric_run,
                            asset_id=data_asset.id,
                            pre_existing_completeness_change_expectations=pre_existing_completeness_change_expectations,
                            created_via=created_via,
                            use_forecast=event.use_forecast,
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
    ) -> DataAsset[Any, Any]:
        try:
            datasource = self._context.data_sources.get(event.datasource_name)
            data_asset = datasource.get_asset(asset_name)
            data_asset.test_connection()  # raises `TestConnectionError` on failure

        except Exception as e:
            # TODO - see if this can be made more specific
            raise RuntimeError(f"Failed to retrieve asset: {e}") from e  # noqa: TRY003 # want to keep this informative for now

        return data_asset  # type: ignore[no-any-return]  # unable to narrow types strictly based on names

    def _get_metrics(self, data_asset: DataAsset[Any, Any]) -> tuple[MetricRun, UUID]:
        batch_request = data_asset.build_batch_request()
        if data_asset.id is None:
            raise RuntimeError("DataAsset.id is None")  # noqa: TRY003
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

    def _get_current_anomaly_detection_coverage(
        self, data_asset_id: UUID | None
    ) -> dict[DataQualityIssues, list[dict[Any, Any]]]:
        """
        This function returns a dict mapping Data Quality Issues to a list of ExpectationConfiguration dicts.
        """
        url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._organization_id}/expectations/",
        )
        with create_session(access_token=self._auth_key) as session:
            response = session.get(
                url=url,
                params={"anomaly_detection": str(True), "data_asset_id": str(data_asset_id)},
            )

        if not response.ok:
            raise GXCloudError(
                message=f"GenerateDataQualityCheckExpectationsAction encountered an error while connecting to GX Cloud. "
                f"Unable to retrieve Anomaly Detection Expectations for Asset with ID={data_asset_id}.",
                response=response,
            )
        data = response.json()
        try:
            return data["data"]  # type: ignore[no-any-return]

        except KeyError as e:
            raise GXCloudError(
                message="Malformed response received from GX Cloud",
                response=response,
            ) from e

    def _should_add_volume_change_detection_coverage(
        self,
        selected_data_quality_issues: Sequence[DataQualityIssues],
        pre_existing_anomaly_detection_coverage: dict[
            DataQualityIssues, list[dict[Any, Any]]
        ],  # list of ExpectationConfiguration dicts
    ) -> bool:
        return (
            DataQualityIssues.VOLUME in selected_data_quality_issues
            and len(pre_existing_anomaly_detection_coverage.get(DataQualityIssues.VOLUME, [])) == 0
        )

    def _should_add_schema_change_detection_coverage(
        self,
        selected_data_quality_issues: Sequence[DataQualityIssues],
        pre_existing_anomaly_detection_coverage: dict[
            DataQualityIssues, list[dict[Any, Any]]
        ],  # list of ExpectationConfiguration dicts
    ) -> bool:
        return (
            DataQualityIssues.SCHEMA in selected_data_quality_issues
            and len(pre_existing_anomaly_detection_coverage.get(DataQualityIssues.SCHEMA, [])) == 0
        )

    def _add_volume_change_expectation(
        self, asset_id: UUID | None, use_forecast: bool, created_via: str | None
    ) -> UUID:
        unique_id = param_safe_unique_id(16)
        lower_bound_param_name = f"{unique_id}_min_value_min"
        upper_bound_param_name = f"{unique_id}_max_value_max"
        min_value: SuiteParameterDict[str, str] | None = {"$PARAMETER": lower_bound_param_name}
        max_value: SuiteParameterDict[str, str] | None = {"$PARAMETER": upper_bound_param_name}
        windows = []
        strict_min = False
        strict_max = False

        if use_forecast:
            windows += [
                Window(
                    constraint_fn=ExpectationConstraintFunction.FORECAST,
                    parameter_name=lower_bound_param_name,
                    range=1,
                    offset=Offset(positive=0.0, negative=0.0),
                    strict=True,
                ),
                Window(
                    constraint_fn=ExpectationConstraintFunction.FORECAST,
                    parameter_name=upper_bound_param_name,
                    range=1,
                    offset=Offset(positive=0.0, negative=0.0),
                    strict=True,
                ),
            ]
        else:
            windows += [
                Window(
                    constraint_fn=ExpectationConstraintFunction.MEAN,
                    parameter_name=lower_bound_param_name,
                    range=1,
                    offset=Offset(positive=0.0, negative=0.0),
                    strict=True,
                )
            ]
            max_value = None
            strict_min = True

        expectation = gx_expectations.ExpectTableRowCountToBeBetween(
            windows=windows,
            strict_min=strict_min,
            strict_max=strict_max,
            min_value=min_value,
            max_value=max_value,
        )
        expectation_id = self._create_expectation_for_asset(
            expectation=expectation, asset_id=asset_id, created_via=created_via
        )
        return expectation_id

    def _add_schema_change_expectation(
        self, metric_run: MetricRun, asset_id: UUID | None, created_via: str | None
    ) -> UUID:
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
            expectation=expectation, asset_id=asset_id, created_via=created_via
        )
        return expectation_id

    def _add_completeness_change_expectations(
        self,
        metric_run: MetricRun,
        asset_id: UUID | None,
        pre_existing_completeness_change_expectations: list[
            dict[Any, Any]
        ],  # list of ExpectationConfiguration dicts
        created_via: str | None,
        use_forecast: bool = False,
    ) -> list[UUID]:
        table_row_count = next(
            metric
            for metric in metric_run.metrics
            if metric.metric_name == MetricTypes.TABLE_ROW_COUNT
        )

        if not table_row_count:
            raise RuntimeError("missing TABLE_ROW_COUNT metric")  # noqa: TRY003

        column_null_values_metric: list[ColumnMetric[int]] = [
            metric
            for metric in metric_run.metrics
            if isinstance(metric, ColumnMetric)
            and metric.metric_name == MetricTypes.COLUMN_NULL_COUNT
        ]

        if not column_null_values_metric or len(column_null_values_metric) == 0:
            raise RuntimeError("missing COLUMN_NULL_COUNT metrics")  # noqa: TRY003

        expectation_ids = []
        # Single-expectation approach using ExpectColumnProportionOfNonNullValuesToBeBetween
        # Expectations are only added to columns that do not have coverage
        columns_missing_completeness_coverage = self._get_columns_missing_completeness_coverage(
            column_null_values_metric=column_null_values_metric,
            pre_existing_completeness_change_expectations=pre_existing_completeness_change_expectations,
        )
        for column in columns_missing_completeness_coverage:
            column_name = column.column
            null_count = column.value
            row_count = table_row_count.value
            expectation: gx_expectations.Expectation

            # Single-expectation approach using ExpectColumnProportionOfNonNullValuesToBeBetween
            unique_id = param_safe_unique_id(16)
            min_param_name = f"{unique_id}_proportion_min"
            max_param_name = f"{unique_id}_proportion_max"

            # Calculate non-null proportion
            non_null_count = row_count - null_count if row_count > 0 else 0
            non_null_proportion = non_null_count / row_count if row_count > 0 else 0

            if use_forecast:
                expectation = gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
                    windows=[
                        Window(
                            constraint_fn=ExpectationConstraintFunction.FORECAST,
                            parameter_name=min_param_name,
                            range=1,
                            offset=Offset(positive=0.0, negative=0.0),
                            strict=True,
                        ),
                        Window(
                            constraint_fn=ExpectationConstraintFunction.FORECAST,
                            parameter_name=max_param_name,
                            range=1,
                            offset=Offset(positive=0.0, negative=0.0),
                            strict=True,
                        ),
                    ],
                    column=column_name,
                    min_value={"$PARAMETER": min_param_name},
                    max_value={"$PARAMETER": max_param_name},
                )
            elif non_null_proportion == 0:
                expectation = gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
                    column=column_name,
                    max_value=0,
                )
            elif non_null_proportion == 1:
                expectation = gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
                    column=column_name,
                    min_value=1,
                )
            else:
                # Use triangular interpolation to compute min/max values
                interpolated_offset = self._compute_triangular_interpolation_offset(
                    value=non_null_proportion, input_range=(0.0, 1.0)
                )

                expectation = gx_expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
                    windows=[
                        Window(
                            constraint_fn=ExpectationConstraintFunction.MEAN,
                            parameter_name=min_param_name,
                            range=5,
                            offset=Offset(
                                positive=interpolated_offset, negative=interpolated_offset
                            ),
                            strict=False,
                        ),
                        Window(
                            constraint_fn=ExpectationConstraintFunction.MEAN,
                            parameter_name=max_param_name,
                            range=5,
                            offset=Offset(
                                positive=interpolated_offset, negative=interpolated_offset
                            ),
                            strict=False,
                        ),
                    ],
                    column=column_name,
                    min_value={"$PARAMETER": min_param_name},
                    max_value={"$PARAMETER": max_param_name},
                )

            expectation_id = self._create_expectation_for_asset(
                expectation=expectation, asset_id=asset_id, created_via=created_via
            )
            expectation_ids.append(expectation_id)

        return expectation_ids

    def _get_columns_missing_completeness_coverage(
        self,
        column_null_values_metric: list[ColumnMetric[int]],
        pre_existing_completeness_change_expectations: list[
            dict[Any, Any]
        ],  # list of ExpectationConfiguration dicts
    ) -> list[ColumnMetric[int]]:
        try:
            columns_with_completeness_coverage = {
                expectation.get("config").get("kwargs").get("column")  # type: ignore[union-attr]
                for expectation in pre_existing_completeness_change_expectations
            }
        except AttributeError as e:
            raise InvalidExpectationConfigurationError(str(e)) from e
        columns_without_completeness_coverage = [
            column
            for column in column_null_values_metric
            if column.column not in columns_with_completeness_coverage
        ]
        return columns_without_completeness_coverage

    def _compute_triangular_interpolation_offset(
        self, value: float, input_range: tuple[float, float]
    ) -> float:
        """
        Compute triangular interpolation offset for expectation windows.

        Args:
            value: The input value to interpolate
            input_range: The input range as (min, max) tuple

        Returns:
            The computed interpolation offset
        """
        options = TriangularInterpolationOptions(
            input_range=input_range,
            output_range=(0, 0.1),
            round_precision=5,
        )
        return max(0.0001, round(triangular_interpolation(value, options), 5))

    def _create_expectation_for_asset(
        self,
        expectation: gx_expectations.Expectation,
        asset_id: UUID | None,
        created_via: str | None,
    ) -> UUID:
        url = urljoin(
            base=self._base_url,
            url=f"/api/v1/organizations/{self._organization_id}/expectations/{asset_id}",
        )

        expectation_payload = expectation.configuration.to_json_dict()
        expectation_payload["autogenerated"] = True
        if created_via is not None:
            expectation_payload["created_via"] = created_via

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
