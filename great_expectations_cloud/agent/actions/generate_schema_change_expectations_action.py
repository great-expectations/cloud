from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Final
from urllib.parse import urljoin
from uuid import UUID

import great_expectations.expectations as gx_expectations
from great_expectations.core.http import create_session
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
    RunCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import DataAsset

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class PartialSchemaChangeExpectationError(GXAgentError):
    def __init__(self, assets_with_errors: list[str], assets_attempted: int):
        message_header = f"Unable to fetch schemas for {len(assets_with_errors)} of {assets_attempted} Data Assets."
        errors = ", ".join(assets_with_errors)
        message_footer = "Check your connection details, delete and recreate these Data Assets."
        message = f"{message_header}\n\u2022 {errors}\n{message_footer}"
        super().__init__(message)


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
        self._organization_id = organization_id

    @override
    def run(self, event: GenerateSchemaChangeExpectationsEvent, id: str) -> ActionResult:
        created_resources: list[CreatedResource] = []
        assets_with_errors: list[str] = []
        # assets = event.data_assets
        assets = ["some_asset"]
        for asset_name in assets:
            try:
                # data_asset = self._retrieve_asset_from_asset_name(event, asset_name)
                # data_asset_id = data_asset.id
                # metric_run, metric_run_id = self._get_metrics(data_asset)
                # expectation_suite_name = event.data_asset_to_expectation_suite_name[asset_name]
                data_asset_id = UUID("bb117d82-3e2d-497c-84fe-627df5f39b52")
                expectation = self._add_schema_change_expectation(
                    # metric_run, expectation_suite_name, data_asset_id=data_asset_id
                    data_asset_id=data_asset_id
                )
                # created_resources.append(
                #     CreatedResource(resource_id=str(metric_run_id), type="MetricRun")
                # )
                created_resources.append(
                    CreatedResource(resource_id=expectation.id, type="Expectation")
                )
            except Exception:
                assets_with_errors.append(asset_name)

        if assets_with_errors:
            raise PartialSchemaChangeExpectationError(
                assets_with_errors=assets_with_errors,
                assets_attempted=len(assets),
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
        self,
        # metric_run: MetricRun, expectation_suite_name: str,
        data_asset_id: UUID,
    ) -> gx_expectations.Expectation:
        # TODO: use mercury API to add expectation to suite
        # Should we use the GraphQL API to add the expectation to the suite?
        # Or the REST API like Core which fetches the suite and adds the expectation?
        pass

        # REST
        #
        # # Get the suite - I think we can do this with core.
        # try:
        #     expectation_suite = self._context.suites.get(name=expectation_suite_name)
        # except DataContextError as e:
        #     raise RuntimeError(  # want to keep this informative for now
        #         f"Expectation Suite with name {expectation_suite_name} was not found."
        #     ) from e
        # # Add the expectation
        # try:
        #     expectation = expectation_suite.add_expectation(
        #         expectation=gx_expectations.ExpectTableColumnsToMatchSet(
        #             column_set=metric_run.metrics[0].value
        #         )
        #     )
        #     # Do not save the suite
        #     # expectation_suite.save()
        # except Exception as e:
        #     raise RuntimeError(f"Failed to add expectation to suite: {e}") from e  # want to keep this informative for now
        # return expectation

        # Update the suite using the REST endpoint

        # Graphql

        # column_set=metric_run.metrics[0].value
        column_set = ["created_at", "updated_at"]

        expectation = gx_expectations.ExpectTableColumnsToMatchSet(column_set=column_set)
        # TODO: How to future proof for pydantic v2 instead of using deprecateed .dict()? Can we check pydantic version?
        # breakpoint()
        # if pydantic.__version__ < "2.0.0":
        #     expectation_dict = expectation.dict()
        # else:
        #     expectation_dict = expectation.model_dump()
        expectation_dict = expectation.dict()
        expectation_dict["autogenerated"] = True
        expectation_json = json.dumps(expectation_dict)

        # Example:
        # (Pdb) expectation_dict
        # {'id': None, 'meta': None, 'notes': None, 'result_format': <ResultFormat.BASIC: 'BASIC'>, 'description': None, 'catch_exceptions': False, 'rendered_content': None, 'windows': None, 'batch_id': None, 'column_set': ['created_at', 'updated_at'], 'exact_match': None, 'autogenerated': True}

        # TODO: What is the right query here?
        # add_expectation_to_asset_mutation = f"""
        #         mutation {{
        #             addExpectationToDataAsset(input: {{
        #                 dataAssetId: "{data_asset_id}",
        #                 expectationConfiguration: {expectation_json}
        #             }}) {{
        #                 geCloudId
        #                 expectationType
        #                 kwargs
        #             }}
        #         }}
        #     """
        add_expectation_to_asset_mutation = """
            mutation addExpectationToAsset($input: AddExpectationToDataAssetInput!) {
                addExpectationToDataAsset(input: $input) {
                    expectation {
                        geCloudId
                        gxManaged
                    }
                }
            }
        """

        variables = {
            "input": {
                "dataAssetId": str(data_asset_id),
                "expectationConfiguration": expectation_json,
            }
        }

        payload = {
            "query": add_expectation_to_asset_mutation,
            "variables": variables,
        }
        url = urljoin(
            base=self._base_url,
            url=f"/organizations/{self._organization_id}/graphql",
        )
        headers = {"Content-Type": "application/json"}
        with create_session(access_token=self._auth_key) as session:
            res = session.post(url=url, data=payload, headers=headers)

        res.raise_for_status()

        expectation.id = res.json()["data"]["addExpectationToDataAsset"]["geCloudId"]

        return expectation

        # Original w Core
        # try:
        #     expectation_suite = self._context.suites.get(name=expectation_suite_name)
        # except DataContextError as e:
        #     raise RuntimeError(  # want to keep this informative for now
        #         f"Expectation Suite with name {expectation_suite_name} was not found."
        #     ) from e
        #
        # try:
        #     expectation = expectation_suite.add_expectation(
        #         expectation=gx_expectations.ExpectTableColumnsToMatchSet(
        #             column_set=metric_run.metrics[0].value
        #         )
        #     )
        #     expectation_suite.save()
        # except Exception as e:
        #     raise RuntimeError(f"Failed to add expectation to suite: {e}") from e  # want to keep this informative for now
        # return expectation

    def _raise_on_any_metric_exception(self, metric_run: MetricRun) -> None:
        if any(metric.exception for metric in metric_run.metrics):
            raise RuntimeError(  # noqa: TRY003 # one off error
                "One or more metrics failed to compute."
            )


# TODO: Revert this change, only for testing:
register_event_action("1", RunCheckpointEvent, GenerateSchemaChangeExpectationsAction)
