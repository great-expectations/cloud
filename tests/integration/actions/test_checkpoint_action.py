from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

import great_expectations.exceptions as gx_exceptions
import pytest

from great_expectations_cloud.agent.models import (
    RunCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset


from great_expectations.core.http import create_session

from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction


@pytest.fixture(scope="module")
def get_missing_checkpoint_error_type():
    return gx_exceptions.DataContextError


@pytest.fixture
def datasource_names_to_asset_names(datasource, data_asset):
    return {datasource.name: {data_asset.name}}


@pytest.fixture(scope="module")
def checkpoint(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
    get_missing_checkpoint_error_type: type[Exception],
) -> Iterator[Checkpoint]:
    checkpoint_name = f"{data_asset.datasource.name} | {data_asset.name}"
    _ = context.add_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "expectation_suite_name": expectation_suite.expectation_suite_name,
                "batch_request": batch_request,
            },
            {
                "expectation_suite_name": expectation_suite.expectation_suite_name,
                "batch_request": batch_request,
            },
        ],
    )
    _ = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "expectation_suite_name": expectation_suite.expectation_suite_name,
                "batch_request": batch_request,
            }
        ],
    )
    checkpoint = context.get_checkpoint(name=checkpoint_name)
    assert (
        len(checkpoint.validations) == 1
    ), "Checkpoint was not updated in the previous method call."
    yield checkpoint
    # PP-691: this is a bug
    # you should only have to pass name
    context.delete_checkpoint(
        # name=checkpoint_name,
        id=checkpoint.ge_cloud_id,
    )
    with pytest.raises(get_missing_checkpoint_error_type):
        context.get_checkpoint(name=checkpoint_name)


@pytest.fixture
def checkpoint_event(checkpoint, datasource_names_to_asset_names):
    return RunCheckpointEvent(
        type="run_checkpoint_request",
        checkpoint_id=checkpoint.ge_cloud_id,
        datasource_names_to_asset_names=datasource_names_to_asset_names,
    )


def test_running_checkpoint_action(
    context, checkpoint_event, cloud_base_url: str, org_id: str, token: str
):
    action = RunCheckpointAction(context=context)
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    action_result = action.run(event=checkpoint_event, id=event_id)
    assert action_result.type == checkpoint_event.type
    assert action_result.id == event_id

    # Also get the resulting validation result
    validation_result_id = action_result.created_resources[0].resource_id

    resource_url = (
        f"{cloud_base_url}/organizations/" f"{org_id}/validation-results/{validation_result_id}"
    )
    session = create_session(access_token=token)
    response = session.get(resource_url)
    data = response.json()

    validation_result = data["data"]["attributes"]["validation_result"]
    assert validation_result["success"]
