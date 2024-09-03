from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Iterator

import great_expectations.exceptions as gx_exceptions
import pandas as pd
import pytest
from great_expectations.core import ExpectationConfiguration

from great_expectations_cloud.agent.models import (
    RunScheduledCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import BatchRequest, PandasDatasource
    from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset


from great_expectations.core.http import create_session

from great_expectations_cloud.agent.actions.run_scheduled_checkpoint import (
    RunScheduledCheckpointAction,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def pandas_test_df() -> pd.DataFrame:
    d = {
        "string": ["a", "b", "c"],
        "datetime": [
            pd.to_datetime("2020-01-01"),
            pd.to_datetime("2020-01-02"),
            pd.to_datetime("2020-01-03"),
        ],
    }
    df = pd.DataFrame(data=d)
    return df


@pytest.fixture(scope="module")
def get_missing_datasource_error_type() -> type[Exception]:
    return ValueError


@pytest.fixture(scope="module")
def get_missing_data_asset_error_type() -> type[Exception]:
    return LookupError


@pytest.fixture(scope="module")
def in_memory_batch_request_missing_dataframe_error_type() -> type[Exception]:
    return ValueError


@pytest.fixture(scope="module")
def get_missing_expectation_suite_error_type():
    return gx_exceptions.DataContextError


@pytest.fixture(scope="module")
def get_missing_checkpoint_error_type():
    return gx_exceptions.DataContextError


@pytest.fixture(scope="module")
def datasource(
    context: CloudDataContext,
    get_missing_datasource_error_type: type[Exception],
) -> Iterator[PandasDatasource]:
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource = context.sources.add_pandas(
        name=datasource_name,
    )
    assert datasource.name == datasource_name
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource.name = datasource_name
    datasource = context.sources.add_or_update_pandas(
        datasource=datasource,
    )
    assert (
        datasource.name == datasource_name
    ), "The datasource was not updated in the previous method call."
    yield datasource
    context.delete_datasource(datasource_name=datasource_name)
    with pytest.raises(get_missing_datasource_error_type):
        context.get_datasource(datasource_name=datasource_name)


@pytest.fixture(scope="module")
def data_asset(
    datasource: PandasDatasource,
    get_missing_data_asset_error_type: type[Exception],
) -> Iterator[DataFrameAsset]:
    asset_name = f"i{uuid.uuid4().hex}"
    _ = datasource.add_dataframe_asset(
        name=asset_name,
    )
    data_asset = datasource.get_asset(asset_name=asset_name)
    yield data_asset
    datasource.delete_asset(asset_name=asset_name)
    with pytest.raises(get_missing_data_asset_error_type):
        datasource.get_asset(asset_name=asset_name)


@pytest.fixture(scope="module")
def batch_request(
    data_asset: DataFrameAsset,
    pandas_test_df: pd.DataFrame,
    in_memory_batch_request_missing_dataframe_error_type: type[Exception],
):
    with pytest.raises(in_memory_batch_request_missing_dataframe_error_type):
        data_asset.build_batch_request()
    return data_asset.build_batch_request(dataframe=pandas_test_df)


@pytest.fixture
def datasource_names_to_asset_names(datasource, data_asset):
    return {datasource.name: {data_asset.name}}


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
    get_missing_expectation_suite_error_type: type[Exception],
) -> Iterator[ExpectationSuite]:
    expectation_suite_name = f"{data_asset.datasource.name} | {data_asset.name}"
    expectation_suite = context.add_expectation_suite(
        expectation_suite_name=expectation_suite_name,
    )
    expectation_suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "string",
                "mostly": 1,
            },
        )
    )
    _ = context.add_or_update_expectation_suite(expectation_suite=expectation_suite)
    expectation_suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
    assert (
        len(expectation_suite.expectations) == 1
    ), "Expectation Suite was not updated in the previous method call."
    yield expectation_suite
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
    with pytest.raises(get_missing_expectation_suite_error_type):
        context.get_expectation_suite(expectation_suite_name=expectation_suite_name)


@pytest.fixture(scope="module")
def scheduled_checkpoint(
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
def checkpoint_event(checkpoint, datasource_names_to_asset_names, org_id_env_var: str):
    return RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        checkpoint_id=checkpoint.ge_cloud_id,
        schedule_id=uuid.UUID("e37cc13f-141d-4818-93c2-e3ec60024683"),
        datasource_names_to_asset_names=datasource_names_to_asset_names,
        organization_id=uuid.UUID(org_id_env_var),
    )


def test_running_checkpoint_action(
    context, checkpoint_event, cloud_base_url: str, org_id_env_var: str, token_env_var: str
):
    action = RunScheduledCheckpointAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=uuid.UUID(org_id_env_var),
        auth_key=token_env_var,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    # Act
    action_result = action.run(event=checkpoint_event, id=event_id)

    # Assert
    # Check that the action was successful, and we have received correct checkpoint event.
    assert action_result.type == checkpoint_event.type
    assert action_result.id == event_id

    # Check that the checkpoint was successful by querying the DB.
    validation_result_id = action_result.created_resources[0].resource_id
    resource_url = (
        f"{cloud_base_url}/organizations/"
        f"{org_id_env_var}/validation-results/{validation_result_id}"
    )
    with create_session(access_token=token_env_var) as session:
        response = session.get(resource_url)
        data = response.json()

    validation_result = data["data"]["attributes"]["validation_result"]
    assert validation_result["success"]
