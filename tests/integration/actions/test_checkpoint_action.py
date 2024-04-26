from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Iterator

import great_expectations.exceptions as gx_exceptions
import pandas as pd
import pytest
from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context import CloudDataContext

from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction
from great_expectations_cloud.agent.models import (
    RunCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.core import ExpectationSuite
    from great_expectations.datasource.fluent import BatchRequest, PandasDatasource
    from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset


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
    # it doesn't like this part
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


def test_loading_context(context):
    assert isinstance(context, CloudDataContext)


def test_running_checkpoint_action(context, checkpoint_event):
    action = RunCheckpointAction(context=context)
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    action_result = action.run(event=checkpoint_event, id=event_id)
    assert action_result.type == checkpoint_event.type
    assert action_result.id == event_id
