from __future__ import annotations

import logging
import os
import uuid
from typing import TYPE_CHECKING, Final, Iterator

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
import pandas as pd
import pytest
from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context import CloudDataContext

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.datasource.fluent import BatchRequest, PandasDatasource
    from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset


LOGGER: Final = logging.getLogger("tests")


@pytest.fixture(scope="module")
def in_memory_batch_request_missing_dataframe_error_type() -> type[Exception]:
    return ValueError


@pytest.fixture(scope="module")
def get_missing_expectation_suite_error_type():
    return gx_exceptions.DataContextError


@pytest.fixture(scope="module")
def get_missing_datasource_error_type() -> type[Exception]:
    return ValueError


@pytest.fixture(scope="module")
def get_missing_data_asset_error_type() -> type[Exception]:
    return LookupError


@pytest.fixture(scope="module")
def cloud_base_url() -> str:
    return "http://localhost:5000"


@pytest.fixture
def org_id():
    return os.environ.get("GX_CLOUD_ORGANIZATION_ID")


@pytest.fixture
def token():
    return os.environ.get("GX_CLOUD_ACCESS_TOKEN")


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


# TODO make a data asset that we can actually calculate these metrics from?


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
def datasource(
    context: CloudDataContext,
    get_missing_datasource_error_type: type[Exception],
) -> Iterator[PandasDatasource]:
    datasource_name = f"{uuid.uuid4()}"
    datasource = context.sources.add_pandas(
        name=datasource_name,
    )
    assert datasource.name == datasource_name
    datasource_name = f"{uuid.uuid4()}"
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
    asset_name = f"{uuid.uuid4()}"
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
) -> BatchRequest:
    with pytest.raises(in_memory_batch_request_missing_dataframe_error_type):
        data_asset.build_batch_request()
    return data_asset.build_batch_request(dataframe=pandas_test_df)


@pytest.fixture(scope="module")
def context() -> CloudDataContext:
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    return context
