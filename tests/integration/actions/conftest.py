from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Iterator

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
import pandas as pd
import pytest

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import PandasDatasource
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
    # TODO Bad config
    context.data_sources.delete(name="david_datasource")
    datasource = context.data_sources.add_pandas(
        name=datasource_name,
    )
    assert datasource.name == datasource_name
    datasource_name = f"i{uuid.uuid4().hex}"
    datasource.name = datasource_name
    datasource = context.data_sources.add_or_update_pandas(
        datasource=datasource,
    )
    assert (
        datasource.name == datasource_name
    ), "The datasource was not updated in the previous method call."
    yield datasource
    context.data_sources.delete_pandas(name=datasource_name)
    with pytest.raises(get_missing_datasource_error_type):
        context.data_sources.get(name=datasource_name)


@pytest.fixture(scope="module")
def data_asset(
    datasource: PandasDatasource,
    get_missing_data_asset_error_type: type[Exception],
) -> Iterator[DataFrameAsset]:
    asset_name = f"i{uuid.uuid4().hex}"
    _ = datasource.add_dataframe_asset(
        name=asset_name,
    )
    data_asset = datasource.get_asset(name=asset_name)
    yield data_asset
    datasource.delete_asset(name=asset_name)
    with pytest.raises(get_missing_data_asset_error_type):
        datasource.get_asset(name=asset_name)


@pytest.fixture(scope="module")
def batch_request(
    data_asset: DataFrameAsset,
    pandas_test_df: pd.DataFrame,
    in_memory_batch_request_missing_dataframe_error_type: type[Exception],
):
    return data_asset.build_batch_request(options={"dataframe": pandas_test_df})


@pytest.fixture
def datasource_names_to_asset_names(datasource, data_asset):
    return {datasource.name: {data_asset.name}}


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
    get_missing_expectation_suite_error_type: type[Exception],
) -> Iterator[gx.ExpectationSuite]:
    expectation_suite_name = f"{data_asset.datasource.name} | {data_asset.name}"
    expectation_suite = gx.ExpectationSuite(name=expectation_suite_name)
    context.suites.add(expectation_suite)

    expectation = gx.expectations.ExpectColumnValuesToBeNull(
        column="string",
        mostly=1,
    )
    expectation_suite.add_expectation(
        expectation=expectation,
    )

    expectation_suite = context.suites.get(name=expectation_suite_name)
    assert (
        len(expectation_suite.expectations) == 1
    ), "Expectation Suite was not updated in the previous method call."
    yield expectation_suite
    context.suites.delete(name=expectation_suite_name)
    with pytest.raises(get_missing_expectation_suite_error_type):
        context.suites.get(name=expectation_suite_name)
