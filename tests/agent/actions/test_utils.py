from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.sql_datasource import TableAsset

from great_expectations_cloud.agent.actions.utils import (
    SCHEMA_DATASOURCES,
    apply_datasource_schema_to_asset,
    get_asset_names,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_get_asset_names_with_sql_datasource(mocker: MockerFixture) -> None:
    mock_sqlalchemy_inspect = mocker.patch("great_expectations_cloud.agent.actions.utils.inspect")
    datasource = mocker.Mock(spec=SQLDatasource)
    engine = mocker.Mock()
    datasource.get_engine.return_value = engine
    inspector = mocker.Mock()
    mock_sqlalchemy_inspect.return_value = inspector
    dialect = mocker.Mock()
    inspector.dialect = dialect
    identifier_preparer = mocker.Mock()
    inspector.dialect.identifier_preparer = identifier_preparer
    inspector.get_table_names.return_value = ["table_1", "table_2"]
    inspector.get_view_names.return_value = ["view_1", "view_2"]
    identifier_preparer.quote.side_effect = ["table_1", "table_2", "view_1", "view_2"]

    table_names = get_asset_names(datasource)

    assert table_names == ["table_1", "table_2", "view_1", "view_2"]
    mock_sqlalchemy_inspect.assert_called_once_with(engine)
    inspector.get_table_names.assert_called_once_with()
    inspector.get_view_names.assert_called_once_with()
    identifier_preparer.quote.assert_has_calls(
        [
            mocker.call("table_1"),
            mocker.call("table_2"),
            mocker.call("view_1"),
            mocker.call("view_2"),
        ]
    )


@pytest.mark.parametrize("datasource_class", SCHEMA_DATASOURCES)
def test_get_asset_names_with_schema_datasource(
    mocker: MockerFixture, datasource_class: type
) -> None:
    mock_sqlalchemy_inspect = mocker.patch("great_expectations_cloud.agent.actions.utils.inspect")
    datasource = mocker.Mock(spec=datasource_class)
    datasource.schema_ = "test_schema"
    engine = mocker.Mock()
    datasource.get_engine.return_value = engine
    inspector = mocker.Mock()
    mock_sqlalchemy_inspect.return_value = inspector
    dialect = mocker.Mock()
    inspector.dialect = dialect
    identifier_preparer = mocker.Mock()
    inspector.dialect.identifier_preparer = identifier_preparer
    inspector.get_table_names.return_value = ["table_1", "table_2"]
    inspector.get_view_names.return_value = ["view_1", "view_2"]
    identifier_preparer.quote.side_effect = ["table_1", "table_2", "view_1", "view_2"]

    table_names = get_asset_names(datasource)

    assert table_names == ["table_1", "table_2", "view_1", "view_2"]
    mock_sqlalchemy_inspect.assert_called_once_with(engine)
    inspector.get_table_names.assert_called_once_with(schema="test_schema")
    inspector.get_view_names.assert_called_once_with(schema="test_schema")
    identifier_preparer.quote.assert_has_calls(
        [
            mocker.call("table_1"),
            mocker.call("table_2"),
            mocker.call("view_1"),
            mocker.call("view_2"),
        ]
    )


@pytest.mark.parametrize("datasource_class", SCHEMA_DATASOURCES)
def test_get_asset_names_with_schema_datasource_no_schema(
    mocker: MockerFixture, datasource_class: type
) -> None:
    mock_sqlalchemy_inspect = mocker.patch("great_expectations_cloud.agent.actions.utils.inspect")
    datasource = mocker.Mock(spec=datasource_class)
    datasource.schema_ = None
    engine = mocker.Mock()
    datasource.get_engine.return_value = engine
    inspector = mocker.Mock()
    mock_sqlalchemy_inspect.return_value = inspector
    dialect = mocker.Mock()
    inspector.dialect = dialect
    identifier_preparer = mocker.Mock()
    inspector.dialect.identifier_preparer = identifier_preparer
    inspector.get_table_names.return_value = ["table_1", "table_2"]
    inspector.get_view_names.return_value = ["view_1", "view_2"]
    identifier_preparer.quote.side_effect = ["table_1", "table_2", "view_1", "view_2"]

    table_names = get_asset_names(datasource)

    assert table_names == ["table_1", "table_2", "view_1", "view_2"]
    mock_sqlalchemy_inspect.assert_called_once_with(engine)
    inspector.get_table_names.assert_called_once_with()
    inspector.get_view_names.assert_called_once_with()
    identifier_preparer.quote.assert_has_calls(
        [
            mocker.call("table_1"),
            mocker.call("table_2"),
            mocker.call("view_1"),
            mocker.call("view_2"),
        ]
    )


class TestApplyDatasourceSchemaToAsset:
    @staticmethod
    def _mock_datasource(mocker: MockerFixture, spec: type, schema: str | None) -> Mock:
        datasource: Mock = mocker.Mock(spec=spec)
        datasource.schema_ = schema
        return datasource

    @pytest.mark.parametrize("datasource_class", SCHEMA_DATASOURCES)
    def test_sets_schema_on_table_asset(
        self, mocker: MockerFixture, datasource_class: type
    ) -> None:
        datasource = self._mock_datasource(mocker, datasource_class, schema="my_schema")
        data_asset = TableAsset(name="trips", table_name="trips")

        apply_datasource_schema_to_asset(datasource, data_asset)

        assert data_asset.schema_name == "my_schema"

    @pytest.mark.parametrize("datasource_class", SCHEMA_DATASOURCES)
    def test_noop_when_datasource_has_no_schema(
        self, mocker: MockerFixture, datasource_class: type
    ) -> None:
        datasource = self._mock_datasource(mocker, datasource_class, schema=None)
        data_asset = TableAsset(name="trips", table_name="trips")

        apply_datasource_schema_to_asset(datasource, data_asset)

        assert data_asset.schema_name is None

    @pytest.mark.parametrize("datasource_class", SCHEMA_DATASOURCES)
    def test_noop_when_asset_already_has_schema(
        self, mocker: MockerFixture, datasource_class: type
    ) -> None:
        datasource = self._mock_datasource(mocker, datasource_class, schema="datasource_schema")
        data_asset = TableAsset(name="trips", table_name="trips", schema_name="existing_schema")

        apply_datasource_schema_to_asset(datasource, data_asset)

        assert data_asset.schema_name == "existing_schema"

    def test_noop_for_non_schema_datasource(self, mocker: MockerFixture) -> None:
        datasource = self._mock_datasource(mocker, SQLDatasource, schema="some_schema")
        data_asset = TableAsset(name="trips", table_name="trips")

        apply_datasource_schema_to_asset(datasource, data_asset)

        assert data_asset.schema_name is None

    def test_noop_for_non_table_asset(self, mocker: MockerFixture) -> None:
        datasource = self._mock_datasource(mocker, SCHEMA_DATASOURCES[0], schema="my_schema")
        data_asset = mocker.Mock()
        data_asset.schema_name = None

        apply_datasource_schema_to_asset(datasource, data_asset)

        assert data_asset.schema_name is None
