from __future__ import annotations

from great_expectations.datasource.fluent import SnowflakeDatasource, SQLDatasource

from great_expectations_cloud.agent.actions.utils import get_asset_names


def test_get_asset_names_with_sql_datasource(mocker):
    mock_sqlalchemy_inspect = mocker.patch("great_expectations_cloud.agent.actions.utils.inspect")
    datasource = mocker.Mock(spec=SQLDatasource)
    engine = mocker.Mock()
    datasource.get_engine.return_value = engine
    inspector = mocker.Mock()
    mock_sqlalchemy_inspect.return_value = inspector
    inspector.get_asset_names.return_value = ["table_1", "table_2"]
    inspector.get_view_names.return_value = ["view_1", "view_2"]

    table_names = get_asset_names(datasource)

    assert table_names == ["table_1", "table_2", "view_1", "view_2"]
    mock_sqlalchemy_inspect.assert_called_once_with(engine)
    inspector.get_asset_names.assert_called_once_with()
    inspector.get_view_names.assert_called_once_with()


def test_get_asset_names_with_snowflake_datasource(mocker):
    mock_sqlalchemy_inspect = mocker.patch("great_expectations_cloud.agent.actions.utils.inspect")
    datasource = mocker.Mock(spec=SnowflakeDatasource)
    datasource.schema_ = "test_schema"
    engine = mocker.Mock()
    datasource.get_engine.return_value = engine
    inspector = mocker.Mock()
    mock_sqlalchemy_inspect.return_value = inspector
    inspector.get_asset_names.return_value = ["table_1", "table_2"]
    inspector.get_view_names.return_value = ["view_1", "view_2"]

    table_names = get_asset_names(datasource)

    assert table_names == ["table_1", "table_2", "view_1", "view_2"]
    mock_sqlalchemy_inspect.assert_called_once_with(engine)
    inspector.get_asset_names.assert_called_once_with(schema="test_schema")
    inspector.get_view_names.assert_called_once_with(schema="test_schema")


def test_get_asset_names_with_snowflake_datasource_no_schema(mocker):
    mock_sqlalchemy_inspect = mocker.patch("great_expectations_cloud.agent.actions.utils.inspect")
    datasource = mocker.Mock(spec=SnowflakeDatasource)
    datasource.schema_ = None
    engine = mocker.Mock()
    datasource.get_engine.return_value = engine
    inspector = mocker.Mock()
    mock_sqlalchemy_inspect.return_value = inspector
    inspector.get_asset_names.return_value = ["table_1", "table_2"]
    inspector.get_view_names.return_value = ["view_1", "view_2"]

    table_names = get_asset_names(datasource)

    assert table_names == ["table_1", "table_2", "view_1", "view_2"]
    mock_sqlalchemy_inspect.assert_called_once_with(engine)
    inspector.get_asset_names.assert_called_once_with()
    inspector.get_view_names.assert_called_once_with()
