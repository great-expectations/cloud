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


def test_get_asset_names_with_snowflake_datasource(mocker):
    mock_sqlalchemy_inspect = mocker.patch("great_expectations_cloud.agent.actions.utils.inspect")
    datasource = mocker.Mock(spec=SnowflakeDatasource)
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


def test_get_asset_names_with_snowflake_datasource_no_schema(mocker):
    mock_sqlalchemy_inspect = mocker.patch("great_expectations_cloud.agent.actions.utils.inspect")
    datasource = mocker.Mock(spec=SnowflakeDatasource)
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
