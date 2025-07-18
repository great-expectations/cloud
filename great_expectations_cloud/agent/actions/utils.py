from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.datasource.fluent import SnowflakeDatasource, SQLDatasource
from great_expectations.datasource.fluent.sqlserver_datasource import SQLServerDatasource
from sqlalchemy import inspect
from itertools import chain

from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect

if TYPE_CHECKING:
    from sqlalchemy.engine import Inspector


def get_asset_names(datasource: SQLDatasource) -> list[str]:
    inspector: Inspector = inspect(datasource.get_engine())
    if isinstance(datasource, SnowflakeDatasource) and datasource.schema_:
        # Snowflake-SQLAlchemy uses the default_schema if no schema is provided to get_table_names
        # Or if the role does not have access to the schema (it silently fails and defaults to using default_schema)
        # See https://github.com/snowflakedb/snowflake-sqlalchemy/blob/e78319725d4b96ea205ef1264b744c65eb37853d/src/snowflake/sqlalchemy/snowdialect.py#L731
        # Explicitly passing the schema to the inspector to get the table and view names
        # Also converting to list to ensure JSON serializable
        tables = list(inspector.get_table_names(schema=datasource.schema_))
        views = list(inspector.get_view_names(schema=datasource.schema_))
        return tables + views
    
    elif isinstance(datasource, SQLServerDatasource) and datasource.schema_:
        # SQL Server datasource with schema scoping - consistent with Snowflake pattern
        # Explicitly passing the schema to the inspector to get the table and view names
        # Also converting to list to ensure JSON serializable
        tables = list(inspector.get_table_names(schema=datasource.schema_))
        views = list(inspector.get_view_names(schema=datasource.schema_))
        return tables + views
    # engine = datasource.get_engine()
    # if engine.dialect.name == GXSqlDialect.MSSQL:
    #     tables = list(chain.from_iterable(list(map(lambda o: f"{schema}.{o}", inspector.get_table_names(schema=schema))) for schema in inspector.get_schema_names()))
    #     views = list(chain.from_iterable(list(map(lambda o: f"{schema}.{o}", inspector.get_view_names(schema=schema))) for schema in inspector.get_schema_names()))
    #     return tables + views

    tables = list(inspector.get_table_names())
    views = list(inspector.get_view_names())
    return tables + views

# def verify_data_asset(log, datasource: SQLDatasource, asset):
#     """
#     Legacy function - no longer needed with new schema fallback approach.
#     Assets with schema_name=None are now automatically handled by TableAsset.as_selectable()
#     """
#     try:
#         engine = datasource.get_engine()
#         if engine.dialect.name == GXSqlDialect.MSSQL and asset.type == "table" and asset.schema_name == None and ("." in asset.name):
#             datasource.delete_asset(asset.name)
#             datasource.add_table_asset(name = asset.name, schema_name = asset.name.split('.')[0], table_name = asset.name.split('.')[1])
#     except Exception as e:
#         log(f"Failed to verify asset {asset.name}: {e}")
