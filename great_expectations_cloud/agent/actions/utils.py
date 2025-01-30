from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.datasource.fluent import SnowflakeDatasource, SQLDatasource
from sqlalchemy import inspect
from itertools import chain

from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect

if TYPE_CHECKING:
    from sqlalchemy.engine import Inspector


def get_table_names(datasource: SQLDatasource) -> list[str]:
    engine = datasource.get_engine()
    inspector: Inspector = inspect(engine)
    if isinstance(datasource, SnowflakeDatasource) and datasource.schema_:
        # Snowflake-SQLAlchemy uses the default_schema if no schema is provided to get_table_names
        # Or if the role does not have access to the schema (it silently fails and defaults to using default_schema)
        # See https://github.com/snowflakedb/snowflake-sqlalchemy/blob/e78319725d4b96ea205ef1264b744c65eb37853d/src/snowflake/sqlalchemy/snowdialect.py#L731
        # Explicitly passing the schema to the inspector to get the table names
        # Also converting to list to ensure JSON serializable
        return list(inspector.get_table_names(schema=datasource.schema_))
    
    if engine.dialect.name == GXSqlDialect.MSSQL:
        tables = list(chain.from_iterable(list(map(lambda o: f"{schema}.{o}", inspector.get_table_names(schema=schema))) for schema in inspector.get_schema_names()))
        views = list(chain.from_iterable(list(map(lambda o: f"{schema}.{o}", inspector.get_view_names(schema=schema))) for schema in inspector.get_schema_names()))
        return tables + views

    return list(inspector.get_table_names())

def verify_data_asset(log, datasource: SQLDatasource, asset):
    try:
        engine = datasource.get_engine()
        if engine.dialect.name == GXSqlDialect.MSSQL and asset.type == "table" and asset.schema_name == None and ("." in asset.name):
            datasource.delete_asset(asset.name)
            datasource.add_table_asset(name = asset.name, schema_name = asset.name.split('.')[0], table_name = asset.name.split('.')[1])
    except Exception as e:
        log(f"Failed to verify asset {asset.name}: {e}")
