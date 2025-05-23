from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.datasource.fluent import SnowflakeDatasource, SQLDatasource
from sqlalchemy import inspect

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

    tables = list(inspector.get_table_names())
    views = list(inspector.get_view_names())
    return tables + views
