from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.datasource.fluent import SnowflakeDatasource, SQLDatasource
from sqlalchemy import inspect

if TYPE_CHECKING:
    from sqlalchemy.engine import Inspector


def get_table_names(datasource: SQLDatasource) -> list[str]:
    inspector: Inspector = inspect(datasource.get_engine())
    if isinstance(datasource, SnowflakeDatasource) and datasource.schema_:
        # Snowflake-SQLAlchemy uses the default_schema if no schema is provided to get_table_names
        # Or if the role does not have access to the schema (it silently fails and defaults to using default_schema)
        # See https://github.com/snowflakedb/snowflake-sqlalchemy/blob/e78319725d4b96ea205ef1264b744c65eb37853d/src/snowflake/sqlalchemy/snowdialect.py#L731
        # Explicitly passing the schema to the inspector to get the table names
        # Also converting to list to ensure JSON serializable
        return list(inspector.get_table_names(schema=datasource.schema_))

    return list(inspector.get_table_names())
