from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.datasource.fluent import SnowflakeDatasource, SQLDatasource
from great_expectations.datasource.fluent.sql_server_datasource import SQLServerDatasource
from pydantic import Field
from pydantic_settings import BaseSettings
from sqlalchemy import inspect

if TYPE_CHECKING:
    from sqlalchemy.engine import Inspector
    from sqlalchemy.sql.compiler import IdentifierPreparer

SCHEMA_DATASOURCES = (SnowflakeDatasource, SQLServerDatasource)


def get_asset_names(datasource: SQLDatasource) -> list[str]:
    inspector: Inspector = inspect(datasource.get_engine())
    identifier_preparer: IdentifierPreparer = inspector.dialect.identifier_preparer

    if isinstance(datasource, SCHEMA_DATASOURCES) and datasource.schema_:
        # Some datasources don't reliably scope table/view listing to the configured schema
        # unless it is passed explicitly to the inspector. For example,
        # Snowflake-SQLAlchemy silently falls back to its default_schema, and SQL Server's
        # dialect defaults to dbo because the schema cannot be embedded in the connection string.
        # See https://github.com/snowflakedb/snowflake-sqlalchemy/blob/e78319725d4b96ea205ef1264b744c65eb37853d/src/snowflake/sqlalchemy/snowdialect.py#L731
        tables = list(inspector.get_table_names(schema=datasource.schema_))
        views = list(inspector.get_view_names(schema=datasource.schema_))
        asset_names = tables + views
    else:
        tables = list(inspector.get_table_names())
        views = list(inspector.get_view_names())
        asset_names = tables + views

    # the identifier preparer adds quotes when they are necessary
    quoted_asset_names: list[str] = [
        identifier_preparer.quote(asset_name) for asset_name in asset_names
    ]
    return quoted_asset_names


class ExpectAICredentials(BaseSettings):
    openai_api_key: str | None = Field(default=None)

    @property
    def expect_ai_enabled(self) -> bool:
        return self.openai_api_key is not None


def ensure_openai_credentials() -> None:
    env_vars = ExpectAICredentials()
    if not env_vars.expect_ai_enabled:
        msg = "OpenAI credentials are not set for one or more Agents. Please set the OPENAI_API_KEY environment variable in all Agent deployments to enable ExpectAI."
        raise ValueError(msg)
