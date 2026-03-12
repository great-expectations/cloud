from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent.interfaces import (
        DataAsset,
        Datasource,
        _DataAssetT,
        _ExecutionEngineT,
    )
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine
    from sqlalchemy.engine import Engine


class QueryRunner:
    """
    A tool for running SQL queries and checking if they compile.
    """

    def __init__(self, context: CloudDataContext):
        """
        Initialize a new QueryRunner instance.

        :param context: The Great Expectations CloudDataContext object to use for data source retrieval.
        """
        self._context = context

    def _get_data_source_from_context(
        self, data_source_name: str
    ) -> Datasource[_DataAssetT, _ExecutionEngineT]:
        """
        Retrieve a data source from the context by its name.

        :param data_source_name: The name of the data source to retrieve.
        :return: The Datasource object associated with the given name.
        """
        return self._context.data_sources.get(name=data_source_name)

    def check_query_compiles(
        self, data_source_name: str, query_text: str
    ) -> tuple[bool, str | None]:
        """
        Check if a SQL query compiles using the provided SQLAlchemy engine.

        :param data_source_name: Name of the data source to use for compilation.
        :param query_text: The raw SQL query string to compile.
        :return: A tuple where the first element is a boolean indicating if the query compiles successfully, and the second element is an error message if compilation fails, otherwise None.
        """
        ds: Datasource[DataAsset[Any, Any], SqlAlchemyExecutionEngine] = (
            self._get_data_source_from_context(data_source_name)
        )
        engine: Engine = ds.get_execution_engine().engine

        return self._check_query_compiles(engine=engine, query_text=query_text)

    @staticmethod
    def _check_query_compiles(engine: Engine, query_text: str) -> tuple[bool, str | None]:
        """
        Check if a SQL query compiles using the provided SQLAlchemy engine.

        :param engine: A SQLAlchemy Engine object used to compile the query.
        :param query_text: The raw SQL query string to compile.
        :return: A tuple where the first element is a boolean indicating if the query compiles successfully, and the second element is an error message if compilation fails, otherwise None.
        """
        try:
            with engine.connect() as conn:
                if engine.dialect.name == "mssql":
                    conn.execute(text("SET PARSEONLY ON"))
                    conn.execute(text(query_text))
                    conn.execute(text("SET PARSEONLY OFF"))
                else:
                    conn.execute(text("EXPLAIN " + query_text))
        except Exception as e:
            return False, str(e)
        return True, None

    def get_dialect(self, data_source_name: str) -> str:
        """
        Get the dialect of a data source by its name.

        :param data_source_name: The name of the data source to retrieve.
        :return: The dialect of the data source as a string.
        """
        ds: Datasource[DataAsset[Any, Any], SqlAlchemyExecutionEngine] = (
            self._get_data_source_from_context(data_source_name)
        )
        engine: SqlAlchemyExecutionEngine = ds.get_execution_engine()
        dialect: str = engine.dialect.name
        return dialect.lower()

    def get_dialect_constraints(self, data_source_name: str) -> str:
        """
        Get dialect-specific SQL constraints for prompt engineering.

        :param data_source_name: The name of the data source to retrieve.
        :return: Constraint text to include in LLM prompts, or empty string if none apply.
        """
        dialect = self.get_dialect(data_source_name)
        return _MSSQL_CTE_RESTRICTION if dialect == "mssql" else ""


_MSSQL_CTE_RESTRICTION = (
    "CRITICAL: Do NOT use Common Table Expressions (CTEs / WITH clauses) in any SQL queries. "
    "CTEs are not supported for SQL Server and Fabric in the execution engine. "
    "Use subqueries instead. For example, instead of:\n"
    "  WITH dupes AS (SELECT col, COUNT(*) AS n FROM {batch} GROUP BY col HAVING COUNT(*) > 1) "
    "SELECT * FROM dupes\n"
    "Use:\n"
    "  SELECT * FROM (SELECT col, COUNT(*) AS n FROM {batch} GROUP BY col HAVING COUNT(*) > 1) AS dupes"
)
