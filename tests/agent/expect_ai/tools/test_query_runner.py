from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from great_expectations_cloud.agent.expect_ai.tools.query_runner import (
    QueryRunner,
    mssql_cte_restriction,
)


def _make_engine(dialect_name: str) -> tuple[MagicMock, MagicMock]:
    """Return (mock_engine, mock_conn) with the given dialect name."""
    mock_engine = MagicMock()
    mock_engine.dialect.name = dialect_name
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = lambda s: mock_conn
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return mock_engine, mock_conn


@pytest.mark.unit
def test_check_query_compiles_uses_explain_for_non_mssql() -> None:
    engine, conn = _make_engine("snowflake")

    success, error = QueryRunner._check_query_compiles(engine, "SELECT 1")

    assert success is True
    assert error is None
    assert conn.execute.call_count == 1
    executed = str(conn.execute.call_args[0][0])
    assert "EXPLAIN" in executed
    assert "SET PARSEONLY" not in executed


@pytest.mark.unit
def test_check_query_compiles_uses_parseonly_for_mssql() -> None:
    engine, conn = _make_engine("mssql")

    success, error = QueryRunner._check_query_compiles(engine, "SELECT 1 FROM dbo.t")

    assert success is True
    assert error is None
    calls = [str(c[0][0]) for c in conn.execute.call_args_list]
    assert any("SET PARSEONLY ON" in c for c in calls)
    assert any("SELECT 1 FROM dbo.t" in c for c in calls)
    assert any("SET PARSEONLY OFF" in c for c in calls)
    assert not any("EXPLAIN" in c for c in calls)


@pytest.mark.unit
def test_check_query_compiles_returns_error_on_mssql_syntax_error() -> None:
    engine, conn = _make_engine("mssql")
    conn.execute.side_effect = Exception("Incorrect syntax near 'BADINPUT'")

    success, error = QueryRunner._check_query_compiles(engine, "SELECT BADINPUT")

    assert success is False
    assert error is not None
    assert "Incorrect syntax" in error


@pytest.mark.unit
def test_get_dialect_constraints_returns_restriction_for_mssql() -> None:
    mock_context = MagicMock()
    mock_ds = MagicMock()
    mock_engine = MagicMock()
    mock_engine.dialect.name = "mssql"
    mock_ds.get_execution_engine.return_value = mock_engine
    mock_context.data_sources.get.return_value = mock_ds

    runner = QueryRunner(context=mock_context)
    result = runner.get_dialect_constraints(data_source_name="test_ds")

    assert "CTE" in result
    assert "SQL Server" in result
    assert "subqueries" in result


@pytest.mark.unit
def test_get_dialect_constraints_returns_empty_for_non_mssql() -> None:
    mock_context = MagicMock()
    mock_ds = MagicMock()
    mock_engine = MagicMock()
    mock_engine.dialect.name = "postgresql"
    mock_ds.get_execution_engine.return_value = mock_engine
    mock_context.data_sources.get.return_value = mock_ds

    runner = QueryRunner(context=mock_context)
    result = runner.get_dialect_constraints(data_source_name="test_ds")

    assert result == ""


@pytest.mark.unit
def test_mssql_cte_restriction_contains_key_guidance() -> None:
    restriction = mssql_cte_restriction()

    assert "CTE" in restriction
    assert "WITH" in restriction
    assert "subqueries" in restriction
    assert "SQL Server" in restriction
    assert "Fabric" in restriction
