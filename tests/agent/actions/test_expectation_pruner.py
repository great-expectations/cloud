from __future__ import annotations

import pytest
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToNotBeNull,
    ExpectTableRowCountToBeBetween,
)

from great_expectations_cloud.agent.actions.expectation_pruner import ExpectationPruner

pytestmark = pytest.mark.unit


def test_prune_expectations_basic():
    """Test basic pruning with max expectations cap."""
    # ARRANGE
    pruner = ExpectationPruner(max_expectations=5)
    expectations = [
        ExpectColumnValuesToBeInSet(column=f"col_{i}", value_set=["a", "b"]) for i in range(10)
    ]

    # ACT
    result = pruner.prune_expectations(expectations)

    # ASSERT
    assert len(result) <= 5


def test_limit_per_type_and_column():
    """Test that only one expectation per type+column is kept."""
    # ARRANGE
    pruner = ExpectationPruner(max_expectations=10)
    expectations = [
        ExpectColumnValuesToBeInSet(column="status", value_set=["a", "b"]),
        ExpectColumnValuesToBeInSet(column="status", value_set=["c", "d"]),  # Duplicate type+column
        ExpectColumnValuesToBeUnique(column="id"),
        ExpectColumnValuesToNotBeNull(column="status"),  # Different type, same column
    ]

    # ACT
    result = pruner.prune_expectations(expectations)

    # ASSERT
    # Should keep first of each type+column combination
    assert len(result) == 3
    type_col_combos = [(exp.expectation_type, getattr(exp, "column", None)) for exp in result]
    assert len(set(type_col_combos)) == 3


def test_limit_per_column_two_max():
    """Test limiting to 2 expectations per column."""
    # ARRANGE
    pruner = ExpectationPruner(max_expectations=20)
    expectations = [
        ExpectColumnValuesToBeInSet(column="status", value_set=["a"]),
        ExpectColumnValuesToNotBeNull(column="status"),
        ExpectColumnValuesToBeUnique(column="status"),  # Third on same column
        ExpectColumnValuesToBeInSet(column="other", value_set=["x"]),
    ]

    # ACT
    result = pruner._limit_per_column(expectations, max_for_col=2)

    # ASSERT
    status_count = sum(1 for exp in result if getattr(exp, "column", None) == "status")
    assert status_count == 2


def test_limit_per_column_one_max():
    """Test limiting to 1 expectation per column."""
    # ARRANGE
    pruner = ExpectationPruner(max_expectations=20)
    expectations = [
        ExpectColumnValuesToBeInSet(column="col1", value_set=["a"]),
        ExpectColumnValuesToNotBeNull(column="col1"),
        ExpectColumnValuesToBeUnique(column="col2"),
        ExpectColumnValuesToNotBeNull(column="col2"),
    ]

    # ACT
    result = pruner._limit_per_column(expectations, max_for_col=1)

    # ASSERT
    col1_count = sum(1 for exp in result if getattr(exp, "column", None) == "col1")
    col2_count = sum(1 for exp in result if getattr(exp, "column", None) == "col2")
    assert col1_count == 1
    assert col2_count == 1


def test_prune_expectations_preserves_table_level():
    """Test that table-level expectations (without column) are preserved."""
    # ARRANGE
    pruner = ExpectationPruner(max_expectations=10)
    expectations = [
        ExpectTableRowCountToBeBetween(min_value=1, max_value=1000),
        ExpectColumnValuesToBeInSet(column="col1", value_set=["a"]),
    ]

    # ACT
    result = pruner.prune_expectations(expectations)

    # ASSERT
    table_level = [exp for exp in result if not hasattr(exp, "column")]
    assert len(table_level) == 1


def test_prune_invalid_columns():
    """Test pruning expectations with invalid column names."""
    # ARRANGE
    pruner = ExpectationPruner()
    valid_columns = {"col1", "col2", "col3"}
    expectations = [
        ExpectColumnValuesToBeInSet(column="col1", value_set=["a"]),
        ExpectColumnValuesToBeInSet(column="invalid_col", value_set=["b"]),  # Invalid
        ExpectColumnValuesToNotBeNull(column="col2"),
        ExpectTableRowCountToBeBetween(min_value=1, max_value=100),  # No column
    ]

    # ACT
    result = pruner.prune_invalid_columns(expectations, valid_columns)

    # ASSERT
    assert len(result) == 3
    column_names = [getattr(exp, "column", None) for exp in result]
    assert "invalid_col" not in column_names
    assert "col1" in column_names
    assert "col2" in column_names
    assert None in column_names  # table-level expectation


def test_prune_invalid_columns_with_column_list():
    """Test pruning expectations with column_list attribute."""
    # ARRANGE
    pruner = ExpectationPruner()
    valid_columns = {"col1", "col2"}

    # Create mock expectations with column_list
    exp_valid = ExpectColumnValuesToBeInSet(column="col1", value_set=["a"])
    exp_valid.column_list = ["col1", "col2"]  # All valid

    exp_invalid = ExpectColumnValuesToBeInSet(column="col1", value_set=["a"])
    exp_invalid.column_list = ["col1", "invalid_col"]  # Contains invalid

    expectations = [exp_valid, exp_invalid]

    # ACT
    result = pruner.prune_invalid_columns(expectations, valid_columns)

    # ASSERT
    assert len(result) == 1


def test_prune_expectations_full_pipeline():
    """Test the full pruning pipeline with many expectations."""
    # ARRANGE
    pruner = ExpectationPruner(max_expectations=10)
    expectations = []

    # Add many expectations for various columns
    for i in range(5):
        for col in ["col1", "col2", "col3", "col4"]:
            expectations.append(ExpectColumnValuesToBeInSet(column=col, value_set=[f"val_{i}"]))

    # 20 total expectations
    assert len(expectations) == 20

    # ACT
    result = pruner.prune_expectations(expectations)

    # ASSERT
    assert len(result) <= 10
    # Verify no duplicate type+column combinations
    type_col_combos = [(exp.expectation_type, getattr(exp, "column", None)) for exp in result]
    assert len(type_col_combos) == len(set(type_col_combos))


def test_prune_expectations_empty_list():
    """Test pruning with empty expectations list."""
    # ARRANGE
    pruner = ExpectationPruner()
    expectations = []

    # ACT
    result = pruner.prune_expectations(expectations)

    # ASSERT
    assert result == []


def test_prune_invalid_columns_empty_valid_set():
    """Test pruning with empty valid columns set removes all column-level expectations."""
    # ARRANGE
    pruner = ExpectationPruner()
    valid_columns = set()
    expectations = [
        ExpectColumnValuesToBeInSet(column="col1", value_set=["a"]),
        ExpectTableRowCountToBeBetween(min_value=1, max_value=100),
    ]

    # ACT
    result = pruner.prune_invalid_columns(expectations, valid_columns)

    # ASSERT
    assert len(result) == 1  # Only table-level expectation
    assert not hasattr(result[0], "column")
