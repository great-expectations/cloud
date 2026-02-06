from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import great_expectations.expectations as gxe


class ExpectationPruner:
    """Expectation list pruner.

    NOTE: This should likely be treated as temporary.
    The goal is to limit the number of expectations.
    We will likely push this type of reasoning into the AI agent in the future.
    """

    def __init__(self, max_expectations: int = 10) -> None:
        self._max_expectations = max_expectations

    def prune_expectations(self, expectations: list[gxe.Expectation]) -> list[gxe.Expectation]:
        """Apply a few heuristics to cut down on the number of expectations.

        The idea here is we go through multiple rounds of filtering, resulting in a smaller list.
        """
        # only one per type per col
        expectations = self._limit_per_type_and_column(expectations)

        # limit the number per column to 2
        expectations = self._limit_per_column(expectations, max_for_col=2)

        # limit the number per column to 1 if we still have a lot
        if len(expectations) > self._max_expectations:
            expectations = self._limit_per_column(expectations, max_for_col=1)

        # cap the number of expectations
        return expectations[: self._max_expectations]

    def _limit_per_column(
        self,
        expectations: list[gxe.Expectation],
        max_for_col: int = 1,
    ) -> list[gxe.Expectation]:
        expectations_for_col_so_far = defaultdict[str, int](int)
        output: list[gxe.Expectation] = []
        for exp in expectations:
            if column := getattr(exp, "column", None):
                if expectations_for_col_so_far[column] < max_for_col:
                    output += [exp]
                    expectations_for_col_so_far[column] += 1
            else:
                output += [exp]
        return output

    def _limit_per_type_and_column(
        self,
        expectations: list[gxe.Expectation],
    ) -> list[gxe.Expectation]:
        so_far = defaultdict[tuple[str, str], int](int)
        output: list[gxe.Expectation] = []
        for exp in expectations:
            if column := getattr(exp, "column", None):
                key = (exp.expectation_type, column)
                if so_far[key] < 1:
                    output += [exp]
                    so_far[key] += 1
            else:
                output += [exp]
        return output

    def prune_invalid_columns(
        self,
        expectations: list[gxe.Expectation],
        valid_columns: set[str],
    ) -> list[gxe.Expectation]:
        """Prune expectations that reference columns not in the provided set.

        Args:
            expectations: List of expectations to filter
            valid_columns: Set of valid column names. Expectations referencing
                columns not in this set will be removed.

        Returns:
            Filtered list of expectations that only reference valid columns.
        """
        output: list[gxe.Expectation] = []
        for exp in expectations:
            # Check for single column attribute
            if column := getattr(exp, "column", None):
                if column not in valid_columns:
                    continue

            # Check for column_list attribute
            if column_list := getattr(exp, "column_list", None):
                if any(col not in valid_columns for col in column_list):
                    continue

            # Keep expectations that don't have column/column_list or have all valid columns
            output.append(exp)
        return output
