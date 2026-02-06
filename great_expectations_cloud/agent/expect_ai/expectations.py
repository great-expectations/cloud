from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal

from great_expectations.expectations import (
    Expectation,
)
from great_expectations.expectations import (
    ExpectColumnDistinctValuesToBeInSet as GXExpectColumnDistinctValuesToBeInSet,
)
from great_expectations.expectations import (
    ExpectColumnDistinctValuesToContainSet as GXExpectColumnDistinctValuesToContainSet,
)
from great_expectations.expectations import (
    ExpectColumnDistinctValuesToEqualSet as GXExpectColumnDistinctValuesToEqualSet,
)
from great_expectations.expectations import (
    ExpectColumnMaxToBeBetween as GXExpectColumnMaxToBeBetween,
)
from great_expectations.expectations import (
    ExpectColumnMeanToBeBetween as GXExpectColumnMeanToBeBetween,
)
from great_expectations.expectations import (
    ExpectColumnMedianToBeBetween as GXExpectColumnMedianToBeBetween,
)
from great_expectations.expectations import (
    ExpectColumnMinToBeBetween as GXExpectColumnMinToBeBetween,
)
from great_expectations.expectations import (
    ExpectColumnPairValuesAToBeGreaterThanB as GXExpectColumnPairValuesAToBeGreaterThanB,
)
from great_expectations.expectations import (
    ExpectColumnStdevToBeBetween as GXExpectColumnStdevToBeBetween,
)
from great_expectations.expectations import (
    ExpectColumnUniqueValueCountToBeBetween as GXExpectColumnUniqueValueCountToBeBetween,
)
from great_expectations.expectations import (
    ExpectColumnValueLengthsToBeBetween as GXExpectColumnValueLengthsToBeBetween,
)
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween as GXExpectColumnValuesToBeBetween,
)
from great_expectations.expectations import (
    ExpectColumnValuesToBeUnique as GXExpectColumnValuesToBeUnique,
)
from great_expectations.expectations import (
    ExpectColumnValuesToMatchLikePattern as GXExpectColumnValuesToMatchLikePattern,
)
from great_expectations.expectations import (
    ExpectColumnValuesToMatchRegex as GXExpectColumnValuesToMatchRegex,
)
from great_expectations.expectations import (
    ExpectCompoundColumnsToBeUnique as GXExpectCompoundColumnsToBeUnique,
)
from great_expectations.expectations import (
    ExpectTableColumnsToMatchOrderedList as GXExpectTableColumnsToMatchOrderedList,
)
from great_expectations.expectations import (
    UnexpectedRowsExpectation as GXUnexpectedRowsExpectation,
)
from pydantic import BaseModel, Field
from typing_extensions import override


class OpenAIGXExpectation(ABC, BaseModel):
    expectation_type: str = Field(..., description="The type of the expectation")
    description: str = Field(
        ...,
        description="A plain language description of the business intent for the expectation.",
    )

    @abstractmethod
    def get_gx_expectation(self) -> Expectation:
        raise NotImplementedError


class ExpectTableColumnsToMatchOrderedList(OpenAIGXExpectation):
    expectation_type: Literal["expect_table_columns_to_match_ordered_list"] = (
        "expect_table_columns_to_match_ordered_list"
    )
    column_list: list[str] = Field(
        ..., description="A list of all column names from the table in the expected order."
    )

    @override
    def get_gx_expectation(self) -> GXExpectTableColumnsToMatchOrderedList:
        return GXExpectTableColumnsToMatchOrderedList(
            column_list=self.column_list,
            description=self.description,
        )


class ExpectColumnMinToBeBetween(OpenAIGXExpectation):
    expectation_type: Literal["expect_column_min_to_be_between"] = "expect_column_min_to_be_between"
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    min_value: float = Field(..., description="The minimum value of the min (inclusive)")
    max_value: float = Field(..., description="The maximum value of the min (inclusive)")

    @override
    def get_gx_expectation(self) -> GXExpectColumnMinToBeBetween:
        return GXExpectColumnMinToBeBetween(
            column=self.column,
            min_value=self.min_value,
            max_value=self.max_value,
            description=self.description,
        )


class ExpectColumnMaxToBeBetween(OpenAIGXExpectation):
    expectation_type: Literal["expect_column_max_to_be_between"] = "expect_column_max_to_be_between"
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    min_value: float = Field(..., description="The minimum value of the max (inclusive)")
    max_value: float = Field(..., description="The maximum value of the max (inclusive)")

    @override
    def get_gx_expectation(self) -> GXExpectColumnMaxToBeBetween:
        return GXExpectColumnMaxToBeBetween(
            column=self.column,
            min_value=self.min_value,
            max_value=self.max_value,
            description=self.description,
        )


class ExpectColumnMedianToBeBetween(OpenAIGXExpectation):
    expectation_type: Literal["expect_column_median_to_be_between"] = (
        "expect_column_median_to_be_between"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    min_value: float = Field(..., description="The minimum value of the median (inclusive)")
    max_value: float = Field(..., description="The maximum value of the median (inclusive)")

    @override
    def get_gx_expectation(self) -> GXExpectColumnMedianToBeBetween:
        return GXExpectColumnMedianToBeBetween(
            column=self.column,
            min_value=self.min_value,
            max_value=self.max_value,
            description=self.description,
        )


class ExpectColumnMeanToBeBetween(OpenAIGXExpectation):
    expectation_type: Literal["expect_column_mean_to_be_between"] = (
        "expect_column_mean_to_be_between"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    min_value: float = Field(..., description="The minimum value of the mean (inclusive)")
    max_value: float = Field(..., description="The maximum value of the mean (inclusive)")

    @override
    def get_gx_expectation(self) -> GXExpectColumnMeanToBeBetween:
        return GXExpectColumnMeanToBeBetween(
            column=self.column,
            min_value=self.min_value,
            max_value=self.max_value,
            description=self.description,
        )


class ExpectColumnStdevToBeBetween(OpenAIGXExpectation):
    expectation_type: Literal["expect_column_stdev_to_be_between"] = (
        "expect_column_stdev_to_be_between"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    min_value: float = Field(
        ..., description="The minimum value of the standard deviation (inclusive)"
    )
    max_value: float = Field(
        ..., description="The maximum value of the standard deviation (inclusive)"
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnStdevToBeBetween:
        return GXExpectColumnStdevToBeBetween(
            column=self.column,
            min_value=self.min_value,
            max_value=self.max_value,
            description=self.description,
            strict_min=False,
            strict_max=False,
        )


class ExpectColumnValuesToBeBetween(OpenAIGXExpectation):
    expectation_type: Literal["expect_column_values_to_be_between"] = (
        "expect_column_values_to_be_between"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    min_value: float = Field(..., description="The minimum value of the values (inclusive)")
    max_value: float = Field(..., description="The maximum value of the values (inclusive)")
    mostly: float = Field(
        ...,
        description="The ratio of values that must be between the min and max for the expectation to pass, a float between 0 and 1",
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnValuesToBeBetween:
        return GXExpectColumnValuesToBeBetween(
            column=self.column,
            min_value=self.min_value,
            max_value=self.max_value,
            mostly=self.mostly,
            description=self.description,
        )


class ExpectColumnValueLengthsToBeBetween(OpenAIGXExpectation):
    """Expect the string length of values in a column to fall within a specified range."""

    expectation_type: Literal["expect_column_value_lengths_to_be_between"] = (
        "expect_column_value_lengths_to_be_between"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    min_value: int = Field(..., description="The minimum value of the value lengths (inclusive)")
    max_value: int = Field(..., description="The maximum value of the value lengths (inclusive)")
    mostly: float = Field(
        ...,
        description="The ratio of values that must have lengths between the min and max for the expectation to pass, a float between 0 and 1",
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnValueLengthsToBeBetween:
        return GXExpectColumnValueLengthsToBeBetween(
            column=self.column,
            min_value=self.min_value,
            max_value=self.max_value,
            mostly=self.mostly,
            description=self.description,
        )


class ExpectColumnValuesToBeUnique(OpenAIGXExpectation):
    """Expect values in a column to be unique."""

    expectation_type: Literal["expect_column_values_to_be_unique"] = (
        "expect_column_values_to_be_unique"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    mostly: float = Field(
        ...,
        description="The ratio of values that must be unique for the expectation to pass, a float between 0 and 1",
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnValuesToBeUnique:
        return GXExpectColumnValuesToBeUnique(
            column=self.column,
            description=self.description,
        )


class ExpectColumnValuesToMatchRegex(OpenAIGXExpectation):
    expectation_type: Literal["expect_column_values_to_match_regex"] = (
        "expect_column_values_to_match_regex"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    regex: str = Field(..., description="A regular expression to match the column values")
    mostly: float = Field(
        ...,
        description="The ratio of values that must match the regex for the expectation to pass, a float between 0 and 1",
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnValuesToMatchRegex:
        return GXExpectColumnValuesToMatchRegex(
            column=self.column,
            regex=self.regex,
            mostly=self.mostly,
            description=self.description,
        )


class ExpectColumnValuesToMatchLikePattern(OpenAIGXExpectation):
    expectation_type: Literal["expect_column_values_to_match_like_pattern"] = (
        "expect_column_values_to_match_like_pattern"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    like_pattern: str = Field(
        ..., description="A SQL like pattern expression to match the column values"
    )
    mostly: float = Field(
        ...,
        description="The ratio of values that must match the regex for the expectation to pass, a float between 0 and 1",
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnValuesToMatchLikePattern:
        return GXExpectColumnValuesToMatchLikePattern(
            column=self.column,
            like_pattern=self.like_pattern,
            mostly=self.mostly,
            description=self.description,
        )


class ExpectColumnUniqueValueCountToBeBetween(OpenAIGXExpectation):
    """Expect the number of unique (distinct) values in a column to be between a min and max value."""

    expectation_type: Literal["expect_column_unique_value_count_to_be_between"] = (
        "expect_column_unique_value_count_to_be_between"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    min_value: int = Field(..., description="The minimum number of unique values (inclusive)")
    max_value: int = Field(..., description="The maximum number of unique values (inclusive)")

    @override
    def get_gx_expectation(self) -> GXExpectColumnUniqueValueCountToBeBetween:
        return GXExpectColumnUniqueValueCountToBeBetween(
            column=self.column,
            min_value=self.min_value,
            max_value=self.max_value,
            description=self.description,
        )


class ExpectColumnDistinctValuesToBeInSet(OpenAIGXExpectation):
    """Expect all values in a column to be in a set of expected values."""

    expectation_type: Literal["expect_column_distinct_values_to_be_in_set"] = (
        "expect_column_distinct_values_to_be_in_set"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    value_set: list[str | float | int] = Field(
        ..., description="A list of the expected values. This list should never be empty."
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnDistinctValuesToBeInSet:
        return GXExpectColumnDistinctValuesToBeInSet(
            column=self.column,
            value_set=self.value_set,
            description=self.description,
        )


class ExpectColumnDistinctValuesToContainSet(OpenAIGXExpectation):
    """Expect all values in a provided set to be covered among the values in a column."""

    expectation_type: Literal["expect_column_distinct_values_to_contain_set"] = (
        "expect_column_distinct_values_to_contain_set"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    value_set: list[str | float | int] = Field(
        ..., description="A list of the expected values. This list should never be empty."
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnDistinctValuesToContainSet:
        return GXExpectColumnDistinctValuesToContainSet(
            column=self.column,
            value_set=self.value_set,
            description=self.description,
        )


class ExpectColumnDistinctValuesToEqualSet(OpenAIGXExpectation):
    """Expect the distinct values of a column to exactly match a provided set."""

    expectation_type: Literal["expect_column_distinct_values_to_equal_set"] = (
        "expect_column_distinct_values_to_equal_set"
    )
    column: str = Field(
        ..., description="The column name from the table to which the expectation is applied"
    )
    value_set: list[str | float | int] = Field(
        ..., description="A list of the expected values. This list should never be empty."
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnDistinctValuesToEqualSet:
        return GXExpectColumnDistinctValuesToEqualSet(
            column=self.column,
            value_set=self.value_set,
            description=self.description,
        )


class ExpectCompoundColumnsToBeUnique(OpenAIGXExpectation):
    """Expect the values in a list of columns to be unique when combined."""

    expectation_type: Literal["expect_compound_columns_to_be_unique"] = (
        "expect_compound_columns_to_be_unique"
    )
    column_list: list[str] = Field(
        ...,
        description="A list of column names from the table that should together have unique values. This list must have at least two columns.",
    )

    @override
    def get_gx_expectation(self) -> GXExpectCompoundColumnsToBeUnique:
        return GXExpectCompoundColumnsToBeUnique(
            # TODO: THIS IS A HACK TO WORK AROUND A GX BUG
            # SEE: https://greatexpectationslabs.slack.com/archives/C03AEDCEE12/p1729794478218249
            column_list=[column.lower() for column in self.column_list],
            description=self.description,
        )


class ExpectColumnPairValuesAToBeGreaterThanB(OpenAIGXExpectation):
    """Expect the values in column A to be greater than or equal to the values in column B"""

    expectation_type: Literal["expect_column_pair_values_a_to_be_greater_than_b"] = (
        "expect_column_pair_values_a_to_be_greater_than_b"
    )
    column_A: str = Field(
        ..., description="The column name from the table whose values should be greater"
    )
    column_B: str = Field(
        ..., description="The column name from the table whose values should be smaller"
    )

    @override
    def get_gx_expectation(self) -> GXExpectColumnPairValuesAToBeGreaterThanB:
        return GXExpectColumnPairValuesAToBeGreaterThanB(
            column_A=self.column_A,
            column_B=self.column_B,
            description=self.description,
            or_equal=True,
        )


class UnexpectedRowsExpectation(OpenAIGXExpectation):
    """Test data using a SQL query that should return unexpected rows. Access the current batch of data as `{batch}`."""

    expectation_type: Literal["unexpected_rows_expectation"] = "unexpected_rows_expectation"
    query: str = Field(..., description="A SQL query that should return the unexpected rows")

    @override
    def get_gx_expectation(self) -> GXUnexpectedRowsExpectation:
        return GXUnexpectedRowsExpectation(
            unexpected_rows_query=self.query,
            description=self.description,
        )


# We've removed expectations we use to generate:
# 6/3/2025 - ExpectColumnValuesToBeNull: Was producing similar Expectations to the
#                 completeness Expectations produced during Add Asset
# 5/28/2025 - ExpectColumnValuesToBeOfType: The types weren't quite correct and it
#                 provides limited value over our schema expectation.
# 10/9/2024 - ExpectColumnPairValuesToBeInSet: Removed because list[tuple[str, str]]
#                 generated a schema that openai could not parse
class AddExpectationsResponse(BaseModel):
    # Note: while a discriminated union would make more sense in pydantic terms,
    # it produces "oneOf" in the schema which is invalid for OpenAI
    # expectations: List[
    #     Annotated[Union[
    #         ExpectTableColumnsToMatchOrderedList,
    #         ExpectColumnValuesToMatchRegex,
    #         ExpectColumnValuesToMatchLikePattern,
    #         ExpectColumnDistinctValuesToBeInSet,
    #         ExpectColumnDistinctValuesToContainSet,
    #         ExpectColumnDistinctValuesToEqualSet,
    #         ExpectColumnMinToBeBetween,
    #         ExpectColumnMaxToBeBetween,
    #         ExpectColumnMeanToBeBetween,
    #         ExpectColumnMedianToBeBetween,
    #         ExpectColumnStdevToBeBetween,
    #         ExpectColumnValuesToBeBetween,
    #         ExpectColumnValueLengthsToBeBetween,
    #         ExpectCompoundColumnsToBeUnique,
    #         ExpectColumnPairValuesAToBeGreaterThanB,
    #         UnexpectedRowsExpectation,
    #     ], Field(discriminator="expectation_type")]
    # ]
    rationale: str = Field(..., description="The rationale for the expectations")
    expectations: list[
        ExpectTableColumnsToMatchOrderedList
        | ExpectColumnValuesToMatchRegex
        | ExpectColumnValuesToMatchLikePattern
        | ExpectColumnDistinctValuesToBeInSet
        | ExpectColumnDistinctValuesToContainSet
        | ExpectColumnDistinctValuesToEqualSet
        | ExpectColumnMinToBeBetween
        | ExpectColumnMaxToBeBetween
        | ExpectColumnMeanToBeBetween
        | ExpectColumnMedianToBeBetween
        | ExpectColumnStdevToBeBetween
        | ExpectColumnValuesToBeBetween
        | ExpectColumnValueLengthsToBeBetween
        | ExpectCompoundColumnsToBeUnique
        | ExpectColumnPairValuesAToBeGreaterThanB
        | UnexpectedRowsExpectation
    ]
