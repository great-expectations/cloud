from __future__ import annotations

import pytest
from langchain_openai import ChatOpenAI

from great_expectations_cloud.agent.expect_ai.config import OPENAI_MODEL
from great_expectations_cloud.agent.expect_ai.expectations import (
    ExpectColumnDistinctValuesToBeInSet,
    ExpectColumnDistinctValuesToContainSet,
    ExpectColumnDistinctValuesToEqualSet,
    ExpectColumnMaxToBeBetween,
    ExpectColumnMeanToBeBetween,
    ExpectColumnMedianToBeBetween,
    ExpectColumnMinToBeBetween,
    ExpectColumnPairValuesAToBeGreaterThanB,
    ExpectColumnStdevToBeBetween,
    ExpectColumnUniqueValueCountToBeBetween,
    ExpectColumnValueLengthsToBeBetween,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToMatchLikePattern,
    ExpectColumnValuesToMatchRegex,
    ExpectCompoundColumnsToBeUnique,
    ExpectTableColumnsToMatchOrderedList,
    OpenAIGXExpectation,
    UnexpectedRowsExpectation,
)

ExpectationTypes = [
    ExpectColumnDistinctValuesToBeInSet,
    ExpectColumnDistinctValuesToContainSet,
    ExpectColumnDistinctValuesToEqualSet,
    ExpectColumnMaxToBeBetween,
    ExpectColumnMeanToBeBetween,
    ExpectColumnMedianToBeBetween,
    ExpectColumnMinToBeBetween,
    ExpectColumnPairValuesAToBeGreaterThanB,
    ExpectColumnStdevToBeBetween,
    ExpectColumnUniqueValueCountToBeBetween,
    ExpectColumnValueLengthsToBeBetween,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToMatchLikePattern,
    ExpectColumnValuesToMatchRegex,
    ExpectCompoundColumnsToBeUnique,
    ExpectTableColumnsToMatchOrderedList,
    UnexpectedRowsExpectation,
]


# This is a unit test but is used to make sure the integration tests in this file
# cover all our supported expectations.
@pytest.mark.unit
def test_expectation_type_coverage() -> None:
    """Ensures we are testing all expectation types in these integration tests"""
    expectation_types = OpenAIGXExpectation.__subclasses__()
    assert set(expectation_types) == set(ExpectationTypes)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expectation_type",
    ExpectationTypes,
)
@pytest.mark.parametrize("model_name", [OPENAI_MODEL])
async def test_individual_expectation_schemas(
    expectation_type: type[OpenAIGXExpectation], model_name: str
) -> None:
    model = ChatOpenAI(model=model_name).with_structured_output(
        schema=expectation_type, method="json_schema", strict=True
    )
    res = await model.ainvoke("Recommend an expectation about a table of NYC Taxi Data.")
    assert isinstance(res, expectation_type)
