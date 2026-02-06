from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import create_autospec

import pytest
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.metrics.batch.row_count import BatchRowCount, BatchRowCountResult
from great_expectations.metrics.metric_results import MetricErrorResult, MetricErrorResultValue
from great_expectations.validator.metric_configuration import MetricConfigurationID

from great_expectations_cloud.agent.expect_ai.metric_service import (
    MetricNotComputableError,
    MetricService,
)

if TYPE_CHECKING:
    from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import BatchParameters


@pytest.fixture
def mock_batch():
    return create_autospec(Batch)


@pytest.fixture
def mock_batch_definition(mock_batch):
    mock_def = create_autospec(BatchDefinition)
    mock_def.get_batch.return_value = mock_batch
    return mock_def


@pytest.fixture
def mock_metric():
    return create_autospec(BatchRowCount(), instance=True)


@pytest.fixture
def mock_metric_result():
    result = create_autospec(
        BatchRowCountResult(
            id=MetricConfigurationID(
                metric_name="batch_row_count",
                metric_domain_kwargs_id="",
                metric_value_kwargs_id="",
            ),
            value=10,
        ),
        instance=True,
    )
    result.value = 10
    return result


@pytest.fixture
def mock_error_result():
    return create_autospec(
        MetricErrorResult(
            id=MetricConfigurationID(
                metric_name="batch_row_count",
                metric_domain_kwargs_id="",
                metric_value_kwargs_id="",
            ),
            value=MetricErrorResultValue(exception_message="Specific error about what went wrong"),
        ),
        instance=True,
    )


@pytest.mark.unit
def test_get_metric_result_success(
    mock_context, mock_batch, mock_batch_definition, mock_metric, mock_metric_result
):
    # Arrange
    mock_batch.compute_metrics.return_value = mock_metric_result
    batch_parameters: BatchParameters = {"param1": "value1"}

    # Act
    service = MetricService(context=mock_context)
    result = service.get_metric_result(
        batch_definition=mock_batch_definition,
        metric=mock_metric,
        batch_parameters=batch_parameters,
    )

    # Assert
    mock_batch_definition.get_batch.assert_called_once_with(batch_parameters)
    mock_batch.compute_metrics.assert_called_once_with(mock_metric)
    assert result == mock_metric_result


@pytest.mark.unit
def test_get_metric_result_error(
    mock_context, mock_batch, mock_batch_definition, mock_metric, mock_error_result
):
    # Arrange
    mock_batch.compute_metrics.return_value = mock_error_result

    # Act
    service = MetricService(context=mock_context)
    result = service.get_metric_result(
        batch_definition=mock_batch_definition,
        metric=mock_metric,
        batch_parameters=None,
    )

    # Assert
    mock_batch_definition.get_batch.assert_called_once_with(None)
    mock_batch.compute_metrics.assert_called_once_with(mock_metric)
    assert result == mock_error_result


@pytest.mark.unit
@pytest.mark.parametrize(
    "batch_parameters",
    [
        {"param1": "value1", "param2": "value2"},
        None,
    ],
)
def test_get_metric_value_success(
    mock_context,
    mock_batch,
    mock_batch_definition,
    mock_metric,
    mock_metric_result,
    batch_parameters,
):
    # Arrange
    mock_batch.compute_metrics.return_value = mock_metric_result

    # Act
    service = MetricService(context=mock_context)
    result = service.get_metric_value(
        batch_definition=mock_batch_definition,
        metric=mock_metric,
        batch_parameters=batch_parameters,
    )

    # Assert
    mock_batch_definition.get_batch.assert_called_once_with(batch_parameters)
    mock_batch.compute_metrics.assert_called_once_with(mock_metric)
    assert result == 10


@pytest.mark.unit
def test_get_metric_value_error(
    mock_context, mock_batch, mock_batch_definition, mock_metric, mock_error_result
):
    # Arrange
    mock_batch.compute_metrics.return_value = mock_error_result

    # Act
    service = MetricService(context=mock_context)

    # Assert
    with pytest.raises(MetricNotComputableError) as e:
        service.get_metric_value(
            batch_definition=mock_batch_definition,
            metric=mock_metric,
            batch_parameters=None,
        )

    assert "Could not compute metric" in str(e)


@pytest.mark.parametrize(
    "batch_parameters",
    [
        {"param1": "value1", "param2": "value2"},
        None,
    ],
)
@pytest.mark.unit
def test_get_metric_value_or_error_text_success(
    mock_context,
    mock_batch,
    mock_batch_definition,
    mock_metric,
    mock_metric_result,
    batch_parameters,
):
    # Arrange
    mock_batch.compute_metrics.return_value = mock_metric_result

    # Act
    service = MetricService(context=mock_context)
    result_value = service.get_metric_value_or_error_text(
        batch_definition=mock_batch_definition,
        metric=mock_metric,
        batch_parameters=batch_parameters,
    )

    # Assert
    mock_batch_definition.get_batch.assert_called_once_with(batch_parameters)
    mock_batch.compute_metrics.assert_called_once_with(mock_metric)
    assert result_value == mock_metric_result.value


@pytest.mark.unit
def test_get_metric_value_or_error_text_error(
    mock_context, mock_batch, mock_batch_definition, mock_metric, mock_error_result
):
    # Arrange
    mock_batch.compute_metrics.return_value = mock_error_result

    # Act
    service = MetricService(context=mock_context)
    result = service.get_metric_value_or_error_text(
        batch_definition=mock_batch_definition,
        metric=mock_metric,
        batch_parameters=None,
    )

    # Assert
    assert isinstance(result, str)
    assert "Could not compute metric" in result
