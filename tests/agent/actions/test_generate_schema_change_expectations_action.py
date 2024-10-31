from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)

from great_expectations_cloud.agent.actions.generate_schema_change_expectations_action import (
    GenerateSchemaChangeExpectationsAction,
)
from great_expectations_cloud.agent.models import GenerateSchemaChangeExpectationsEvent

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


def test_generate_schema_change_expectations_action_smoke_test(
    mock_context: CloudDataContext, mocker: MockerFixture
):
    """Smoke test for GenerateSchemaChangeExpectationsAction. Until we have a better way to test this in ZELDA-1058,
    we will just test that the calls to`compute_metric_list_run` are made."""
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = GenerateSchemaChangeExpectationsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    action.run(
        event=GenerateSchemaChangeExpectationsEvent(
            type="generate_schema_change_expectations_request.received",
            organization_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["test-data-asset1", "test-data-asset2"],
            expectation_suite_id=uuid.uuid4(),
            create_expectations=True,
        ),
        id="test-id",
    )
    mock_batch_inspector.compute_metric_list_run.assert_called()
