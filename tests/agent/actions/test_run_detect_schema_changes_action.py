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

from great_expectations_cloud.agent.actions.run_detect_schema_changes import (
    DetectSchemaChangesAction,
)
from great_expectations_cloud.agent.models import SchemaChangeDetectedEvent

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
    from pytest_mock import MockerFixture

pytestmark = pytest.mark.unit


def test_run_detect_schema_changes_smoke_test(
    mock_context: CloudDataContext, mocker: MockerFixture
):
    """Smoke test for DetectSchemaChangesAction. Until we have a better way to test this in ZELDA-1058,
    we will just test that the calls to`compute_metric_list_run` are made."""
    mock_metric_repository = mocker.Mock(spec=MetricRepository)
    mock_batch_inspector = mocker.Mock(spec=BatchInspector)

    action = DetectSchemaChangesAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
        base_url="",
        auth_key="",
        organization_id=uuid.uuid4(),
    )

    action._raise_on_any_metric_exception = mocker.Mock()  # type: ignore[method-assign]
    # mock so that we don't raise

    action.run(
        event=SchemaChangeDetectedEvent(
            type="schema_fetch_request.received",
            organiozation_id=uuid.uuid4(),
            datasource_name="test-datasource",
            data_assets=["test-data-asset1", "test-data-asset2"],
            create_expectations=True,
        ),
        id="test-id",
    )
    mock_batch_inspector.compute_metric_list_run.assert_called()
