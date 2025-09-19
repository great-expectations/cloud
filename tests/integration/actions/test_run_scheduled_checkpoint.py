from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest
from great_expectations import Checkpoint, ValidationDefinition

from great_expectations_cloud.agent.models import (
    DomainContext,
    RunScheduledCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset


from great_expectations.core.http import create_session

from great_expectations_cloud.agent.actions.run_scheduled_checkpoint import (
    RunScheduledCheckpointAction,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def scheduled_checkpoint(
    context: CloudDataContext,
    data_asset: DataFrameAsset,
    expectation_suite: ExpectationSuite,
    get_missing_checkpoint_error_type: type[Exception],
) -> Iterator[Checkpoint]:
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="WHOLE_DF")
    validation_defintition = context.validation_definitions.add(
        ValidationDefinition(
            name=f"{data_asset.datasource.name} | {data_asset.name}",
            data=batch_definition,
            suite=expectation_suite,
        )
    )

    checkpoint_name = f"{data_asset.datasource.name} | {data_asset.name}"
    _ = context.checkpoints.add(
        Checkpoint(
            name=checkpoint_name,
            validation_definitions=[validation_defintition, validation_defintition],
        )
    )
    _ = context.checkpoints.add_or_update(
        Checkpoint(
            name=checkpoint_name,
            validation_definitions=[validation_defintition],
        )
    )
    checkpoint = context.checkpoints.get(name=checkpoint_name)
    assert len(checkpoint.validation_definitions) == 1, (
        "Checkpoint was not updated in the previous method call."
    )
    yield checkpoint
    context.checkpoints.delete(name=checkpoint_name)
    with pytest.raises(get_missing_checkpoint_error_type):
        context.checkpoints.get(name=checkpoint_name)


@pytest.fixture
def checkpoint_event(scheduled_checkpoint, datasource_names_to_asset_names, org_id_env_var: str):
    return RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        checkpoint_id=scheduled_checkpoint.ge_cloud_id,
        schedule_id=uuid.UUID("e37cc13f-141d-4818-93c2-e3ec60024683"),
        datasource_names_to_asset_names=datasource_names_to_asset_names,
        organization_id=uuid.UUID(org_id_env_var),
        workspace_id=uuid.uuid4(),
    )


@pytest.mark.skip("This fails due to invalid datasource in db-seeder")
def test_running_checkpoint_action(
    context, checkpoint_event, cloud_base_url: str, org_id_env_var: str, token_env_var: str
):
    workspace_id_env_var = uuid.uuid4()
    action = RunScheduledCheckpointAction(
        context=context,
        base_url=cloud_base_url,
        domain_context=DomainContext(
            organization_id=uuid.UUID(org_id_env_var), workspace_id=workspace_id_env_var
        ),
        auth_key=token_env_var,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    # Act
    action_result = action.run(event=checkpoint_event, id=event_id)

    # Assert
    # Check that the action was successful, and we have received correct checkpoint event.
    assert action_result.type == checkpoint_event.type
    assert action_result.id == event_id

    # Check that the checkpoint was successful by querying the DB.
    validation_result_id = action_result.created_resources[0].resource_id
    resource_url = (
        f"{cloud_base_url}/organizations/{org_id_env_var}/validation-results/{validation_result_id}"
    )
    with create_session(access_token=token_env_var) as session:
        response = session.get(resource_url)
        data = response.json()

    validation_result = data["data"]["attributes"]["validation_result"]
    assert validation_result["success"]


def test_checkpoint_event_with_name():
    checkpoint_event = RunScheduledCheckpointEvent(
        type="run_scheduled_checkpoint.received",
        checkpoint_name="my_checkpoint",
        schedule_id=uuid.uuid4(),
        datasource_names_to_asset_names={},
        organization_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        checkpoint_id=uuid.uuid4(),
    )
    assert checkpoint_event.checkpoint_name == "my_checkpoint"
