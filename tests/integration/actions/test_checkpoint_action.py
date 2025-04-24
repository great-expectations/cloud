from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest
from great_expectations import Checkpoint, ValidationDefinition

from great_expectations_cloud.agent.models import (
    RunCheckpointEvent,
)

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent.pandas_datasource import DataFrameAsset


from great_expectations.core.http import create_session

from great_expectations_cloud.agent.actions.run_checkpoint import RunCheckpointAction

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def checkpoint(
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
def checkpoint_event(checkpoint, datasource_names_to_asset_names, org_id_env_var: str):
    return RunCheckpointEvent(
        type="run_checkpoint_request",
        checkpoint_id=checkpoint.ge_cloud_id,
        datasource_names_to_asset_names=datasource_names_to_asset_names,
        organization_id=uuid.UUID(org_id_env_var),
    )


@pytest.mark.skip("This fails due to invalid datasource in db-seeder")
def test_running_checkpoint_action(
    context, checkpoint_event, cloud_base_url: str, org_id_env_var: str, token_env_var: str
):
    action = RunCheckpointAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=uuid.UUID(org_id_env_var),
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
