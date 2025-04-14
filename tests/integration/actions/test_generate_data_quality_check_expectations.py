from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest
import sqlalchemy as sa
from great_expectations import ExpectationSuite, ValidationDefinition
from great_expectations.expectations.metadata_types import DataQualityIssues

from great_expectations_cloud.agent.actions.generate_data_quality_check_expectations_action import (
    GenerateDataQualityCheckExpectationsAction,
)
from great_expectations_cloud.agent.models import (
    GenerateDataQualityCheckExpectationsEvent,
)

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

pytestmark = pytest.mark.integration


@pytest.fixture
def user_api_token_headers_org_admin_sc_org():
    api_token = os.environ.get("GX_CLOUD_ACCESS_TOKEN")
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/vnd.api+json",
    }


@pytest.fixture
def seed_and_cleanup_test_data(context: CloudDataContext):
    # Seed data
    data_source_name = "local_mercury_db"
    data_source = context.data_sources.get(data_source_name)

    table_data_asset = data_source.add_table_asset(  # type: ignore[attr-defined] # FIXME
        table_name="checkpoints", name="local-mercury-db-checkpoints-table"
    )

    suite = context.suites.add(ExpectationSuite(name="local-mercury-db-checkpoints-table Suite"))

    # Create validation
    batch_definition = table_data_asset.add_batch_definition_whole_table(
        name="local-mercury-db-checkpoints-table-batch-definition"
    )
    validation = context.validation_definitions.add(
        ValidationDefinition(
            name="local-mercury-db-checkpoints-table Validation",
            suite=suite,
            data=batch_definition,
        )
    )

    # Mark the validation as gx_managed
    engine = sa.create_engine("postgresql://postgres:postgres@localhost:5432/mercury")
    with engine.begin() as conn:
        query = f"UPDATE validations SET gx_managed=true WHERE id='{validation.id}'"
        conn.execute(sa.text(query))
        conn.commit()

    # Mark the suite as gx_managed
    with engine.begin() as conn:
        query = f"UPDATE expectation_suites SET gx_managed=true WHERE id='{suite.id}'"
        conn.execute(sa.text(query))
        conn.commit()

    # Yield
    yield table_data_asset, suite

    # clean up

    # Mark the validation as not gx_managed
    with engine.begin() as conn:
        query = f"UPDATE validations SET gx_managed=false WHERE id='{validation.id}'"
        conn.execute(sa.text(query))
        conn.commit()

    # Mark the suite as not gx_managed
    with engine.begin() as conn:
        query = f"UPDATE expectation_suites SET gx_managed=false WHERE id='{suite.id}'"
        conn.execute(sa.text(query))
        conn.commit()

    context.validation_definitions.delete(name="local-mercury-db-checkpoints-table Validation")
    context.suites.delete(name="local-mercury-db-checkpoints-table Suite")
    data_source.delete_asset(name="local-mercury-db-checkpoints-table")


@pytest.fixture
def org_id_env_var_local():
    return "0ccac18e-7631-4bdd-8a42-3c35cce574c6"


@pytest.fixture
def token_env_var_local():
    return os.environ.get("GX_CLOUD_ACCESS_TOKEN")


def test_generate_data_quality_check_expectations_action_no_selected_data_quality_issues(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    org_id_env_var_local: str,
    cloud_base_url: str,
    token_env_var_local: str,
    seed_and_cleanup_test_data,
):
    generate_schema_change_expectations_event = GenerateDataQualityCheckExpectationsEvent(
        type="generate_data_quality_check_expectations_request.received",
        datasource_name="local_mercury_db",
        data_assets=["local-mercury-db-checkpoints-table"],
        organization_id=uuid.UUID(org_id_env_var_local),
        selected_data_quality_issues=None,
    )

    action = GenerateDataQualityCheckExpectationsAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=uuid.UUID(org_id_env_var_local),
        auth_key=token_env_var_local,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    action_result = action.run(event=generate_schema_change_expectations_event, id=event_id)

    # Assert
    assert action_result.type == generate_schema_change_expectations_event.type
    assert action_result.id == event_id
    # only a metric run resource should be created
    assert len(action_result.created_resources) == 1
    assert action_result.created_resources[0].type == "MetricRun"


def test_generate_data_quality_check_expectations_action_schema_change_selected_data_quality_issues(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    org_id_env_var_local: str,
    cloud_base_url: str,
    token_env_var_local: str,
    seed_and_cleanup_test_data,
):
    generate_schema_change_expectations_event = GenerateDataQualityCheckExpectationsEvent(
        type="generate_data_quality_check_expectations_request.received",
        datasource_name="local_mercury_db",
        data_assets=["local-mercury-db-checkpoints-table"],
        organization_id=uuid.UUID(org_id_env_var_local),
        selected_data_quality_issues=[DataQualityIssues.SCHEMA],
    )

    action = GenerateDataQualityCheckExpectationsAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=uuid.UUID(org_id_env_var_local),
        auth_key=token_env_var_local,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    action_result = action.run(event=generate_schema_change_expectations_event, id=event_id)

    # Assert
    assert action_result.type == generate_schema_change_expectations_event.type
    assert action_result.id == event_id
    # expected resources were created
    assert len(action_result.created_resources) == 2
    assert action_result.created_resources[0].type == "MetricRun"
    assert action_result.created_resources[1].type == "Expectation"


def test_generate_data_quality_check_expectations_action_multiple_selected_data_quality_issues(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    org_id_env_var_local: str,
    cloud_base_url: str,
    token_env_var_local: str,
    seed_and_cleanup_test_data,
):
    generate_schema_change_expectations_event = GenerateDataQualityCheckExpectationsEvent(
        type="generate_data_quality_check_expectations_request.received",
        datasource_name="local_mercury_db",
        data_assets=["local-mercury-db-checkpoints-table"],
        organization_id=uuid.UUID(org_id_env_var_local),
        selected_data_quality_issues=[
            DataQualityIssues.SCHEMA,
            DataQualityIssues.VOLUME,
        ],
    )

    action = GenerateDataQualityCheckExpectationsAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=uuid.UUID(org_id_env_var_local),
        auth_key=token_env_var_local,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    action_result = action.run(event=generate_schema_change_expectations_event, id=event_id)

    # Assert
    assert action_result.type == generate_schema_change_expectations_event.type
    assert action_result.id == event_id
    # expected resources were created
    assert len(action_result.created_resources) == 3
    assert action_result.created_resources[0].type == "MetricRun"
    assert action_result.created_resources[1].type == "Expectation"
    assert action_result.created_resources[2].type == "Expectation"


def test_generate_data_quality_check_expectations_action_completeness_selected_data_quality_issues(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    org_id_env_var_local: str,
    cloud_base_url: str,
    token_env_var_local: str,
    seed_and_cleanup_test_data,
    monkeypatch,
):
    """Test that COMPLETENESS data quality issue generates appropriate expectations."""
    # List to capture expectation configs
    generated_expectations = []

    # Store original method
    original_create_expectation = (
        GenerateDataQualityCheckExpectationsAction._create_expectation_for_asset
    )

    def mock_create_expectation(self, expectation, asset_id):
        # Capture the expectation config
        generated_expectations.append(expectation)
        # Call original method
        return original_create_expectation(self, expectation, asset_id)

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_create_expectation_for_asset",
        mock_create_expectation,
    )

    generate_completeness_change_expectations_event = GenerateDataQualityCheckExpectationsEvent(
        type="generate_data_quality_check_expectations_request.received",
        datasource_name="local_mercury_db",
        data_assets=["local-mercury-db-checkpoints-table"],
        organization_id=uuid.UUID(org_id_env_var_local),
        selected_data_quality_issues=[DataQualityIssues.COMPLETENESS],
    )

    action = GenerateDataQualityCheckExpectationsAction(
        context=context,
        base_url=cloud_base_url,
        organization_id=uuid.UUID(org_id_env_var_local),
        auth_key=token_env_var_local,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    action_result = action.run(event=generate_completeness_change_expectations_event, id=event_id)

    # Assert basic properties
    assert action_result.type == generate_completeness_change_expectations_event.type
    assert action_result.id == event_id
    assert len(action_result.created_resources) >= 2
    assert action_result.created_resources[0].type == "MetricRun"

    # Assert expectation properties
    assert len(generated_expectations) > 0  # At least one expectation was created
    for exp_config in generated_expectations:
        # Verify each expectation is one of the expected types
        assert exp_config.expectation_type in [
            "expect_column_values_to_be_null",
            "expect_column_values_to_not_be_null",
        ]
        # Verify each has required properties
        assert exp_config.column is not None
        if exp_config.windows is not None:
            assert exp_config.windows[0].constraint_fn == "mean"
            assert exp_config.windows[0].range == 5
            assert exp_config.windows[0].offset.positive == exp_config.windows[0].offset.negative
            assert exp_config.windows[0].parameter_name == exp_config.mostly["$PARAMETER"]
