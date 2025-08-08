from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest
import sqlalchemy as sa
from great_expectations import ExpectationSuite, ValidationDefinition
from great_expectations.datasource.fluent import PostgresDatasource
from great_expectations.exceptions.exceptions import DataContextError
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


@pytest.fixture(scope="module")
def seed_and_cleanup_test_data(context: CloudDataContext):  # noqa: C901, PLR0912, PLR0915
    # Seed data
    data_source_name = "local_mercury_db"
    data_source = context.data_sources.get(data_source_name)
    assert isinstance(data_source, PostgresDatasource)

    table_data_asset_name = "local-mercury-db-checkpoints-table"
    try:
        table_data_asset = data_source.get_asset(table_data_asset_name)
    except LookupError:
        table_data_asset = data_source.add_table_asset(
            table_name="checkpoints", name=table_data_asset_name
        )

    suite_name = "local-mercury-db-checkpoints-table Suite"
    try:
        suite = context.suites.get(suite_name)
    except DataContextError:
        suite = context.suites.add(ExpectationSuite(name=suite_name))

    # Create validation
    batch_definition_name = "local-mercury-db-checkpoints-table-batch-definition"
    try:
        batch_definition = table_data_asset.get_batch_definition(batch_definition_name)
    except KeyError:
        batch_definition = table_data_asset.add_batch_definition_whole_table(
            name=batch_definition_name
        )

    validation_name = "local-mercury-db-checkpoints-table Validation"
    try:
        validation = context.validation_definitions.get(validation_name)
    except DataContextError:
        validation = context.validation_definitions.add(
            ValidationDefinition(
                name=validation_name,
                suite=suite,
                data=batch_definition,
            )
        )

    # Mark the validation as gx_managed
    engine = sa.create_engine("postgresql://postgres:postgres@localhost:5432/mercury")

    # Check if validation is already gx_managed
    with engine.connect() as conn:
        result = conn.execute(
            sa.text(f"SELECT gx_managed FROM validations WHERE id='{validation.id}'")
        )
        row = result.fetchone()
        needs_update = row and not row[0]  # Only update if not already gx_managed

    # Check if there's already a gx_managed=true validation for this asset
    if needs_update:
        with engine.connect() as conn:
            result = conn.execute(
                sa.text(f"""
                SELECT COUNT(*) FROM validations
                WHERE asset_ref_id = (SELECT asset_ref_id FROM validations WHERE id = '{validation.id}')
                AND gx_managed = true
                AND id != '{validation.id}'
            """)
            )
            existing_gx_managed = result.fetchone()[0] > 0

            if existing_gx_managed:
                # Set the existing gx_managed validation to false first
                conn.execute(
                    sa.text(f"""
                    UPDATE validations SET gx_managed = false
                    WHERE asset_ref_id = (SELECT asset_ref_id FROM validations WHERE id = '{validation.id}')
                    AND gx_managed = true
                """)
                )
                conn.commit()

    # Update if needed
    if needs_update:
        with engine.connect() as conn:
            conn.execute(
                sa.text(f"UPDATE validations SET gx_managed=true WHERE id='{validation.id}'")
            )
            conn.commit()

    # Mark the suite as gx_managed
    # Check if suite is already gx_managed
    with engine.connect() as conn:
        result = conn.execute(
            sa.text(f"SELECT gx_managed FROM expectation_suites WHERE id='{suite.id}'")
        )
        row = result.fetchone()
        needs_update = row and not row[0]  # Only update if not already gx_managed

    # Update if needed
    if needs_update:
        with engine.connect() as conn:
            conn.execute(
                sa.text(f"UPDATE expectation_suites SET gx_managed=true WHERE id='{suite.id}'")
            )
            conn.commit()

    # Yield
    yield table_data_asset, suite

    # clean up

    # Mark the validation as not gx_managed
    try:
        with engine.connect() as conn:
            conn.execute(
                sa.text(f"UPDATE validations SET gx_managed=false WHERE id='{validation.id}'")
            )
            conn.commit()
    except Exception as e:
        print(f"Warning: Error updating validation gx_managed status: {e}")

    # Mark the suite as not gx_managed
    try:
        with engine.connect() as conn:
            conn.execute(
                sa.text(f"UPDATE expectation_suites SET gx_managed=false WHERE id='{suite.id}'")
            )
            conn.commit()
    except Exception as e:
        print(f"Warning: Error updating suite gx_managed status: {e}")

    # Clean up in reverse order of dependencies
    try:
        context.validation_definitions.delete(name="local-mercury-db-checkpoints-table Validation")
    except Exception as e:
        print(f"Warning: Error deleting validation: {e}")

    try:
        context.suites.delete(name="local-mercury-db-checkpoints-table Suite")
    except Exception as e:
        print(f"Warning: Error deleting suite: {e}")

    try:
        data_source.delete_asset(name="local-mercury-db-checkpoints-table")
    except Exception as e:
        print(f"Warning: Error deleting asset: {e}")


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


def test_generate_data_quality_check_expectations_action_schema_change_selected_data_quality_issues_no_pre_existing_anomaly_detection_coverage(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    org_id_env_var_local: str,
    cloud_base_url: str,
    token_env_var_local: str,
    seed_and_cleanup_test_data,
    monkeypatch,
):
    def mock_no_anomaly_detection_coverage(self, data_asset_id: uuid.UUID | None):
        return {}

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_no_anomaly_detection_coverage,
    )

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


def test_generate_data_quality_check_expectations_action_multiple_selected_data_quality_issues_no_pre_existing_anomaly_detection_coverage(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    org_id_env_var_local: str,
    cloud_base_url: str,
    token_env_var_local: str,
    seed_and_cleanup_test_data,
    monkeypatch,
):
    def mock_no_anomaly_detection_coverage(self, data_asset_id: uuid.UUID | None):
        return {}

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_no_anomaly_detection_coverage,
    )

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


def test_generate_data_quality_check_expectations_action_completeness_selected_data_quality_issues_no_pre_existing_anomaly_detection_coverage(
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

    def mock_create_expectation(self, expectation, asset_id, created_via):
        # Capture the expectation config
        generated_expectations.append(expectation)
        # Call original method
        return original_create_expectation(self, expectation, asset_id, created_via)

    def mock_no_anomaly_detection_coverage(self, data_asset_id: uuid.UUID | None):
        return {}

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_no_anomaly_detection_coverage,
    )

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
        assert (
            exp_config.expectation_type
            == "expect_column_proportion_of_non_null_values_to_be_between"
        )
        # Verify each has required properties
        assert exp_config.column is not None
        if exp_config.windows is not None:
            for window in exp_config.windows:
                assert window.constraint_fn == "mean"
                assert window.range == 5
                assert window.offset.positive == window.offset.negative


def test_generate_data_quality_check_expectations_action_completeness_with_proportion_approach(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    org_id_env_var_local: str,
    cloud_base_url: str,
    token_env_var_local: str,
    seed_and_cleanup_test_data,
    monkeypatch,
):
    """Test that COMPLETENESS data quality issue generates a single ExpectColumnProportionOfNonNullValuesToBeBetween expectation."""
    # List to capture expectation configs
    generated_expectations = []

    # Store original method
    original_create_expectation = (
        GenerateDataQualityCheckExpectationsAction._create_expectation_for_asset
    )

    def mock_create_expectation(self, expectation, asset_id, created_via):
        # Capture the expectation config
        generated_expectations.append(expectation)
        # Call original method
        return original_create_expectation(self, expectation, asset_id, created_via)

    def mock_no_anomaly_detection_coverage(self, data_asset_id: uuid.UUID | None):
        return {}

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_no_anomaly_detection_coverage,
    )

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

    # Assert expectation properties - should be a single proportion expectation
    assert len(generated_expectations) > 0  # At least one expectation was created

    # Count the different expectation types
    proportion_expectations = [
        exp
        for exp in generated_expectations
        if exp.expectation_type == "expect_column_proportion_of_non_null_values_to_be_between"
    ]
    null_expectations = [
        exp
        for exp in generated_expectations
        if exp.expectation_type == "expect_column_values_to_be_null"
    ]
    not_null_expectations = [
        exp
        for exp in generated_expectations
        if exp.expectation_type == "expect_column_values_to_not_be_null"
    ]

    # Should only have proportion expectations
    assert len(proportion_expectations) > 0, "Should have at least one proportion expectation"
    assert len(null_expectations) == 0, (
        "Should not have any null expectations when using proportion approach"
    )
    assert len(not_null_expectations) == 0, (
        "Should not have any not-null expectations when using proportion approach"
    )

    # Verify proportion expectation properties
    for exp_config in proportion_expectations:
        assert exp_config.column is not None
        assert exp_config.windows is None or len(exp_config.windows) == 2
        if exp_config.windows is not None:
            for window in exp_config.windows:
                assert window.constraint_fn == "mean"
                assert window.range == 5
                assert window.offset.positive == window.offset.negative


def test_generate_data_quality_check_expectations_action_multiple_selected_data_quality_issues_pre_existing_volume_anomaly_detection_coverage(
    context: CloudDataContext,
    user_api_token_headers_org_admin_sc_org,
    org_id_env_var_local: str,
    cloud_base_url: str,
    token_env_var_local: str,
    seed_and_cleanup_test_data,
    monkeypatch,
):
    """Test that only a schema anomaly detection expectation is generated if a volume anomaly detection expectation already
    exists, but both schema and volume data quality issues are selected."""

    def mock_pre_existing_volume_anomaly_detection_coverage(self, data_asset_id: uuid.UUID | None):
        return {DataQualityIssues.VOLUME: ["only need key to exist"]}

    monkeypatch.setattr(
        GenerateDataQualityCheckExpectationsAction,
        "_get_current_anomaly_detection_coverage",
        mock_pre_existing_volume_anomaly_detection_coverage,
    )

    generate_anomaly_detection_expectations_event = GenerateDataQualityCheckExpectationsEvent(
        type="generate_data_quality_check_expectations_request.received",
        datasource_name="local_mercury_db",
        data_assets=["local-mercury-db-checkpoints-table"],
        organization_id=uuid.UUID(org_id_env_var_local),
        selected_data_quality_issues=[DataQualityIssues.SCHEMA, DataQualityIssues.VOLUME],
    )

    action = GenerateDataQualityCheckExpectationsAction(
        context=context,
        base_url="http://localhost:7000",  # Use the correct URL directly
        organization_id=uuid.UUID(org_id_env_var_local),
        auth_key=token_env_var_local,
    )
    event_id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"

    action_result = action.run(event=generate_anomaly_detection_expectations_event, id=event_id)

    # Assert
    assert action_result.type == generate_anomaly_detection_expectations_event.type
    assert action_result.id == event_id
    assert len(action_result.created_resources) >= 2
    assert action_result.created_resources[0].type == "MetricRun"
