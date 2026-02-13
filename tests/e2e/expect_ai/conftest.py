from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING

import great_expectations as gx
import pytest
import sqlalchemy

from tests.test_config import GxCloudTestConfig, SnowflakeTestConfig

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext


@pytest.fixture(scope="module")
def test_config() -> GxCloudTestConfig:
    """Load test configuration from environment variables."""
    return GxCloudTestConfig()


@pytest.fixture(scope="module")
def get_org_id_from_env(test_config: GxCloudTestConfig) -> str:
    return test_config.gx_cloud_organization_id


@pytest.fixture(scope="module")
def get_workspace_id_from_env(test_config: GxCloudTestConfig) -> str:
    assert test_config.gx_cloud_workspace_id, "No GX_CLOUD_WORKSPACE_ID env var"
    return test_config.gx_cloud_workspace_id


@pytest.fixture(scope="module")
def get_token_from_env(test_config: GxCloudTestConfig) -> str:
    return test_config.gx_cloud_access_token


@pytest.fixture(scope="module")
def snowflake_config() -> dict[str, str]:
    """Combine all Snowflake configuration into a single dictionary."""
    config = SnowflakeTestConfig()

    return {
        "account": config.snowflake_account,
        "user": config.snowflake_user,
        "password": config.snowflake_pw,
        "database": config.snowflake_database,
        "schema": config.snowflake_schema,
        "warehouse": config.snowflake_warehouse,
        "role": config.snowflake_role,
    }


@pytest.fixture(scope="module")
def get_cloud_base_url(test_config: GxCloudTestConfig) -> str:
    return test_config.gx_cloud_base_url or "https://api.greatexpectations.io"


@pytest.fixture(scope="module")
def context(
    get_org_id_from_env: str, get_token_from_env: str, get_cloud_base_url: str
) -> CloudDataContext:
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=get_cloud_base_url,
        cloud_organization_id=get_org_id_from_env,
        cloud_access_token=get_token_from_env,
    )
    return context


@pytest.fixture(scope="module")
def setup_test_data_source(
    context: CloudDataContext,
    snowflake_config: dict[str, str],
) -> Iterator[tuple[object, object, object]]:
    """Set up the test data source, asset, and batch definition with random names and cleanup"""
    # Generate random names to avoid conflicts
    ds_name = f"sql_agent_test_ds_{uuid.uuid4().hex[:8]}"
    table_name = f"test_activities_{uuid.uuid4().hex[:8]}"
    asset_name = f"asset_{uuid.uuid4().hex[:8]}"
    bd_name = f"batch_def_{uuid.uuid4().hex[:8]}"

    # Create test table with sample data
    connection_string = f"snowflake://{snowflake_config['user']}:{snowflake_config['password']}@{snowflake_config['account']}/{snowflake_config['database']}/{snowflake_config['schema']}?warehouse={snowflake_config['warehouse']}&role={snowflake_config['role']}"

    engine = sqlalchemy.create_engine(connection_string)

    # Create table with sample data
    create_table_sql = f"""
    CREATE TABLE {snowflake_config["schema"]}.{table_name} (
        id INTEGER,
        activity_type VARCHAR(50),
        distance FLOAT,
        grade_adjusted_distance FLOAT,
        pace FLOAT,
        heart_rate_max INTEGER,
        heart_rate_min INTEGER
    )
    """

    insert_data_sql = f"""
    INSERT INTO {snowflake_config["schema"]}.{table_name} VALUES
        (1, 'running', 5.0, 5.1, 8.5, 180, 120),
        (2, 'running', 3.2, 3.3, 7.2, 175, 115),
        (3, 'cycling', 15.0, 15.2, 4.5, 165, 110),
        (4, 'running', 10.0, 10.8, 9.1, 185, 125),
        (5, 'running', 2.1, 2.0, 6.8, 170, 118),
        (6, 'cycling', 25.0, 25.1, 3.2, 160, 105),
        (7, 'running', 8.0, 8.4, 8.0, 178, 122),
        (8, 'running', 1.5, 1.6, 12.0, 300, 50),
        (9, 'running', 6.0, 12.0, 7.5, 172, 116),
        (10, 'cycling', 20.0, 20.3, 2.5, 155, 100)
    """

    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(create_table_sql))
        conn.execute(sqlalchemy.text(insert_data_sql))
        conn.commit()

    # Create snowflake data source
    ds = context.data_sources.add_or_update_snowflake(
        name=ds_name,
        account=snowflake_config["account"],
        user=snowflake_config["user"],
        password=snowflake_config["password"],
        database=snowflake_config["database"],
        schema=snowflake_config["schema"],
        warehouse=snowflake_config["warehouse"],
        role=snowflake_config["role"],
    )

    # Create table asset
    asset = ds.add_table_asset(name=asset_name, table_name=table_name)

    # Create batch definition
    bd = asset.add_batch_definition_whole_table(name=bd_name)

    yield ds, asset, bd

    # Cleanup: drop the test table and delete the data source
    try:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(f"DROP TABLE {snowflake_config['schema']}.{table_name}"))
            conn.commit()
    except Exception as e:
        # If table drop fails, continue with cleanup
        # This is acceptable as we're cleaning up test resources
        print(f"Warning: Failed to drop test table {table_name}: {e}")

    context.data_sources.delete(name=ds_name)
