# THIS TEST REQUIRES THAT THE FOLLOWING ENVIRONMENT VARIABLES ARE SET:
#     GX_CLOUD_ORGANIZATION_ID
#     GX_CLOUD_WORKSPACE_ID
#     GX_CLOUD_ACCESS_TOKEN
#     OPENAI_API_KEY
from __future__ import annotations

import logging.config
import os
import random
import string
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from great_expectations import (
    Checkpoint,
    ExpectationSuite,
    ValidationDefinition,
    get_context,
)
from great_expectations.exceptions import StoreBackendError
from sqlalchemy import text

from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import AssetReviewAgent
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    BatchParameters,
    GenerateExpectationsInput,
)
from great_expectations_cloud.agent.expect_ai.config import logging_config
from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
from great_expectations_cloud.agent.expect_ai.tools.metrics import AgentToolsManager
from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

logging.config.dictConfig(logging_config)

logger = logging.getLogger("expect_ai.demo_gx")


def random_resource_name() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=10))


async def create_snowflake_generate_expectations_suite(
    context: CloudDataContext,
) -> AsyncGenerator[str, None]:
    data_source = None
    engine = None
    table_name = None

    try:
        data_source = context.data_sources.add_or_update_snowflake(
            name=random_resource_name(),
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            password=os.getenv("SNOWFLAKE_PW", ""),
            database=os.getenv("SNOWFLAKE_DATABASE", ""),
            schema=os.getenv("SNOWFLAKE_SCHEMA", ""),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        # Get the engine and create a table
        engine = data_source.get_engine()
        data = {
            "col_1": [1, 2],
            "col_2": ["A", "B"],
            "col_3": [
                datetime(year=2025, month=1, day=1, tzinfo=UTC),
                datetime(year=2025, month=2, day=1, tzinfo=UTC),
            ],
        }
        df = pd.DataFrame(data)
        table_name = random_resource_name()
        df.to_sql(table_name, engine, index=False)

        # Create the table asset
        asset_name = random_resource_name()
        table_asset = data_source.add_table_asset(
            name=asset_name,
            table_name=table_name,
        )

        # Create the batch definition
        batch_definition_name = random_resource_name()
        batch_definition = table_asset.add_batch_definition_monthly(batch_definition_name, "col_3")

        generate_expectations_input = GenerateExpectationsInput(
            organization_id=context.ge_cloud_config.organization_id,
            workspace_id=context.ge_cloud_config.workspace_id,
            data_source_name=table_asset.datasource.name,
            data_asset_name=table_asset.name,
            batch_definition_name=batch_definition.name,
            batch_parameters=BatchParameters(
                {"year": 2025, "month": 2},
            ),
        )

        logger.info(f"Processing data source {table_asset.datasource.name}")
        logger.info(f"Processing asset {table_asset.name}")
        logger.info(f"Processing batch definition {batch_definition.name}")

        yield await do_run_gx_e2e(
            context=context,
            generate_expectations_input=generate_expectations_input,
        )
    finally:
        # Clean up all resources
        if table_name and engine:
            try:
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE {table_name}"))
            except Exception as e:
                logger.warning(f"Failed to drop table {table_name}: {e}")

        # Dispose of the engine to close any connections
        if engine:
            try:
                engine.dispose()
            except Exception as e:
                logger.warning(f"Failed to dispose engine: {e}")


async def do_run_gx_e2e(
    context: CloudDataContext,
    generate_expectations_input: GenerateExpectationsInput,
) -> str:
    suffix = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
    suite_name = f"generate_expectations-{suffix}"

    metric_service = MetricService(context=context)
    tools_manager = AgentToolsManager(
        context=context,
        metric_service=metric_service,
    )
    agent = AssetReviewAgent(
        tools_manager=tools_manager,
        query_runner=QueryRunner(context),
        metric_service=metric_service,
    )

    suite = await agent.arun(
        generate_expectations_input=generate_expectations_input,
        temperature=0.0,
        seed=20241209,
    )
    suite = ExpectationSuite(name=suite_name, expectations=suite.expectations)

    print(suite)

    # Remove the descriptions to work around a temporary bug
    for expectation in suite.expectations:
        expectation.meta = {"notes": expectation.description}
        expectation.description = None

    logger.info(f"Adding suite {suite.name}")
    suite = context.suites.add(suite)
    suite = context.suites.get(suite.name)
    asset = context.data_sources.get(generate_expectations_input.data_source_name).get_asset(
        generate_expectations_input.data_asset_name
    )
    bd = asset.get_batch_definition(generate_expectations_input.batch_definition_name)

    logger.info("Storing validation definition")
    vd = context.validation_definitions.add(
        ValidationDefinition(
            name=suite_name,
            suite=suite,
            data=bd,
        )
    )

    logger.info("Storing checkpoint")
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name=suite_name,
            validation_definitions=[vd],
        )
    )

    logger.info("Running validation definition")
    try:
        result = checkpoint.run(batch_parameters=generate_expectations_input.batch_parameters)
    except StoreBackendError as e:
        # this can happen if we have too many results and so the store backend can't handle it
        # try running again a simpler result format
        logger.warning(f"StoreBackendError: {e}")
        logger.warning("Retrying with a simpler result format")
        result = checkpoint.run(batch_parameters=generate_expectations_input.batch_parameters)
    for run_result in result.run_results.values():
        logger.info(f"Results available at {run_result.result_url}")
    return suite_name


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_e2e_gxcloud() -> None:
    organization_id = os.getenv("GX_CLOUD_ORGANIZATION_ID")
    workspace_id = os.getenv("GX_CLOUD_WORKSPACE_ID")
    logger.info("Getting GX Data Context")
    context = get_context(
        mode="cloud",
        cloud_base_url=os.getenv("GX_CLOUD_BASE_URL"),
        cloud_organization_id=organization_id,
        cloud_workspace_id=workspace_id,
        cloud_access_token=os.getenv("GX_CLOUD_ACCESS_TOKEN"),
    )

    suite_name = await anext(create_snowflake_generate_expectations_suite(context=context))

    suite = context.suites.get(suite_name)
    assert suite.name == suite_name
    assert len(suite.expectations) > 0
