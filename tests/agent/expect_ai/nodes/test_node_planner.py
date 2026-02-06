from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NamedTuple

import pandas as pd
import pytest
from great_expectations.datasource.fluent.interfaces import Batch, BatchDefinition
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset
from great_expectations.metrics.batch.batch_column_types import (
    BatchColumnTypesResult,
    ColumnType,
)
from great_expectations.metrics.batch.sample_values import SampleValuesResult
from great_expectations.validator.metric_configuration import MetricConfigurationID
from langchain_core.runnables import RunnableConfig

from great_expectations_cloud.agent.expect_ai.nodes.PlannerNode import (
    PlannerNode,
    sample_values_metric_to_ai_readable_message,
    schema_metric_to_csv_string,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


# Test data constants
TEST_TABLE_NAME = "test_table"
TEST_DATA_SOURCE_NAME = "test_source"
TEST_ASSET_NAME = "test_asset"
TEST_BATCH_NAME = "test_batch"
TEST_ORG_ID = "test_org"

# Schema constants
SCHEMA_CSV = "column_name,column_type\nID,NUMBER(38,0)\nNAME,VARCHAR(16777216)"

# Sample data constants
SAMPLE_DF = pd.DataFrame(
    {
        "ID": [1, 2, 3],
        "NAME": [
            "Acme Corp",
            "Widget Co",
            "Global Industries",
        ],
    }
)
SAMPLE_CSV = "ID,NAME\n1,Acme Corp\n2,Widget Co\n3,Global Industries"

# Batch parameter constants
BATCH_PARAMETERS = [{"param1": "value1"}]

# All supported SQL dialects for testing
SQL_DIALECTS = ["snowflake", "postgresql", "redshift", "databricks"]


class MockMetricResults(NamedTuple):
    """Contains mock metric computation results."""

    schema_result: BatchColumnTypesResult
    sample_values_result: SampleValuesResult


@dataclass
class PlannerTestContext:
    """Container for test dependencies with helper methods for verification."""

    mock_tools_manager: Any
    mock_metric_service: Any
    mock_sql_datasource: SQLDatasource
    mock_gx_asset: TableAsset
    mock_gx_batch: Batch
    schema_result: BatchColumnTypesResult
    sample_values_result: SampleValuesResult
    test_state: Any

    def create_planner(self) -> PlannerNode:
        """Create a PlannerNode with the context's mocks."""
        return PlannerNode(
            tools_manager=self.mock_tools_manager,
            metric_service=self.mock_metric_service,
        )

    def verify_result(self, result) -> None:
        """Verify the common assertions about a PlannerNode result."""
        assert result.organization_id == self.test_state.organization_id
        assert result.data_source_name == self.test_state.data_source_name
        assert result.data_asset_name == self.test_state.data_asset_name
        assert result.batch_definition_name == self.test_state.batch_definition_name
        assert result.batch_parameters is None
        assert len(result.messages) == 2  # Schema + sample values (no system message)

    def verify_dialect_in_message(self, result, dialect_name: str) -> None:
        """Verify that the SQL dialect appears in the message."""
        assert f"SQL dialect: {dialect_name}" in result.messages[0].content

    def verify_message_content(self, result) -> None:
        """Verify that the expected content appears in the messages."""
        assert f"Table name: {self.mock_gx_asset.table_name}" in result.messages[0].content
        assert "Table schema in CSV format with header" in result.messages[0].content
        assert "Table sample values in CSV format with header" in result.messages[1].content


@dataclass
class PlannerTestBuilder:
    """Builder class for creating PlannerNode test contexts with customizable components."""

    mocker: MockerFixture
    dialect_name: str = "snowflake"
    table_name: str = TEST_TABLE_NAME

    # Use Any type for mocks to avoid type checking issues with fields initialized as None
    _sql_datasource: Any = field(default=None, init=False)
    _gx_asset: Any = field(default=None, init=False)
    _gx_batch: Any = field(default=None, init=False)
    _metric_results: Any = field(default=None, init=False)
    _test_state: Any = field(default=None, init=False)

    def with_dialect(self, dialect_name: str) -> PlannerTestBuilder:
        """Set the SQL dialect to use for this test."""
        self.dialect_name = dialect_name
        return self

    def with_table_name(self, table_name: str) -> PlannerTestBuilder:
        """Set the table name to use for this test."""
        self.table_name = table_name
        return self

    def build(self) -> PlannerTestContext:
        """Build and return a complete test context with all mocks configured."""
        # ARRANGE - Create the mocks in the right order
        self._create_datasource()
        self._create_asset()
        self._create_batch()
        self._create_metric_results()
        self._create_test_state()

        # Create mock tools and services
        tools_manager = self.mocker.Mock()
        metric_service = self.mocker.Mock()

        # Configure mock behavior
        self._configure_mock_services(metric_service)

        # Return the fully configured test context
        return PlannerTestContext(
            mock_tools_manager=tools_manager,
            mock_metric_service=metric_service,
            mock_sql_datasource=self._sql_datasource,
            mock_gx_asset=self._gx_asset,
            mock_gx_batch=self._gx_batch,
            schema_result=self._metric_results.schema_result,
            sample_values_result=self._metric_results.sample_values_result,
            test_state=self._test_state,
        )

    def _create_datasource(self) -> None:
        """Create and configure the mock SQL datasource."""
        # Create autospec creates a mock that behaves like the real object
        # so it will have all return_value attributes that mypy can't see
        mock = self.mocker.create_autospec(SQLDatasource)
        mock_engine = self.mocker.Mock()
        mock_engine.dialect.name = self.dialect_name
        mock.get_execution_engine.return_value = mock_engine
        self._sql_datasource = mock

    def _create_asset(self) -> None:
        """Create and configure the mock table asset."""
        mock = self.mocker.create_autospec(TableAsset, table_name=self.table_name)
        self._gx_asset = mock

    def _create_batch(self) -> None:
        """Create the mock batch and batch definition."""
        mock_batch = self.mocker.create_autospec(Batch)
        mock_batch_def = self.mocker.create_autospec(BatchDefinition[Any])
        mock_batch_def.get_batch.return_value = mock_batch

        # Connect the asset to the batch definition
        # No need for type assertion since we're using Any type for fields
        self._gx_asset.get_batch_definition.return_value = mock_batch_def
        self._gx_batch = mock_batch

    def _create_metric_results(self) -> None:
        """Create the metric results that will be returned by the batch."""
        metric_id = MetricConfigurationID(
            metric_name="",
            metric_domain_kwargs_id=(),
            metric_value_kwargs_id=(),
        )

        schema_result = BatchColumnTypesResult(
            id=metric_id,
            value=[
                ColumnType(name="id", type="NUMBER(38,0)"),
                ColumnType(name="name", type="VARCHAR(16777216)"),
            ],
        )

        sample_values_result = SampleValuesResult(
            id=metric_id,
            value=SAMPLE_DF,
        )

        # Configure the batch to return these results
        # No need for type assertion since we're using Any type for fields
        self._gx_batch.compute_metrics.return_value = (
            schema_result,
            sample_values_result,
        )

        self._metric_results = MockMetricResults(
            schema_result=schema_result,
            sample_values_result=sample_values_result,
        )

    def _create_test_state(self) -> None:
        """Create the input state for the PlannerNode."""
        state = self.mocker.Mock()
        state.data_source_name = TEST_DATA_SOURCE_NAME
        state.data_asset_name = TEST_ASSET_NAME
        state.batch_definition_name = TEST_BATCH_NAME
        state.batch_parameters = None
        state.organization_id = TEST_ORG_ID
        state.existing_expectation_contexts = []
        self._test_state = state

    def _configure_mock_services(self, metric_service) -> None:
        """Configure the metric service with appropriate returns and side effects."""
        # Configure controller and its side effects
        mock_controller = self.mocker.Mock()
        mock_controller.get_metric.return_value = self.mocker.Mock(as_csv=lambda: SCHEMA_CSV)
        mock_controller.get_metric.side_effect = [
            self.mocker.Mock(batch_parameters=BATCH_PARAMETERS),
            self.mocker.Mock(as_csv=lambda: SCHEMA_CSV),
            self.mocker.Mock(as_csv=lambda: SAMPLE_CSV),
        ]

        # Connect services to datasource and asset
        metric_service.get_data_source.return_value = self._sql_datasource
        # No need for type assertion since we're using Any type for fields
        self._sql_datasource.get_asset.return_value = self._gx_asset


@pytest.fixture
def test_builder(mocker: MockerFixture) -> PlannerTestBuilder:
    """Create a PlannerTestBuilder for flexible test setup."""
    return PlannerTestBuilder(mocker=mocker)


class TestPlannerNode:
    @pytest.mark.unit
    @pytest.mark.asyncio
    @pytest.mark.parametrize("dialect_name", SQL_DIALECTS)
    async def test_updates_state_with_metrics(
        self,
        test_builder: PlannerTestBuilder,
        dialect_name: str,
    ) -> None:
        """Test that the PlannerNode correctly updates state with metrics for different SQL dialects."""
        # ARRANGE
        # Build the test context with the specified SQL dialect
        context = test_builder.with_dialect(dialect_name).build()
        planner = context.create_planner()

        # ACT
        # Execute the planner node with the test state
        result = await planner(state=context.test_state, config=RunnableConfig())

        # ASSERT
        # Verify the planner node properly updates the state with metrics
        context.verify_result(result)
        context.verify_dialect_in_message(result, dialect_name)
        context.verify_message_content(result)


class TestSchemaMetricToCsvString:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "column_inputs, expected_csv_output_lines",
        [
            (  # All lowercase
                [
                    ColumnType(name="id", type="NUMBER(38,0)"),
                    ColumnType(name="name", type="VARCHAR(16777216)"),
                ],
                [
                    "column_name,column_type",
                    "id,NUMBER(38,0)",
                    "name,VARCHAR(16777216)",
                ],
            ),
            (  # All uppercase
                [
                    ColumnType(name="ID", type="NUMBER(38,0)"),
                    ColumnType(name="NAME", type="VARCHAR(16777216)"),
                ],
                [
                    "column_name,column_type",
                    "ID,NUMBER(38,0)",
                    "NAME,VARCHAR(16777216)",
                ],
            ),
            (  # Mixed case
                [
                    ColumnType(name="ProductId", type="INT"),
                    ColumnType(name="customerName", type="TEXT"),
                ],
                [
                    "column_name,column_type",
                    "ProductId,INT",
                    "customerName,TEXT",
                ],
            ),
        ],
    )
    def test_maps_correctly(
        self,
        mocker: MockerFixture,
        column_inputs: list[ColumnType],
        expected_csv_output_lines: list[str],
    ) -> None:
        """Test that schema metrics are correctly converted to CSV format."""
        # ARRANGE
        metric_id = MetricConfigurationID(
            metric_name="",
            metric_domain_kwargs_id=(),
            metric_value_kwargs_id=(),
        )

        metric_result = BatchColumnTypesResult(
            id=metric_id,
            value=column_inputs,
        )

        # ACT
        output = schema_metric_to_csv_string(metric_result)

        # ASSERT
        assert output == "\n".join(expected_csv_output_lines)

    @pytest.mark.unit
    def test_handles_empty_list(self, mocker: MockerFixture) -> None:
        """Test that empty schema metrics are handled correctly."""
        # ARRANGE
        metric_id = MetricConfigurationID(
            metric_name="",
            metric_domain_kwargs_id=(),
            metric_value_kwargs_id=(),
        )

        metric_result = BatchColumnTypesResult(
            id=metric_id,
            value=[],
        )

        # ACT
        output = schema_metric_to_csv_string(metric_result)

        # ASSERT
        assert output == "column_name,column_type"


class TestSampleValuesMetricToAiReadableMessage:
    @pytest.mark.unit
    def test_maps_correctly(self, mocker: MockerFixture) -> None:
        """Test that sample values are correctly converted to a readable format."""
        # ARRANGE
        metric_id = MetricConfigurationID(
            metric_name="",
            metric_domain_kwargs_id=(),
            metric_value_kwargs_id=(),
        )

        metric_result = SampleValuesResult(
            id=metric_id,
            value=pd.DataFrame(
                {
                    "ID": [1, 2, 3],
                    "NAME": [
                        "Acme Corp",
                        "Widget Co",
                        "Global Industries",
                    ],
                }
            ),
        )

        # ACT
        output = sample_values_metric_to_ai_readable_message(metric_result)

        # ASSERT
        assert output == "\n".join(
            [
                "ID,NAME",
                "1,Acme Corp",
                "2,Widget Co",
                "3,Global Industries",
            ]
        )

    @pytest.mark.unit
    def test_handles_empty_dataframe(self, mocker: MockerFixture) -> None:
        """Test that empty DataFrames are handled correctly."""
        # ARRANGE
        metric_id = MetricConfigurationID(
            metric_name="",
            metric_domain_kwargs_id=(),
            metric_value_kwargs_id=(),
        )

        metric_result = SampleValuesResult(
            id=metric_id,
            value=pd.DataFrame(),
        )

        # ACT
        output = sample_values_metric_to_ai_readable_message(metric_result)

        # ASSERT
        assert output == ""
