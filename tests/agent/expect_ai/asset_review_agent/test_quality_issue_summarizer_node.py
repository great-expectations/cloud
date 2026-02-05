from __future__ import annotations

from unittest.mock import AsyncMock, Mock, create_autospec, patch

import pytest
from great_expectations.core.batch_definition import BatchDefinition
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from openai import APIConnectionError, APITimeoutError

from great_expectations_cloud.agent.expect_ai.asset_review_agent.agent import (
    QualityIssueSummarizerNode,
)
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    DataQualityPlan,
    DataQualityPlanComponent,
    GenerateExpectationsState,
)
from great_expectations_cloud.agent.expect_ai.exceptions import InvalidResponseTypeError


class TestQualityIssueSummarizerNodeCall:
    @pytest.fixture
    def quality_summarizer_node(self) -> QualityIssueSummarizerNode:
        return QualityIssueSummarizerNode()

    @pytest.fixture
    def sample_state(self) -> GenerateExpectationsState:
        return GenerateExpectationsState(
            organization_id="test_org",
            data_source_name="test_datasource",
            data_asset_name="test_asset",
            batch_definition_name="test_batch_def",
            batch_definition=create_autospec(BatchDefinition, instance=True),
            messages=[
                HumanMessage(content="Previous metric data"),
            ],
            potential_expectations=[],
            expectations=[],
        )

    @pytest.fixture
    def mock_config(self) -> RunnableConfig:
        return RunnableConfig(
            configurable={
                "temperature": 0.5,
                "seed": 123,
            }
        )

    @pytest.fixture
    def mock_data_quality_plan(self) -> DataQualityPlan:
        return DataQualityPlan(
            components=[
                DataQualityPlanComponent(
                    title="Component 1",
                    plan_details="Details 1",
                ),
                DataQualityPlanComponent(
                    title="Component 2",
                    plan_details="Details 2",
                ),
            ]
        )

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_success(
        self,
        quality_summarizer_node: QualityIssueSummarizerNode,
        sample_state: GenerateExpectationsState,
        mock_config: RunnableConfig,
        mock_data_quality_plan: DataQualityPlan,
    ) -> None:
        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat_class:
            mock_model = Mock()
            mock_model.with_structured_output.return_value = mock_model
            mock_model.with_retry.return_value = mock_model
            mock_model.ainvoke = AsyncMock(return_value=mock_data_quality_plan)
            mock_chat_class.return_value = mock_model

            result = await quality_summarizer_node(sample_state, mock_config)

            mock_chat_class.assert_called_once_with(
                model_name="gpt-4o-2024-11-20",
                temperature=0.5,  # From config
                seed=123,  # From config
                request_timeout=120,
            )

            mock_model.with_structured_output.assert_called_once_with(
                schema=DataQualityPlan,
                method="json_schema",
                strict=True,
            )

            mock_model.with_retry.assert_called_once()
            retry_kwargs = mock_model.with_retry.call_args[1]
            assert retry_kwargs["retry_if_exception_type"] == (APIConnectionError, APITimeoutError)
            assert retry_kwargs["stop_after_attempt"] == 2

            assert "data_quality_plan" in result
            assert result["data_quality_plan"] == mock_data_quality_plan

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_with_system_message(
        self,
        quality_summarizer_node: QualityIssueSummarizerNode,
        sample_state: GenerateExpectationsState,
        mock_config: RunnableConfig,
        mock_data_quality_plan: DataQualityPlan,
    ) -> None:
        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat_class:
            mock_model = Mock()
            mock_model.with_structured_output.return_value = mock_model
            mock_model.with_retry.return_value = mock_model
            mock_model.ainvoke = AsyncMock(return_value=mock_data_quality_plan)
            mock_chat_class.return_value = mock_model

            await quality_summarizer_node(sample_state, mock_config)

            invoke_args = mock_model.ainvoke.call_args[0][0]
            assert len(invoke_args) == 3  # System + state messages + task
            assert isinstance(invoke_args[0], SystemMessage)
            assert "Quality Issue Summarizer" in invoke_args[0].content
            assert invoke_args[1] == sample_state.messages[0]
            assert isinstance(invoke_args[2], HumanMessage)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_with_default_temperature(
        self,
        quality_summarizer_node: QualityIssueSummarizerNode,
        sample_state: GenerateExpectationsState,
        mock_data_quality_plan: DataQualityPlan,
    ) -> None:
        mock_config = RunnableConfig(configurable={})

        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat_class:
            mock_model = Mock()
            mock_model.with_structured_output.return_value = mock_model
            mock_model.with_retry.return_value = mock_model
            mock_model.ainvoke = AsyncMock(return_value=mock_data_quality_plan)
            mock_chat_class.return_value = mock_model

            await quality_summarizer_node(sample_state, mock_config)

            mock_chat_class.assert_called_once_with(
                model_name="gpt-4o-2024-11-20",
                temperature=0.3,  # Default
                seed=None,  # Default
                request_timeout=120,
            )

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_call_invalid_response_type(
        self,
        quality_summarizer_node: QualityIssueSummarizerNode,
        sample_state: GenerateExpectationsState,
        mock_config: RunnableConfig,
    ) -> None:
        with patch(
            "great_expectations_cloud.agent.expect_ai.asset_review_agent.agent.ChatOpenAI"
        ) as mock_chat_class:
            mock_model = Mock()
            mock_model.with_structured_output.return_value = mock_model
            mock_model.with_retry.return_value = mock_model
            mock_model.ainvoke = AsyncMock(return_value="Invalid response")
            mock_chat_class.return_value = mock_model

            with pytest.raises(InvalidResponseTypeError):
                await quality_summarizer_node(sample_state, mock_config)
