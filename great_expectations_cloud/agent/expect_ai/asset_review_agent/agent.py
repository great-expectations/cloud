from __future__ import annotations

import logging
from asyncio import gather
from collections.abc import Mapping
from typing import TYPE_CHECKING
from uuid import uuid4

from great_expectations import ExpectationSuite
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolCall,
    ToolMessage,
)
from langchain_core.runnables import RunnableConfig  # noqa: TC002
from langchain_openai import ChatOpenAI
from langgraph.constants import END, START
from langgraph.graph.state import CompiledStateGraph, StateGraph
from langgraph.types import Send
from openai import APIConnectionError, APITimeoutError
from pydantic import BaseModel, ConfigDict

from great_expectations_cloud.agent.expect_ai.asset_review_agent.prompts import (
    EXPECTATION_ASSISTANT_SYSTEM_MESSAGE,
    EXPECTATION_BUILDER_SYSTEM_MESSAGE,
    EXPECTATION_BUILDER_TASK_MESSAGE,
    QUALITY_ISSUE_SUMMARIZER_SYSTEM_MESSAGE,
    QUALITY_ISSUE_SUMMARIZER_TASK_MESSAGE,
)
from great_expectations_cloud.agent.expect_ai.asset_review_agent.state import (
    BatchParameters,
    DataQualityPlan,
    ExpectationBuilderOutput,
    ExpectationBuilderState,
    GenerateExpectationsConfig,
    GenerateExpectationsInput,
    GenerateExpectationsOutput,
    GenerateExpectationsOutputMetrics,
    GenerateExpectationsState,
)
from great_expectations_cloud.agent.expect_ai.config import OPENAI_MODEL
from great_expectations_cloud.agent.expect_ai.exceptions import (
    InvalidResponseTypeError,
    MissingDataQualityPlanError,
)
from great_expectations_cloud.agent.expect_ai.expectations import AddExpectationsResponse
from great_expectations_cloud.agent.expect_ai.graphs.expectation_checker import (
    ExpectationChecker,
    ExpectationCheckerInput,
)
from great_expectations_cloud.agent.expect_ai.nodes.PlannerNode import PlannerNode

if TYPE_CHECKING:
    from great_expectations_cloud.agent.analytics import ExpectAIAnalytics
    from great_expectations_cloud.agent.expect_ai.metric_service import MetricService
    from great_expectations_cloud.agent.expect_ai.tools.metrics import AgentToolsManager
    from great_expectations_cloud.agent.expect_ai.tools.query_runner import QueryRunner

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

MAX_PLAN_DEPTH = 3


ToolCallLike = ToolCall | dict[str, object]


def _toolcall_triplet(tc: ToolCallLike) -> tuple[str, Mapping[str, object], str]:
    name = str(tc.get("name", "")) if isinstance(tc, dict) else str(getattr(tc, "name", ""))
    args_any = (tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})) or {}
    tc_id = str(tc.get("id", "")) if isinstance(tc, dict) else str(getattr(tc, "id", ""))
    if not isinstance(args_any, Mapping):
        args_any = {}
    return name, args_any, tc_id


def _sorted_items_repr(args: Mapping[str, object]) -> str:
    try:
        return str(sorted(args.items()))
    except Exception:
        return "[]"


class AssetReviewAgentResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    expectation_suite: ExpectationSuite
    metrics: GenerateExpectationsOutputMetrics


class AssetReviewAgent:
    def __init__(
        self,
        tools_manager: AgentToolsManager,
        query_runner: QueryRunner,
        metric_service: MetricService,
        analytics: ExpectAIAnalytics | None = None,
    ):
        self._tools_manager = tools_manager
        self._query_runner = query_runner
        self._metric_service = metric_service
        self._analytics = analytics

    async def arun(
        self,
        generate_expectations_input: GenerateExpectationsInput,
        thread_id: str | None = None,
        temperature: float = 0.7,
        seed: int | None = None,
    ) -> AssetReviewAgentResult:
        if thread_id is None:
            thread_id = str(uuid4())

        agent = self._build_agent_graph()

        output = await agent.ainvoke(
            generate_expectations_input,
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "temperature": temperature,
                    "seed": seed,
                },
                "recursion_limit": 30,
            },
        )
        result = GenerateExpectationsOutput(**output)
        suite = ExpectationSuite(
            name=f"generate_expectations--{generate_expectations_input.data_asset_name}",
            expectations=[expectation.get_gx_expectation() for expectation in result.expectations],
        )
        return AssetReviewAgentResult(
            expectation_suite=suite,
            metrics=result.metrics,
        )

    def get_raw_graph_for_langgraph_studio(
        self,
    ) -> CompiledStateGraph[
        GenerateExpectationsState,
        GenerateExpectationsConfig,
        GenerateExpectationsInput,
        GenerateExpectationsOutput,
    ]:
        return self._get_graph_builder().compile()

    def _build_agent_graph(
        self,
    ) -> CompiledStateGraph[
        GenerateExpectationsState,
        GenerateExpectationsConfig,
        GenerateExpectationsInput,
        GenerateExpectationsOutput,
    ]:
        builder = self._get_graph_builder()
        return builder.compile()

    def _get_graph_builder(
        self,
    ) -> StateGraph[
        GenerateExpectationsState,
        GenerateExpectationsConfig,
        GenerateExpectationsInput,
        GenerateExpectationsOutput,
    ]:
        self._expectation_checker_subgraph = ExpectationChecker(
            query_runner=self._query_runner,
            analytics=self._analytics,
        ).graph()

        builder = StateGraph(
            state_schema=GenerateExpectationsState,
            context_schema=GenerateExpectationsConfig,
            input_schema=GenerateExpectationsInput,
            output_schema=GenerateExpectationsOutput,
        )
        builder.add_node(
            "planner",
            PlannerNode(
                tools_manager=self._tools_manager,
                metric_service=self._metric_service,
            ),
        )
        builder.add_node(
            "expectation_assistant",
            ExpectationAssistantNode(tools_manager=self._tools_manager),
        )
        builder.add_node("metric_provider", MetricProviderNode(tools_manager=self._tools_manager))
        builder.add_node("quality_issue_summarizer", QualityIssueSummarizerNode())
        builder.add_node(
            "expectation_builder",
            ExpectationBuilderNode(
                sql_tools_manager=self._query_runner,
                templated_system_message=EXPECTATION_BUILDER_SYSTEM_MESSAGE,
                task_human_message=EXPECTATION_BUILDER_TASK_MESSAGE,
            ),
        )
        builder.add_node("expectation_checker", self._invoke_expectation_checker)
        builder.add_edge(START, "planner")
        builder.add_edge("planner", "expectation_assistant")
        builder.add_conditional_edges(
            "expectation_assistant",
            tools_condition,
            ["metric_provider", "quality_issue_summarizer", "expectation_assistant"],
        )
        builder.add_conditional_edges(
            "quality_issue_summarizer",
            expectation_builder_fanout,
            ["expectation_builder"],
        )
        builder.add_edge("metric_provider", "expectation_assistant")
        builder.add_edge("expectation_builder", "expectation_checker")
        builder.add_edge("expectation_checker", END)
        return builder

    async def _invoke_expectation_checker(
        self, state: GenerateExpectationsState, config: RunnableConfig
    ) -> GenerateExpectationsOutput:
        expectations = []
        for expectation in state.potential_expectations:
            checker_input = ExpectationCheckerInput(
                expectation=expectation,
                data_source_name=state.data_source_name,
                data_asset_name=state.data_asset_name,
            )
            result = await self._expectation_checker_subgraph.ainvoke(checker_input, config=config)
            if result.get("error") is None:
                expectations.append(result["expectation"])

        return GenerateExpectationsOutput(
            expectations=expectations, metrics=state.collected_metrics
        )


class ExpectationAssistantNode:
    def __init__(self, tools_manager: AgentToolsManager):
        self._tools_manager = tools_manager

    async def __call__(
        self, state: GenerateExpectationsState, config: RunnableConfig
    ) -> GenerateExpectationsState:
        """Use the metrics and the plan to generate data quality expectations."""
        logger.debug("Reviewing current metrics and data")
        state.total_turns += 1
        tools = self._tools_manager.get_tools(
            data_source_name=state.data_source_name,
        )
        tools_model = ChatOpenAI(
            model_name=OPENAI_MODEL,
            temperature=config["configurable"].get("temperature", 0.2),
            seed=config["configurable"].get("seed", None),
            request_timeout=120,
        ).bind_tools(tools=tools, strict=True)

        remaining_batches = max(0, MAX_PLAN_DEPTH - state.metric_batches_executed)
        assistant_system = EXPECTATION_ASSISTANT_SYSTEM_MESSAGE + (
            f"\n\nREMAINING_METRIC_BATCHES: {remaining_batches}. "
            "If 0, emit the complete Data Quality Plan now and DO NOT emit any tool_calls."
        )
        messages_for_invocation = [SystemMessage(content=assistant_system), *state.messages]

        response = tools_model.with_retry(
            retry_if_exception_type=(APIConnectionError, APITimeoutError),
            stop_after_attempt=2,
        ).invoke(messages_for_invocation)

        state.messages.append(response)
        if isinstance(response, AIMessage):
            # Drop tool_calls if no batches remain
            raw_tool_calls: list[ToolCall] = list(response.tool_calls or [])
            if remaining_batches == 0 and len(raw_tool_calls) > 0:
                response.tool_calls = []
                state.planned_tool_calls = 0
                return state
            # De-duplicate planned tool calls against executed signatures in a single pass
            filtered_raw: list[ToolCall] = []
            for tc_raw in raw_tool_calls:
                name, args, _ = _toolcall_triplet(tc_raw)
                column = args.get("column", "")
                signature = f"{name}:{column}:{_sorted_items_repr(args)}"
                if signature not in state.executed_tool_signatures:
                    filtered_raw.append(tc_raw)
            response.tool_calls = filtered_raw
            state.planned_tool_calls = len(filtered_raw)
        return state


class MetricProviderNode:
    def __init__(self, tools_manager: AgentToolsManager):
        self._tools_manager = tools_manager

    async def __call__(
        self, state: GenerateExpectationsState, config: RunnableConfig
    ) -> dict[str, list[BaseMessage]]:
        """Call the tools to get the metrics."""
        logger.debug("Getting additional metrics")
        state.total_turns += 1
        result: list[BaseMessage] = []
        if not isinstance(state.messages[-1], AIMessage):
            return {"messages": result}

        # Work around bug in Langgraph Studio for serialization.
        # This is a temporary fix until the bug is resolved.
        if isinstance(state.batch_parameters, dict):
            state.batch_parameters = BatchParameters(**state.batch_parameters)

        tools_by_name = {
            tool.name: tool
            for tool in self._tools_manager.get_tools(
                data_source_name=state.data_source_name,
            )
        }

        # Execute all tool calls concurrently for speed
        async def _run_tool(tool_call: ToolCall) -> ToolMessage:
            tool_name, tool_args, tool_id = _toolcall_triplet(tool_call)
            tool = tools_by_name[tool_name]
            args = {
                "batch_definition": state.batch_definition,
                "batch_parameters": state.batch_parameters,
                **tool_args,
            }
            logger.debug(f"Getting metric {tool.name}: {tool_args!s}")
            observation = tool.func(**args) if tool.func is not None else None
            if observation is None:
                observation = "METRIC FAILED\nMetric: {tool.name}\nError: tool has no function"
            if "Could not compute metric" in str(observation):
                column_name = tool_args.get("column", "unknown")
                metric_name = tool.name
                error_text = str(observation)[:400]
                observation = (
                    f"METRIC FAILED\n"
                    f"Metric: {metric_name}\n"
                    f"Column: {column_name}\n"
                    f"Error: {error_text}\n"
                    f"Action: Analyze this error and try a different metric approach."
                )
            if isinstance(observation, list):
                observation = "\n".join(str(item) for item in observation)
            # Record signature to prevent duplicates later
            column = tool_args.get("column", "")
            signature = f"{tool_name}:{column}:{_sorted_items_repr(tool_args)}"
            state.executed_tool_signatures.add(signature)
            return ToolMessage(content=str(observation), tool_call_id=tool_id)

        tool_msgs = await gather(*(_run_tool(tc) for tc in (state.messages[-1].tool_calls or [])))
        result.extend(tool_msgs)
        # track execution count
        state.executed_tool_calls = state.executed_tool_calls + len(tool_msgs)
        # increment batch count after one assistant batch handled
        state.metric_batches_executed += 1
        result.append(
            HumanMessage(
                "Here is the info you asked for. Please review all of these responses and use tools to obtain additional details unless you have all the possible metrics that would be needed to define data quality checks."
            )
        )
        return {"messages": result}


class QualityIssueSummarizerNode:
    async def __call__(
        self, state: GenerateExpectationsState, config: RunnableConfig
    ) -> dict[str, DataQualityPlan]:
        logger.debug("Building a data quality plan")
        task = HumanMessage(content=QUALITY_ISSUE_SUMMARIZER_TASK_MESSAGE)

        messages = [
            SystemMessage(content=QUALITY_ISSUE_SUMMARIZER_SYSTEM_MESSAGE),
            *state.messages,
            task,
        ]

        model = ChatOpenAI(
            model_name=OPENAI_MODEL,
            temperature=config["configurable"].get("temperature", 0.3),
            seed=config["configurable"].get("seed", None),
            request_timeout=120,
        ).with_structured_output(schema=DataQualityPlan, method="json_schema", strict=True)
        data_quality_plan = await model.with_retry(
            retry_if_exception_type=(APIConnectionError, APITimeoutError),
            stop_after_attempt=2,
        ).ainvoke(messages)
        if not isinstance(data_quality_plan, DataQualityPlan):
            raise InvalidResponseTypeError(type(data_quality_plan), DataQualityPlan)

        return {"data_quality_plan": data_quality_plan}


class ExpectationBuilderNode:
    def __init__(
        self,
        sql_tools_manager: QueryRunner,
        templated_system_message: str,
        task_human_message: str,
    ):
        self._sql_tools_manager = sql_tools_manager
        self._templated_system_message = templated_system_message
        self._task_human_message = task_human_message

    async def __call__(
        self, state: ExpectationBuilderState, config: RunnableConfig
    ) -> ExpectationBuilderOutput:
        """Use the metrics and the plan to generate data quality expectations."""
        plan_component = state.plan_component
        logger.debug("Building expectations for data quality plan component")
        structured_output_model = ChatOpenAI(
            model_name=OPENAI_MODEL,
            temperature=config["configurable"].get("temperature", 0.3),
            seed=config["configurable"].get("seed", None),
            request_timeout=60,
        ).with_structured_output(schema=AddExpectationsResponse, method="json_schema", strict=True)
        task_msg = HumanMessage(content=self._task_human_message)
        messages = [
            SystemMessage(
                content=self._templated_system_message.format(
                    dialect=self._sql_tools_manager.get_dialect(
                        data_source_name=state.data_source_name
                    )
                )
            ),
            HumanMessage(content=f"Issue: {plan_component.title}\n\n{plan_component.plan_details}"),
            task_msg,
        ]
        if len(state.existing_expectation_contexts) > 0:
            context_text = "\n".join(
                [
                    f"- {ctx.expectation_type} on {ctx.domain}: {ctx.description}"
                    for ctx in state.existing_expectation_contexts
                ]
            )
            messages.append(
                HumanMessage(
                    content=f"The following Expectations already exist, so do not redundantly generate them:\n{context_text}"
                )
            )
        # Structured output model seems very sensitive to a long message history.
        # So, it needs to be summarized or truncated first.
        response = await structured_output_model.with_retry(
            retry_if_exception_type=(APIConnectionError, APITimeoutError),
            stop_after_attempt=2,
        ).ainvoke(messages)
        if not isinstance(response, AddExpectationsResponse):
            raise InvalidResponseTypeError(type(response), AddExpectationsResponse)

        return ExpectationBuilderOutput(
            potential_expectations=response.expectations,
            data_source_name=state.data_source_name,
        )


##############################
# Conditional Edges
##############################


def tools_condition(state: GenerateExpectationsState) -> str:
    """Condition for whether to call the tools."""
    ai_message = state.messages[-1]
    remaining_batches = max(0, MAX_PLAN_DEPTH - state.metric_batches_executed)
    if (
        isinstance(ai_message, AIMessage)
        and len(ai_message.tool_calls) > 0
        and remaining_batches > 0
    ):
        return "metric_provider"
    # Otherwise, proceed to summarizer to avoid assistant self-loops
    return "quality_issue_summarizer"


def expectation_builder_fanout(state: GenerateExpectationsState) -> list[Send]:
    if state.data_quality_plan is None:
        raise MissingDataQualityPlanError()
    return [
        Send(
            "expectation_builder",
            ExpectationBuilderState(
                plan_component=component,
                plan_development_messages=state.messages,
                data_source_name=state.data_source_name,
                existing_expectation_contexts=state.existing_expectation_contexts,
            ),
        )
        for component in state.data_quality_plan.components
    ]
