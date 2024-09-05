from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Final

from faststream import Context, FastStream
from faststream.rabbit import RabbitBroker
from typing_extensions import (
    Annotated,  # noqa: TCH002 - WARNING: This is used for a type hint, but pydantic will fail if not imported this way
)

from great_expectations_cloud.agent.agent import GXAgent, agent_instance
from great_expectations_cloud.agent.config import GXAgentConfig, GXAgentConfigError
from great_expectations_cloud.agent.queue import declare_queue

if TYPE_CHECKING:
    from fast_depends import Depends

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

# Note: faststream requires module level variables for its testing implementation
config = GXAgentConfig.build()
broker = RabbitBroker(url=str(config.connection_string))
app = FastStream(broker)
queue = declare_queue(config)


@broker.subscriber(queue, retry=1)  # type: ignore[arg-type]
# Type ignored because we want to pass in the declared queue which had retry logic
async def handle(
    msg: dict[str, Any],
    gx_agent: Annotated[GXAgent, Depends(agent_instance)],
    correlation_id: str = Context("message.correlation_id"),
) -> None:
    print(f"Received: {msg}")
    print(f"Correlation ID: {correlation_id}")
    gx_agent._handle_message(msg, correlation_id)


def run_agent() -> None:
    """Run an instance of the GX Agent."""
    try:
        GXAgent(config=config)
        asyncio.run(app.run())
        print("The connection to GX Cloud has been closed.")
    except GXAgentConfigError as error:
        # catch error to avoid stacktrace printout
        LOGGER.error(error)  # noqa: TRY400 # intentionally avoiding logging stacktrace


def get_version() -> str:
    return GXAgent.get_current_gx_agent_version()
