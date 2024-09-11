from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Final

import aiormq
from faststream.rabbit import RabbitQueue
from pika.exceptions import AuthenticationError, ProbableAuthenticationError
from tenacity import after_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

if TYPE_CHECKING:
    from great_expectations_cloud.agent.config import BaseConfig

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
# TODO Set in log dict
LOGGER.setLevel(logging.INFO)


def _refresh_config(config: BaseConfig) -> None:
    """
    Refresh the configuration with new credentials.
    """
    new_config = config.build()
    # Note: utilizing modification of config object to avoid use global variables in functions
    config.queue = new_config.queue
    config.connection_string = new_config.connection_string


# ZEL-505: A race condition can occur if two or more agents are started at the same time
#          due to the generation of passwords for rabbitMQ queues. This can be mitigated
#          by adding a delay and retrying the connection. Retrying with new credentials
#          requires calling get_config again, which handles the password generation.
# Note: This is not the number of retries of processing a message, it's the number of retries
#       to establish a connection to the broker.
@retry(
    retry=retry_if_exception_type((AuthenticationError, ProbableAuthenticationError)),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(3),
    after=after_log(LOGGER, logging.DEBUG),
)
def declare_queue(config: BaseConfig) -> RabbitQueue | None:
    """Manage connection lifecycle."""
    queue = None
    try:
        queue = RabbitQueue(name=config.queue, durable=True, passive=True)
    except KeyboardInterrupt:
        print("Received request to shut down.")
    except (
        asyncio.exceptions.CancelledError,
        aiormq.exceptions.ChannelAccessRefused,
    ):
        print("The connection to GX Cloud has encountered an error.")
        LOGGER.exception("agent.declare_queue.error")
    except (AuthenticationError, ProbableAuthenticationError):
        # Retry with new credentials
        _refresh_config(config)
        # Raise to use the retry decorator to handle the retry logic
        raise

    return queue
