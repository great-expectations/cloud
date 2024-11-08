from __future__ import annotations

import asyncio
import time
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Protocol

from pika.exceptions import (
    AMQPError,
    AuthenticationError,
    ChannelError,
)

from great_expectations_cloud.agent.constants import JOB_TIMEOUT_IN_SECONDS
from great_expectations_cloud.agent.event_handler import EventHandler
from great_expectations_cloud.agent.exceptions import GXAgentUnrecoverableConnectionError

if TYPE_CHECKING:
    from great_expectations_cloud.agent.message_service.asyncio_rabbit_mq_client import (
        AsyncRabbitMQClient,
        OnMessagePayload,
    )
    from great_expectations_cloud.agent.models import Event


@dataclass(frozen=True)
class EventContext:
    """An Event with related properties and actions.

    Attributes:
        event: Pydantic model of type Event
        correlation_id: stable identifier for this Event over its lifecycle
        processed_successfully: callable to signal that the event was
            processed successfully and can be removed from the queue.
        processed_with_failures: callable to signal that processing failed and
            can be removed from the queue.
        redeliver_message: async callable to signal that the broker should
            try to deliver this message again.
    """

    event: Event
    correlation_id: str
    processed_successfully: Callable[[], None]
    processed_with_failures: Callable[[], None]
    redeliver_message: Callable[[], Coroutine[OnMessageCallback, None, None]]


class OnMessageCallback(Protocol):
    """Callback for handling incoming messages."""

    def __call__(self, event_context: EventContext) -> None:
        """Handle an incoming message."""


class Subscriber:
    """Manage an open connection to an event stream."""

    # abstraction between the main application and client serving a specific stream

    def __init__(self, client: AsyncRabbitMQClient):
        """Initialize instance of Subscriber.

        Args:
            client: RabbitMQClient class.
        """
        self.client = client
        self._reconnect_delay = 0

    # TODO: Consider using tenacity to handle reconnecting
    def consume(
        self,
        queue: str,
        on_message: OnMessageCallback,
        current_task: Future[Any] | None = None,
        current_task_started_at: int | None = None,
    ) -> None:
        """Subscribe to queue with on_message callback.

        Listens to an event stream and invokes on_message with an EventContext
        built from the incoming message.

        Args:
            queue: Name of queue.
            on_message: Callback to be invoked with incoming messages.
        """
        callback = partial(self._on_message_handler, on_message=on_message)

        while True:
            try:
                print("In consume before client.run")
                self.client.run(queue=queue, on_message=callback)
                print("In consume after client.run")
                # TODO: Is this a good place to check if the task has taken too long?
                if current_task:
                    if int(time.time()) - current_task_started_at > JOB_TIMEOUT_IN_SECONDS:
                        # TODO: Do we need to do more than just cancel?
                        print("Cancelling task")
                        current_task.cancel()
                        print("Cancelled task")
                        # TODO: What should exception message be?
                        raise TimeoutError("Task took too long to complete.")  # noqa: TRY003 # just for debugging - remove before merge

            except AuthenticationError:
                # If an authentication error happens when trying to connect to rabbitMQ,
                # it means that the connection string is incorrect. Retrying would not
                # enable us to reconnect.
                self.client.stop()
                raise
            except (AMQPError, ChannelError):
                self.client.stop()
                reconnect_delay = self._get_reconnect_delay()
                time.sleep(reconnect_delay)  # todo: update this blocking call to asyncio.sleep
                raise
            except KeyboardInterrupt as e:
                self.client.stop()
                raise KeyboardInterrupt from e
            except GXAgentUnrecoverableConnectionError:
                self.client.stop()
                raise
            if self.client.should_reconnect:
                self.client.reset()
            else:
                break  # exit

    def _on_message_handler(
        self,
        payload: OnMessagePayload,
        on_message: OnMessageCallback,
    ) -> None:
        """Called by the client when a message is received.

        Translate message into a known model and pass results into on_message callback.

        Args:
            payload: dataclass containing required message attributes
            on_message: the caller-provided callback
        """
        event = EventHandler.parse_event_from(payload.body)

        # Allow the caller to determine whether to ack/nack this message,
        # even if the processing occurs in another thread.
        ack_callback = self.client.get_threadsafe_ack_callback(delivery_tag=payload.delivery_tag)
        nack_callback = self.client.get_threadsafe_nack_callback(
            delivery_tag=payload.delivery_tag, requeue=False
        )
        # redeliver_message is not threadsafe
        redeliver_message = partial(
            self._redeliver_message,
            delivery_tag=payload.delivery_tag,
            requeue=True,
            delay=3,
        )

        event_context = EventContext(
            event=event,
            correlation_id=payload.correlation_id,
            processed_successfully=ack_callback,
            processed_with_failures=nack_callback,
            redeliver_message=redeliver_message,
        )

        return on_message(event_context)

    async def _redeliver_message(
        self,
        delivery_tag: int,
        requeue: bool = True,
        delay: float | int = 3,  # noqa: PYI041
    ) -> None:
        """Coroutine to request a redelivery with delay."""
        # not threadsafe
        await asyncio.sleep(delay)
        return self.client.nack(delivery_tag=delivery_tag, requeue=requeue)

    def _get_reconnect_delay(self) -> int:
        """Get a timeout delay with a 1 second backoff for each attempt."""
        if self.client.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        self._reconnect_delay = min(self._reconnect_delay, 30)
        return self._reconnect_delay

    def close(self) -> None:
        """Gracefully closes the Subscriber's connection.

        Must be called after the Subscriber disconnects."""
        self.client.stop()


class SubscriberError(Exception): ...
