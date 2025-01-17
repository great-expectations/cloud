from __future__ import annotations

import asyncio
import logging
import ssl
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Callable, Final, Protocol

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.exceptions import ChannelClosed, ConnectionClosed

from great_expectations_cloud.agent.exceptions import GXAgentUnrecoverableConnectionError

if TYPE_CHECKING:
    from pika.channel import Channel
    from pika.spec import Basic, BasicProperties

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


@dataclass(frozen=True)
class OnMessagePayload:
    correlation_id: str
    delivery_tag: int
    body: bytes


class OnMessageFn(Protocol):
    """
    Callback invoked when a message is received.
    Accepts a single argument, a payload object and returns None.
    """

    def __call__(self, payload: OnMessagePayload) -> None: ...


class AsyncRabbitMQClient:
    """Configuration for a particular AMQP client library."""

    def __init__(self, url: str):
        self._parameters = self._build_client_parameters(url=url)
        self.should_reconnect = False
        self.was_consuming = False
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._is_unrecoverable = False

    def run(self, queue: str, on_message: OnMessageFn) -> None:
        """Run an async connection to RabbitMQ.

        Args:
            queue: string representing queue to subscribe to
            on_message: callback which will be invoked when a message is received.
        """
        # When Pika receives an incoming message, our method _callback_handler will
        # be invoked. Here we add the user-provided callback to its signature:
        on_message_callback = partial(self._callback_handler, on_message=on_message)
        # We pass the curried on_message_callback and queue name to _on_connection_open,
        # which Pika will invoke after RabbitMQ establishes our connection.
        on_connection_open_callback = partial(
            self._on_connection_open, queue=queue, on_message=on_message_callback
        )
        connection = AsyncioConnection(
            parameters=self._parameters,
            on_open_callback=on_connection_open_callback,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )
        self._connection = connection
        connection.ioloop.run_forever()
        if self._is_unrecoverable:
            raise GXAgentUnrecoverableConnectionError(  # noqa: TRY003
                "AsyncRabbitMQClient has encountered an unrecoverable error."
            )

    def stop(self) -> None:
        """Close the connection to RabbitMQ."""
        if self._connection is None:
            return
        LOGGER.debug("Shutting down the connection to RabbitMQ.")
        if not self._closing:
            self._closing = True

        if self._consuming:
            self._stop_consuming()
        self._connection.ioloop.stop()
        LOGGER.debug("The connection to RabbitMQ has been shut down.")

    def reset(self) -> None:
        """Reset client to allow a restart."""
        LOGGER.debug("Resetting client")
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False

    def nack(self, delivery_tag: int, requeue: bool) -> None:
        """Nack a message, and indicate if it should be requeued.

        Note that this method is not threadsafe, and must be invoked in the main thread.
        """
        if self._channel is not None:
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def get_threadsafe_ack_callback(self, delivery_tag: int) -> Callable[[], None]:
        """Get a callback to ack a message from any thread."""
        if self._channel is None:
            # if the channel is gone, we can't ack
            return lambda: None
        loop = asyncio.get_event_loop()
        ack = partial(
            self._ack_threadsafe,
            channel=self._channel,
            loop=loop,
            delivery_tag=delivery_tag,
        )
        return ack

    def get_threadsafe_nack_callback(
        self, delivery_tag: int, requeue: bool = False
    ) -> Callable[[], None]:
        """Get a callback to nack a message from any thread."""
        if self._channel is None:
            # if the channel is gone, we can't nack
            return lambda: None
        loop = asyncio.get_event_loop()
        nack = partial(
            self._nack_threadsafe,
            channel=self._channel,
            loop=loop,
            delivery_tag=delivery_tag,
            requeue=requeue,
        )
        return nack

    def _ack_threadsafe(self, channel: Channel, delivery_tag: int, loop: AbstractEventLoop) -> None:
        """Ack a message in a threadsafe manner."""
        if channel.is_closed is not True:
            ack = partial(channel.basic_ack, delivery_tag=delivery_tag)
            loop.call_soon_threadsafe(callback=ack)

    def _nack_threadsafe(
        self,
        channel: Channel,
        delivery_tag: int,
        loop: AbstractEventLoop,
        requeue: bool,
    ) -> None:
        """Nack a message in a threadsafe manner."""
        if channel.is_closed is not True:
            nack = partial(channel.basic_nack, delivery_tag=delivery_tag, requeue=requeue)
            loop.call_soon_threadsafe(callback=nack)

    def _callback_handler(
        self,
        channel: Channel,
        method_frame: Basic.Deliver,
        header_frame: BasicProperties,
        body: bytes,
        on_message: OnMessageFn,
    ) -> None:
        """Called by Pika when a message is received."""
        # param on_message is provided by the caller as an argument to AsyncRabbitMQClient.run
        correlation_id = header_frame.correlation_id
        delivery_tag = method_frame.delivery_tag
        payload = OnMessagePayload(
            correlation_id=correlation_id, delivery_tag=delivery_tag, body=body
        )
        return on_message(payload)

    def _start_consuming(self, queue: str, on_message: OnMessageFn, channel: Channel) -> None:
        """Consume from a channel with the on_message callback."""
        LOGGER.debug("Issuing consumer-related RPC commands")
        channel.add_on_cancel_callback(self._on_consumer_canceled)
        # set RabbitMQ prefetch count to equal the max_threads value in the GX Agent's ThreadPoolExecutor
        channel.basic_qos(prefetch_count=1)
        self._consumer_tag = channel.basic_consume(queue=queue, on_message_callback=on_message)

    def _on_consumer_canceled(self, method_frame: Basic.Cancel) -> None:
        """Callback invoked when the broker cancels the client's connection."""
        if self._channel is not None:
            LOGGER.info(
                "Consumer was cancelled remotely, shutting down",
                extra={
                    "method_frame": method_frame,
                },
            )
            self._channel.close()

    def _reconnect(self) -> None:
        """Prepare the client to reconnect."""
        LOGGER.debug("Preparing client to reconnect")
        self.should_reconnect = True
        self.stop()

    def _stop_consuming(self) -> None:
        """Cancel the channel, if it exists."""
        if self._channel is not None:
            LOGGER.debug("Sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(self._consumer_tag, callback=self._on_cancel_ok)

    def _on_cancel_ok(self, _unused_frame: Basic.CancelOk) -> None:
        """Callback invoked after broker confirms cancel."""
        self._consuming = False
        if self._channel is not None:
            LOGGER.debug("RabbitMQ acknowledged the cancellation of the consumer")
            self._channel.close()

    def _on_connection_open(
        self, connection: AsyncioConnection, queue: str, on_message: OnMessageFn
    ) -> None:
        """Callback invoked after the broker opens the connection."""
        LOGGER.debug("Connection to RabbitMQ has been opened")
        on_channel_open = partial(self._on_channel_open, queue=queue, on_message=on_message)
        connection.channel(on_open_callback=on_channel_open)

    def _on_connection_open_error(
        self, _unused_connection: AsyncioConnection, reason: pika.Exception
    ) -> None:
        """Callback invoked when there is an error while opening connection."""
        self._reconnect()
        self._log_pika_exception("Connection open failed", reason)

    def _on_connection_closed(
        self, connection: AsyncioConnection, _unused_reason: pika.Exception
    ) -> None:
        """Callback invoked after the broker closes the connection"""
        LOGGER.debug("Connection to RabbitMQ has been closed")
        self._channel = None
        self._is_unrecoverable = True
        if self._closing:
            connection.ioloop.stop()
        else:
            self._reconnect()

    def _close_connection(self, reason: pika.Exception) -> None:
        """Close the connection to the broker."""
        self._consuming = False
        if self._connection is None or self._connection.is_closing or self._connection.is_closed:
            LOGGER.debug("Connection to RabbitMQ is closing or is already closed")
            pass
        else:
            LOGGER.debug("Closing connection to RabbitMQ")

            if isinstance(reason, (ConnectionClosed, ChannelClosed)):
                reply_code = reason.reply_code
                reply_text = reason.reply_text
            else:
                reply_code = 999  # arbitrary value, not in the list of AMQP reply codes: https://www.rabbitmq.com/amqp-0-9-1-reference#constants
                reply_text = str(reason)
            self._connection.close(reply_code=reply_code, reply_text=reply_text)

    def _log_pika_exception(
        self, message: str, reason: pika.Exception, extra: dict[str, str] | None = None
    ) -> None:
        """Log a pika exception. Extra is key-value pairs to include in the log message."""
        if not extra:
            extra = {}
        if isinstance(reason, (ConnectionClosed, ChannelClosed)):
            default_extra: dict[str, str] = {
                "reply_code": str(reason.reply_code),
                "reply_text": str(reason.reply_text),
            }
            LOGGER.error(
                message,
                # mypy not happy with dict | dict, so we use the dict constructor
                extra={**default_extra, **extra},
            )
        else:
            default_extra = {"reason": str(reason)}
            # mypy not happy with dict | dict, so we use the dict constructor
            LOGGER.error(message, extra={**default_extra, **extra})

    def _on_channel_open(self, channel: Channel, queue: str, on_message: OnMessageFn) -> None:
        """Callback invoked after the broker opens the channel."""
        LOGGER.debug("Channel opened")
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_closed)
        self._start_consuming(queue=queue, on_message=on_message, channel=channel)

    def _on_channel_closed(self, channel: Channel, reason: ChannelClosed) -> None:
        """Callback invoked after the broker closes the channel."""
        self._log_pika_exception("Channel closed", reason, extra={"channel": channel})
        self._close_connection(reason)

    def _build_client_parameters(self, url: str) -> pika.URLParameters:
        """Configure parameters used to connect to the broker."""
        parameters = pika.URLParameters(url)
        # only enable SSL if connection string calls for it
        if url.startswith("amqps://"):
            # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_default_certs()
            ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
            parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        return parameters


class ClientError(Exception): ...
