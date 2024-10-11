from __future__ import annotations

import asyncio
import logging
import sys
import traceback
import warnings
from collections import defaultdict
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from importlib.metadata import version as metadata_version
from typing import TYPE_CHECKING, Any, Callable, Dict, Final, Literal
from uuid import UUID

import orjson
from great_expectations.core.http import create_session
from great_expectations.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL
from great_expectations.data_context.data_context.context_factory import get_context
from pika.adapters.utils.connection_workflow import (
    AMQPConnectorException,
)
from pika.exceptions import (
    AMQPError,
    AuthenticationError,
    ChannelError,
    ProbableAuthenticationError,
)
from pydantic import v1 as pydantic_v1
from pydantic.v1 import AmqpDsn, AnyUrl
from tenacity import after_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from great_expectations_cloud.agent.config import (
    GxAgentEnvVars,
    generate_config_validation_error_text,
)
from great_expectations_cloud.agent.constants import USER_AGENT_HEADER, HeaderName
from great_expectations_cloud.agent.event_handler import (
    EventHandler,
)
from great_expectations_cloud.agent.exceptions import (
    GXAgentConfigError,
    GXAgentError,
    GXAgentUnrecoverableConnectionError,
)
from great_expectations_cloud.agent.message_service.asyncio_rabbit_mq_client import (
    AsyncRabbitMQClient,
    ClientError,
)
from great_expectations_cloud.agent.message_service.subscriber import (
    EventContext,
    OnMessageCallback,
    Subscriber,
    SubscriberError,
)
from great_expectations_cloud.agent.models import (
    AgentBaseExtraForbid,
    JobCompleted,
    JobStarted,
    JobStatus,
    ScheduledEventBase,
    UnknownEvent,
    build_failed_job_completed_status,
)

if TYPE_CHECKING:
    import requests
    from great_expectations.data_context import CloudDataContext
    from typing_extensions import Self

    from great_expectations_cloud.agent.actions.agent_action import ActionResult

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
# TODO Set in log dict
LOGGER.setLevel(logging.DEBUG)
HandlerMap = Dict[str, OnMessageCallback]


class GXAgentConfig(AgentBaseExtraForbid):
    """GXAgent configuration.
    Attributes:
        queue: name of queue
        connection_string: address of broker service
    """

    queue: str
    connection_string: AmqpDsn
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl = CLOUD_DEFAULT_BASE_URL  # type: ignore[assignment] # pydantic will coerce
    gx_cloud_organization_id: str
    gx_cloud_access_token: str


def orjson_dumps(v: Any, *, default: Callable[[Any], Any] | None) -> str:
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    # Typing using example from https://github.com/ijl/orjson?tab=readme-ov-file#serialize
    return orjson.dumps(
        v,
        default=default,
    ).decode()


def orjson_loads(v: bytes | bytearray | memoryview | str) -> Any:
    # Typing using example from https://github.com/ijl/orjson?tab=readme-ov-file#deserialize
    return orjson.loads(v)


class Payload(AgentBaseExtraForbid):
    data: Dict[str, Any]  # noqa: UP006  # Python 3.8 requires Dict instead of dict

    class Config:
        extra = "forbid"
        json_dumps = orjson_dumps
        json_loads = orjson_loads


class GXAgent:
    """
    Run GX in any environment from GX Cloud.

    To start the GX Agent, install Python and great_expectations and run `gx-agent`.
    The GX Agent loads a DataContext configuration from GX Cloud, and listens for
    user events triggered from the UI.
    """

    _PYPI_GX_AGENT_PACKAGE_NAME = "great_expectations_cloud"
    _PYPI_GREAT_EXPECTATIONS_PACKAGE_NAME = "great_expectations"

    def __init__(self: Self):
        agent_version: str = self.get_current_gx_agent_version()
        great_expectations_version: str = self._get_current_great_expectations_version()
        print(f"GX Agent version: {agent_version}")
        print(f"Great Expectations version: {great_expectations_version}")
        print("Initializing the GX Agent.")
        self._config = self._get_config()
        print("Loading a DataContext - this might take a moment.")

        with warnings.catch_warnings():
            # suppress warnings about GX version
            warnings.filterwarnings("ignore", message="You are using great_expectations version")
            self._context: CloudDataContext = get_context(cloud_mode=True)
        print("DataContext is ready.")

        self._set_http_session_headers(data_context=self._context)

        # Create a thread pool with a single worker, so we can run long-lived
        # GX processes and maintain our connection to the broker. Note that
        # the CloudDataContext cached here is used by the worker, so
        # it isn't safe to increase the number of workers running GX jobs.
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._current_task: Future[Any] | None = None
        self._redeliver_msg_task: asyncio.Task[Any] | None = None
        self._correlation_ids: defaultdict[str, int] = defaultdict(lambda: 0)

    def run(self) -> None:
        """Open a connection to GX Cloud."""

        print("Opening connection to GX Cloud.")
        self._listen()
        print("The connection to GX Cloud has been closed.")

    # ZEL-505: A race condition can occur if two or more agents are started at the same time
    #          due to the generation of passwords for rabbitMQ queues. This can be mitigated
    #          by adding a delay and retrying the connection. Retrying with new credentials
    #          requires calling get_config again, which handles the password generation.
    @retry(
        retry=retry_if_exception_type(
            (AuthenticationError, ProbableAuthenticationError, AMQPError, ChannelError)
        ),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        after=after_log(LOGGER, logging.DEBUG),
    )
    def _listen(self) -> None:
        """Manage connection lifecycle."""
        subscriber = None
        try:
            client = AsyncRabbitMQClient(url=str(self._config.connection_string))
            subscriber = Subscriber(client=client)
            print("The GX Agent is ready.")
            # Open a connection until encountering a shutdown event
            subscriber.consume(
                queue=self._config.queue,
                on_message=self._handle_event_as_thread_enter,
            )
        except KeyboardInterrupt:
            print("Received request to shut down.")
        except (SubscriberError, ClientError):
            print("The connection to GX Cloud has encountered an error.")
        except GXAgentUnrecoverableConnectionError:
            print("The connection to GX Cloud has encountered an unrecoverable error.")
            sys.exit(1)
        except (
            AuthenticationError,
            ProbableAuthenticationError,
            AMQPConnectorException,
        ):
            # Retry with new credentials
            self._config = self._get_config()
            # Raise to use the retry decorator to handle the retry logic
            raise
        finally:
            if subscriber is not None:
                subscriber.close()

    @classmethod
    def get_current_gx_agent_version(cls) -> str:
        version: str = metadata_version(cls._PYPI_GX_AGENT_PACKAGE_NAME)
        return version

    @classmethod
    def _get_current_great_expectations_version(cls) -> str:
        version: str = metadata_version(cls._PYPI_GREAT_EXPECTATIONS_PACKAGE_NAME)
        return version

    def _handle_event_as_thread_enter(self, event_context: EventContext) -> None:
        """Schedule _handle_event to run in a thread.

        Callback passed to Subscriber.consume which forwards events to
        the EventHandler for processing.

        Args:
            event_context: An Event with related properties and actions.
        """
        if self._reject_correlation_id(event_context.correlation_id) is True:
            # this event has been redelivered too many times - remove it from circulation
            event_context.processed_with_failures()
            return
        elif self._can_accept_new_task() is not True:
            # request that this message is redelivered later
            loop = asyncio.get_event_loop()
            # store a reference the task to ensure it isn't garbage collected
            self._redeliver_msg_task = loop.create_task(event_context.redeliver_message())
            return

        self._current_task = self._executor.submit(
            self._handle_event,
            event_context=event_context,
        )

        if self._current_task is not None:
            # add a callback for when the thread exits and pass it the event context
            on_exit_callback = partial(
                self._handle_event_as_thread_exit, event_context=event_context
            )
            self._current_task.add_done_callback(on_exit_callback)

    def get_data_context(self, event_context: EventContext) -> CloudDataContext:
        """Helper method to get a DataContext Agent. Overridden in GX-Runner."""
        return self._context

    def get_organization_id(self, event_context: EventContext) -> UUID:
        """Helper method to get the organization ID. Overridden in GX-Runner."""
        return UUID(self._config.gx_cloud_organization_id)

    def get_auth_key(self) -> str:
        """Helper method to get the auth key. Overridden in GX-Runner."""
        return self._config.gx_cloud_access_token

    def _handle_event(self, event_context: EventContext) -> ActionResult:
        """Pass events to EventHandler.

        Callback passed to Subscriber.consume which forwards events to
        the EventHandler for processing.

        Args:
            event_context: event with related properties and actions.
        """
        # warning:  this method will not be executed in the main thread

        data_context = self.get_data_context(event_context=event_context)
        # ensure that great_expectations.http requests to GX Cloud include the job_id/correlation_id
        self._set_http_session_headers(
            correlation_id=event_context.correlation_id, data_context=data_context
        )

        org_id = self.get_organization_id(event_context)
        base_url = self._config.gx_cloud_base_url
        auth_key = self.get_auth_key()

        if isinstance(event_context.event, ScheduledEventBase):
            self._create_scheduled_job_and_set_started(event_context, org_id)
        else:
            self._update_status(
                correlation_id=event_context.correlation_id, status=JobStarted(), org_id=org_id
            )
        print(f"Starting job {event_context.event.type} ({event_context.correlation_id}) ")
        LOGGER.info(
            "Starting job",
            extra={
                "event_type": event_context.event.type,
                "correlation_id": event_context.correlation_id,
                "organization_id": str(org_id),
            },
        )
        handler = EventHandler(context=data_context)
        # This method might raise an exception. Allow it and handle in _handle_event_as_thread_exit
        result = handler.handle_event(
            event=event_context.event,
            id=event_context.correlation_id,
            base_url=base_url,
            auth_key=auth_key,
            organization_id=org_id,
        )
        return result

    def _handle_event_as_thread_exit(
        self, future: Future[ActionResult], event_context: EventContext
    ) -> None:
        """Callback invoked when the thread running GX exits.

        Args:
            future: object returned from the thread
            event_context: event with related properties and actions.
        """
        # warning:  this method will not be executed in the main thread

        org_id = self.get_organization_id(event_context)

        # get results or errors from the thread
        error = future.exception()
        if error is None:
            result: ActionResult = future.result()

            if result.type == UnknownEvent().type:
                status = JobCompleted(
                    success=False,
                    created_resources=[],
                    error_stack_trace="The version of the GX Agent you are using does not support this functionality. Please upgrade to the most recent image tagged with `stable`.",
                    processed_by=self._get_processed_by(),
                )
                LOGGER.error(
                    "Job completed with error. Ensure agent is up-to-date.",
                    extra={
                        "event_type": event_context.event.type,
                        "id": event_context.correlation_id,
                        "organization_id": str(org_id),
                    },
                )
            else:
                status = JobCompleted(
                    success=True,
                    created_resources=result.created_resources,
                    processed_by=self._get_processed_by(),
                )
                LOGGER.info(
                    "Completed job",
                    extra={
                        "event_type": event_context.event.type,
                        "correlation_id": event_context.correlation_id,
                        "job_duration": (
                            result.job_duration.total_seconds() if result.job_duration else None
                        ),
                        "organization_id": str(org_id),
                    },
                )
        else:
            status = build_failed_job_completed_status(error)
            LOGGER.info(traceback.format_exc())
            LOGGER.info(
                "Job completed with error",
                extra={
                    "event_type": event_context.event.type,
                    "correlation_id": event_context.correlation_id,
                    "organization_id": str(org_id),
                },
            )

        self._update_status(
            correlation_id=event_context.correlation_id, status=status, org_id=org_id
        )

        # ack message and cleanup resources
        event_context.processed_successfully()
        self._current_task = None

    def _get_processed_by(self) -> Literal["agent", "runner"]:
        """Return the name of the service that processed the event."""
        return "runner" if self._config.queue == "gx-runner" else "agent"

    def _can_accept_new_task(self) -> bool:
        """Are we currently processing a task or are we free to take a new one?"""
        return self._current_task is None or self._current_task.done()

    def _reject_correlation_id(self, id: str) -> bool:
        """Has this correlation ID been seen too many times?"""
        MAX_REDELIVERY = 10
        MAX_KEYS = 100000
        self._correlation_ids[id] += 1
        delivery_count = self._correlation_ids[id]
        if delivery_count > MAX_REDELIVERY:
            should_reject = True
        else:
            should_reject = False
        # ensure the correlation ids dict doesn't get too large:
        if len(self._correlation_ids.keys()) > MAX_KEYS:
            self._correlation_ids.clear()
        return should_reject

    @classmethod
    def _get_config(cls) -> GXAgentConfig:
        """Construct GXAgentConfig."""

        # ensure we have all required env variables, and provide a useful error if not

        try:
            env_vars = GxAgentEnvVars()
        except pydantic_v1.ValidationError as validation_err:
            raise GXAgentConfigError(
                generate_config_validation_error_text(validation_err)
            ) from validation_err

        # obtain the broker url and queue name from Cloud
        agent_sessions_url = (
            f"{env_vars.gx_cloud_base_url}/organizations/"
            f"{env_vars.gx_cloud_organization_id}/agent-sessions"
        )

        session = create_session(access_token=env_vars.gx_cloud_access_token)
        response = session.post(agent_sessions_url)
        session.close()
        if response.ok is not True:
            raise GXAgentError(  # noqa: TRY003 # TODO: use AuthenticationError
                "Unable to authenticate to GX Cloud. Please check your credentials."
            )

        json_response = response.json()
        queue = json_response["queue"]
        connection_string = json_response["connection_string"]

        try:
            # pydantic will coerce the url to the correct type
            return GXAgentConfig(
                queue=queue,
                connection_string=connection_string,
                gx_cloud_base_url=env_vars.gx_cloud_base_url,
                gx_cloud_organization_id=env_vars.gx_cloud_organization_id,
                gx_cloud_access_token=env_vars.gx_cloud_access_token,
            )
        except pydantic_v1.ValidationError as validation_err:
            raise GXAgentConfigError(
                generate_config_validation_error_text(validation_err)
            ) from validation_err

    def _update_status(self, correlation_id: str, status: JobStatus, org_id: UUID) -> None:
        """Update GX Cloud on the status of a job.

        Args:
            correlation_id: job identifier
            status: pydantic model encapsulating the current status.
        """
        LOGGER.info(
            "Updating status",
            extra={
                "correlation_id": correlation_id,
                "status": str(status),
                "organization_id": str(org_id),
            },
        )
        agent_sessions_url = (
            f"{self._config.gx_cloud_base_url}/organizations/{org_id}"
            + f"/agent-jobs/{correlation_id}"
        )
        with create_session(access_token=self.get_auth_key()) as session:
            data = status.json()
            session.patch(agent_sessions_url, data=data)
            LOGGER.info(
                "Status updated",
                extra={
                    "correlation_id": correlation_id,
                    "status": str(status),
                    "organization_id": str(org_id),
                },
            )

    def _create_scheduled_job_and_set_started(
        self, event_context: EventContext, org_id: UUID
    ) -> None:
        """Create a job in GX Cloud for scheduled events.

        This is because the scheduler + lambda create the event in the queue, and the agent consumes it. The agent then
        sends a request to the agent-jobs endpoint to create the job in mercury to keep track of the job status.
        Non-scheduled events by contrast create the job in mercury and the event in the queue at the same time.

        Args:
            event_context: event with related properties and actions.
        """
        data = {
            "correlation_id": event_context.correlation_id,
            "event": event_context.event.dict(),
        }
        LOGGER.info(
            "Creating scheduled job and setting started",
            extra={
                "correlation_id": str(event_context.correlation_id),
                "event_type": str(event_context.event.type),
                "organization_id": str(org_id),
            },
        )

        agent_sessions_url = (
            f"{self._config.gx_cloud_base_url}/organizations/{org_id}" + "/agent-jobs"
        )
        with create_session(access_token=self.get_auth_key()) as session:
            payload = Payload(data=data)
            session.post(agent_sessions_url, data=payload.json())
            LOGGER.info(
                "Created scheduled job and set started",
                extra={
                    "correlation_id": str(event_context.correlation_id),
                    "event_type": str(event_context.event.type),
                    "organization_id": str(org_id),
                },
            )

    def get_header_name(self) -> type[HeaderName]:
        return HeaderName

    def get_user_agent_header(self) -> str:
        return USER_AGENT_HEADER

    def _get_version(self) -> str:
        return self.get_current_gx_agent_version()

    def _set_http_session_headers(
        self, data_context: CloudDataContext, correlation_id: str | None = None
    ) -> None:
        """
        Set the session headers for requests to GX Cloud.
        In particular, set the User-Agent header to identify the GX Agent and the correlation_id as
        Agent-Job-Id if provided.

        Note: the Agent-Job-Id header value will be set for all GX Cloud request until this method is
        called again.
        """
        from great_expectations import __version__  # type: ignore[attr-defined] # TODO: fix this
        from great_expectations.core import http
        from great_expectations.data_context.store.gx_cloud_store_backend import GXCloudStoreBackend

        header_name = self.get_header_name()
        user_agent_header = self.get_user_agent_header()

        agent_version = self._get_version()
        LOGGER.debug(
            "Setting session headers for GX Cloud",
            extra={
                "user_agent": header_name.USER_AGENT,
                "agent_version": agent_version,
                "header_name": header_name.AGENT_JOB_ID,
                "correlation_id": correlation_id,
            },
        )

        if correlation_id:
            # OSS doesn't use the same session for all requests, so we need to set the header for each store
            for store in data_context.stores.values():
                backend = store._store_backend
                if isinstance(backend, GXCloudStoreBackend):
                    backend._session.headers[header_name.AGENT_JOB_ID] = correlation_id

        def _update_headers_agent_patch(
            session: requests.Session, access_token: str
        ) -> requests.Session:
            """
            This accounts for direct agent requests to GX Cloud and OSS calls outside of a GXCloudStoreBackend
            """
            headers = {
                "Content-Type": "application/vnd.api+json",
                "Authorization": f"Bearer {access_token}",
                "Gx-Version": __version__,
                header_name.USER_AGENT: f"{user_agent_header}/{agent_version}",
            }
            if correlation_id:
                headers[header_name.AGENT_JOB_ID] = correlation_id
            session.headers.update(headers)
            return session

        # TODO: this is relying on a private implementation detail
        # use a public API once it is available
        http._update_headers = _update_headers_agent_patch
