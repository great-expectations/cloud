from __future__ import annotations

import asyncio
import logging
import traceback
import warnings
from collections import defaultdict
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from importlib.metadata import version as metadata_version
from typing import TYPE_CHECKING, Any, Dict, Final

from great_expectations import get_context
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import AmqpDsn, AnyUrl
from great_expectations.core.http import create_session
from great_expectations.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL
from packaging.version import Version

from great_expectations_cloud.agent.config import (
    GxAgentEnvVars,
    generate_config_validation_error_text,
)
from great_expectations_cloud.agent.constants import USER_AGENT_HEADER, HeaderName
from great_expectations_cloud.agent.event_handler import (
    EventHandler,
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
    AgentBaseModel,
    JobCompleted,
    JobStarted,
    JobStatus,
    UnknownEvent,
)

if TYPE_CHECKING:
    import requests
    from great_expectations.data_context import CloudDataContext
    from typing_extensions import Self

    from great_expectations_cloud.agent.actions.agent_action import ActionResult

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
HandlerMap = Dict[str, OnMessageCallback]


class GXAgentConfig(AgentBaseModel):
    """GXAgent configuration.
    Attributes:
        queue: name of queue
        connection_string: address of broker service
    """

    queue: str
    connection_string: AmqpDsn
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl = CLOUD_DEFAULT_BASE_URL
    gx_cloud_organization_id: str
    gx_cloud_access_token: str


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
        self._set_http_session_headers()
        self._config = self._get_config()
        print("Loading a DataContext - this might take a moment.")

        with warnings.catch_warnings():
            # suppress warnings about GX version
            warnings.filterwarnings("ignore", message="You are using great_expectations version")
            self._context: CloudDataContext = get_context(cloud_mode=True)

        print("DataContext is ready.")

        # Create a thread pool with a single worker, so we can run long-lived
        # GX processes and maintain our connection to the broker. Note that
        # the CloudDataContext cached here is used by the worker, so
        # it isn't safe to increase the number of workers running GX jobs.
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._current_task: Future[Any] | None = None
        self._correlation_ids: defaultdict[str, int] = defaultdict(lambda: 0)

    def run(self) -> None:
        """Open a connection to GX Cloud."""

        print("Opening connection to GX Cloud.")
        self._listen()
        print("Connection to GX Cloud has been closed.")

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
            print("Received request to shutdown.")
        except (SubscriberError, ClientError):
            print("Connection to GX Cloud has encountered an error.")
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
        elif self._can_accept_new_task() is not True or isinstance(
            event_context.event, UnknownEvent
        ):
            # request that this message is redelivered later. If the event is UnknownEvent
            # we don't understand it, so requeue it in the hope that someone else does.
            loop = asyncio.get_event_loop()
            loop.create_task(event_context.redeliver_message())
            return

        # ensure that great_expectations.http requests to GX Cloud include the job_id/correlation_id
        self._set_http_session_headers(correlation_id=event_context.correlation_id)

        # send this message to a thread for processing
        self._current_task = self._executor.submit(self._handle_event, event_context=event_context)

        if self._current_task is not None:
            # add a callback for when the thread exits and pass it the event context
            on_exit_callback = partial(
                self._handle_event_as_thread_exit, event_context=event_context
            )
            self._current_task.add_done_callback(on_exit_callback)

    def _handle_event(self, event_context: EventContext) -> ActionResult:
        """Pass events to EventHandler.

        Callback passed to Subscriber.consume which forwards events to
        the EventHandler for processing.

        Args:
            event_context: event with related properties and actions.
        """
        # warning:  this method will not be executed in the main thread
        self._update_status(job_id=event_context.correlation_id, status=JobStarted())
        print(f"Starting job {event_context.event.type} ({event_context.correlation_id}) ")
        handler = EventHandler(context=self._context)
        # This method might raise an exception. Allow it and handle in _handle_event_as_thread_exit
        result = handler.handle_event(event=event_context.event, id=event_context.correlation_id)
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

        # get results or errors from the thread
        error = future.exception()
        if error is None:
            result: ActionResult = future.result()
            status = JobCompleted(
                success=True,
                created_resources=result.created_resources,
            )
            print(f"Completed job: {event_context.event.type} ({event_context.correlation_id})")
        else:
            status = JobCompleted(success=False, error_stack_trace=str(error))
            print(traceback.format_exc())
            print(
                f"Job completed with error: {event_context.event.type} ({event_context.correlation_id})"
            )
        self._update_status(job_id=event_context.correlation_id, status=status)

        # ack message and cleanup resources
        event_context.processed_successfully()
        self._current_task = None

    def _can_accept_new_task(self) -> bool:
        """Are we currently processing a task, or are we free to take a new one?"""
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
        except pydantic.ValidationError as validation_err:
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
        if response.ok is not True:
            raise GXAgentError("Unable to authenticate to GX Cloud. Please check your credentials.")

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
        except pydantic.ValidationError as validation_err:
            raise GXAgentConfigError(
                generate_config_validation_error_text(validation_err)
            ) from validation_err

    def _update_status(self, job_id: str, status: JobStatus) -> None:
        """Update GX Cloud on the status of a job.

        Args:
            job_id: job identifier, also known as correlation_id
            status: pydantic model encapsulating the current status
        """
        LOGGER.info(f"Updating status: {job_id} - {status}")
        agent_sessions_url = (
            f"{self._config.gx_cloud_base_url}/organizations/{self._config.gx_cloud_organization_id}"
            + f"/agent-jobs/{job_id}"
        )
        session = create_session(access_token=self._config.gx_cloud_access_token)
        data = status.json()
        session.patch(agent_sessions_url, data=data)

    def _set_http_session_headers(self, correlation_id: str | None = None) -> None:
        """
        Set the the session headers for requests to GX Cloud.
        In particular, set the User-Agent header to identify the GX Agent and the correlation_id as
        Agent-Job-Id if provided.

        Note: the Agent-Job-Id header value will be set for all GX Cloud request until this method is
        called again.
        """
        from great_expectations import __version__
        from great_expectations.core import http
        from great_expectations.data_context.store.gx_cloud_store_backend import GXCloudStoreBackend

        if Version(__version__) > Version(
            "0.19"  # using 0.19 instead of 1.0 to account for pre-releases
        ):
            # TODO: public API should be available in v1
            LOGGER.info(
                f"Unable to set {HeaderName.USER_AGENT} or {HeaderName.AGENT_JOB_ID} header for requests to GX Cloud"
            )
            return

        agent_version = self.get_current_gx_agent_version()
        LOGGER.debug(
            f"Setting session headers for GX Cloud. {HeaderName.USER_AGENT}:{agent_version} {HeaderName.AGENT_JOB_ID}:{correlation_id}"
        )

        if correlation_id:
            # OSS doesn't use the same session for all requests, so we need to set the header for each store
            for store in self._context.stores.values():
                backend = store._store_backend
                if isinstance(backend, GXCloudStoreBackend):
                    backend._session.headers[HeaderName.AGENT_JOB_ID] = correlation_id

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
                HeaderName.USER_AGENT: f"{USER_AGENT_HEADER}/{agent_version}",
            }
            if correlation_id:
                headers[HeaderName.AGENT_JOB_ID] = correlation_id
            session.headers.update(headers)
            return session

        # TODO: this is relying on a private implementation detail
        # use a public API once it is available
        http._update_headers = _update_headers_agent_patch


class GXAgentError(Exception):
    ...


class GXAgentConfigError(GXAgentError):
    ...
