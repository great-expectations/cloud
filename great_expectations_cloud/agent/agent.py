from __future__ import annotations

import logging
import warnings
from importlib.metadata import version as metadata_version
from typing import TYPE_CHECKING, Any, Callable, Dict, Final, Literal
from uuid import UUID

import orjson
from fast_depends import dependency_provider
from great_expectations.core.http import create_session
from great_expectations.data_context.data_context.context_factory import get_context

from great_expectations_cloud.agent.config import (
    BaseConfig,
    GXAgentConfig,
)
from great_expectations_cloud.agent.constants import USER_AGENT_HEADER, HeaderName
from great_expectations_cloud.agent.event_handler import (
    EventHandler,
)
from great_expectations_cloud.agent.models import (
    AgentBaseExtraForbid,
    EventContext,
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
LOGGER.setLevel(logging.INFO)


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


def agent_instance() -> GXAgent:
    # This function is used to inject the GX Agent instance into the handle function
    # The dependency is overridden with the real instance later on
    raise NotImplementedError("Missing GX Agent instance")


class GXAgent:
    """
    Run GX in any environment from GX Cloud.

    To start the GX Agent, install Python and great_expectations and run `gx-agent`.
    The GX Agent loads a DataContext configuration from GX Cloud, and listens for
    user events triggered from the UI.
    """

    _PYPI_GX_AGENT_PACKAGE_NAME = "great_expectations_cloud"
    _PYPI_GREAT_EXPECTATIONS_PACKAGE_NAME = "great_expectations"

    def __init__(self: Self, config: BaseConfig) -> None:
        self.configure(config)
        self._print_startup_message()
        self._load_data_context_on_startup()

        # Override the agent_instance dependency with the real instance after startup
        dependency_provider.dependency_overrides[agent_instance] = lambda: self

    def configure(self, config: BaseConfig) -> None:
        # Cast the config from protocol BaseConfig to concrete GXAgentConfig
        self._config = GXAgentConfig(**config.dict())

    def _print_startup_message(self) -> None:
        agent_version: str = self.get_current_gx_agent_version()
        great_expectations_version: str = self._get_current_great_expectations_version()
        print(f"GX Agent version: {agent_version}")
        print(f"Great Expectations version: {great_expectations_version}")
        print("Initializing the GX Agent.")

    def _load_data_context_on_startup(self) -> None:
        print("Loading a DataContext - this might take a moment.")

        with warnings.catch_warnings():
            # suppress warnings about GX version
            warnings.filterwarnings("ignore", message="You are using great_expectations version")
            self._context: CloudDataContext = get_context(cloud_mode=True)
        print("DataContext is ready.")

        self._set_http_session_headers(data_context=self._context)
        print("Opening connection to GX Cloud.")

    @classmethod
    def get_current_gx_agent_version(cls) -> str:
        version: str = metadata_version(cls._PYPI_GX_AGENT_PACKAGE_NAME)
        return version

    @classmethod
    def _get_current_great_expectations_version(cls) -> str:
        version: str = metadata_version(cls._PYPI_GREAT_EXPECTATIONS_PACKAGE_NAME)
        return version

    def get_data_context(self, event_context: EventContext) -> CloudDataContext:
        """Helper method to get a DataContext Agent. Overridden in GX-Runner."""
        return self._context

    def get_organization_id(self, event_context: EventContext) -> UUID:
        """Helper method to get the organization ID. Overridden in GX-Runner."""
        return UUID(self._config.gx_cloud_organization_id)

    def get_auth_key(self) -> str:
        """Helper method to get the auth key. Overridden in GX-Runner."""
        return self._config.gx_cloud_access_token

    def _handle_message(self, msg: Dict[str, Any], correlation_id: str) -> None:  # noqa: UP006
        # warning:  this method will not be executed in the main thread
        event = EventHandler.parse_event_from_dict(msg)
        event_context = EventContext(
            event=event,
            correlation_id=correlation_id,
        )
        organization_id = None
        status = None
        try:
            organization_id = self.get_organization_id(event_context)
            result = self._handle_event(event_context)
        except Exception as e:
            status = build_failed_job_completed_status(e)
            LOGGER.exception(
                "Job completed with error",
                extra={
                    "event_type": event_context.event.type,
                    "correlation_id": event_context.correlation_id,
                },
            )
        else:  # handle no exception
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
                        "organization_id": str(organization_id),
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
                        "organization_id": str(organization_id),
                    },
                )
        finally:
            if organization_id and status:
                self._update_status(
                    job_id=event_context.correlation_id, status=status, org_id=organization_id
                )
            elif not organization_id:
                LOGGER.error(
                    "Organization ID is not available.",
                    extra={
                        "event_type": event_context.event.type,
                        "correlation_id": event_context.correlation_id,
                        "event": event.dict(),
                    },
                )
            elif not status:
                LOGGER.error(
                    "Status is not available.",
                    extra={
                        "event_type": event_context.event.type,
                        "correlation_id": event_context.correlation_id,
                        "organization_id": str(organization_id),
                    },
                )

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
                job_id=event_context.correlation_id, status=JobStarted(), org_id=org_id
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
        # This method might raise an exception. Allow it and handle in _handle_message
        result = handler.handle_event(
            event=event_context.event,
            id=event_context.correlation_id,
            base_url=base_url,
            auth_key=auth_key,
            organization_id=org_id,
        )
        return result

    def _get_processed_by(self) -> Literal["agent", "runner"]:
        """Return the name of the service that processed the event."""
        return "runner" if self._config.queue == "gx-runner" else "agent"

    def _update_status(self, job_id: str, status: JobStatus, org_id: UUID) -> None:
        """Update GX Cloud on the status of a job.

        Args:
            job_id: job identifier, also known as correlation_id
            status: pydantic model encapsulating the current status
        """
        LOGGER.info(
            "Updating status",
            extra={"job_id": job_id, "status": str(status), "organization_id": str(org_id)},
        )
        agent_sessions_url = (
            f"{self._config.gx_cloud_base_url}/organizations/{org_id}" + f"/agent-jobs/{job_id}"
        )
        with create_session(access_token=self.get_auth_key()) as session:
            data = status.json()
            session.patch(agent_sessions_url, data=data)
            LOGGER.info(
                "Status updated",
                extra={"job_id": job_id, "status": str(status), "organization_id": str(org_id)},
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
            extra={**data, "organization_id": str(org_id)},
        )

        agent_sessions_url = (
            f"{self._config.gx_cloud_base_url}/organizations/{org_id}" + "/agent-jobs"
        )
        with create_session(access_token=self.get_auth_key()) as session:
            payload = Payload(data=data)
            session.post(agent_sessions_url, data=payload.json())
            LOGGER.info(
                "Created scheduled job and set started",
                extra={**data, "organization_id": str(org_id)},
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
                "job_id": header_name.AGENT_JOB_ID,
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


def get_version() -> str:
    return GXAgent.get_current_gx_agent_version()
