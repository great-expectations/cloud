from __future__ import annotations

import random
import string
import uuid
from typing import Any, Protocol

from great_expectations.core.http import create_session
from great_expectations.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL
from pydantic import v1 as pydantic_v1
from pydantic.v1 import AmqpDsn, AnyUrl, BaseSettings, ValidationError

from great_expectations_cloud.agent.exceptions import GXAgentConfigError, GXAgentError
from great_expectations_cloud.agent.models import AgentBaseExtraForbid

# Note: MAX_DELIVERY is only used for UnknownEvent, to attempt again (in case the agent is outdated) in hopes that agent is updated
# if processing fails due to an exception, the message will not be redelivered, and instead an error will be logged for the job (if possible)
MAX_DELIVERY = 10


class BaseConfig(Protocol):
    """Base configuration required
    Attributes:
        queue: name of queue
        connection_string: address of broker service
    """

    queue: str
    connection_string: AmqpDsn
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl = CLOUD_DEFAULT_BASE_URL  # type: ignore[assignment] # pydantic will coerce

    @classmethod
    def build(cls) -> BaseConfig:
        raise NotImplementedError

    def dict(self) -> dict[str, Any]:
        raise NotImplementedError


class GXAgentConfig(AgentBaseExtraForbid):
    queue: str
    connection_string: AmqpDsn
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl = CLOUD_DEFAULT_BASE_URL  # type: ignore[assignment] # pydantic will coerce

    # Specific to GX Org Agents
    gx_cloud_organization_id: str
    gx_cloud_access_token: str

    @classmethod
    def build(cls) -> GXAgentConfig:
        """Builds and returns GXAgentConfig."""

        # If in testing environment, return a dummy config
        if Env().is_test:
            return GXAgentConfig(
                gx_cloud_organization_id=str(uuid.uuid4()),
                queue="test-queue",
                connection_string=AmqpDsn("amqp://test:test@localhost:5672", scheme="amqp"),
                gx_cloud_base_url=AnyUrl("http://localhost:5000", scheme="http"),
                gx_cloud_access_token="".join(
                    random.choices(string.ascii_letters + string.digits, k=20)  # noqa: S311
                ),
            )

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


class GxAgentEnvVars(BaseSettings):
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl = CLOUD_DEFAULT_BASE_URL  # type: ignore[assignment]
    gx_cloud_organization_id: str
    gx_cloud_access_token: str

    def __init__(self, **overrides: str | AnyUrl) -> None:
        """
        Custom __init__ to prevent type error when relying on environment variables.

        TODO:once mypy fully support annoting **kwargs with a Unpack[TypedDict], we should do that.
        https://peps.python.org/pep-0692/
        """
        super().__init__(**overrides)


class Env(BaseSettings):
    is_test: bool = False


def generate_config_validation_error_text(validation_error: ValidationError) -> str:
    missing_variables = ", ".join(
        [str(validation_error["loc"][0]) for validation_error in validation_error.errors()]
    )
    error_text = f"Missing or badly formed environment variable(s). Make sure to set the following environment variable(s): {missing_variables}"
    return error_text
