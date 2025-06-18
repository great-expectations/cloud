from __future__ import annotations

from typing import Optional

from great_expectations.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL
from pydantic.v1 import AnyUrl, BaseSettings, ValidationError


class GxAgentEnvVars(BaseSettings):
    gx_cloud_base_url: AnyUrl = AnyUrl(url=CLOUD_DEFAULT_BASE_URL, scheme="https")
    gx_cloud_organization_id: str
    gx_cloud_access_token: str
    enable_progress_bars: bool = True

    amqp_host_override: Optional[str] = None  # noqa: UP045 # pipe not working with 3.9
    amqp_port_override: Optional[int] = None  # noqa: UP045 # pipe not working with 3.9

    def __init__(self, **overrides: str | AnyUrl) -> None:
        """
        Custom __init__ to prevent type error when relying on environment variables.

        TODO:once mypy fully support annoting **kwargs with a Unpack[TypedDict], we should do that.
        https://peps.python.org/pep-0692/
        """
        super().__init__(**overrides)


def generate_config_validation_error_text(validation_error: ValidationError) -> str:
    missing_variables = ", ".join(
        [str(validation_error["loc"][0]) for validation_error in validation_error.errors()]
    )
    error_text = f"Missing or badly formed environment variable(s). Make sure to set the following environment variable(s): {missing_variables}"
    return error_text
