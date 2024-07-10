from __future__ import annotations

from great_expectations.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL
from pydantic.v1 import AnyUrl, BaseSettings, ValidationError


class GxAgentEnvVars(BaseSettings):
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl = AnyUrl(CLOUD_DEFAULT_BASE_URL)
    gx_cloud_organization_id: str
    gx_cloud_access_token: str


def generate_config_validation_error_text(validation_error: ValidationError) -> str:
    missing_variables = ", ".join(
        [str(validation_error["loc"][0]) for validation_error in validation_error.errors()]
    )
    error_text = f"Missing or badly formed environment variable(s). Make sure to set the following environment variable(s): {missing_variables}"
    return error_text
