from great_expectations_cloud.compatibility import pydantic
from great_expectations_cloud.compatibility.pydantic import AnyUrl
from great_expectations_cloud.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL


class GxAgentEnvVars(pydantic.BaseSettings):
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl = CLOUD_DEFAULT_BASE_URL  # type: ignore[assignment]
    gx_cloud_organization_id: str
    gx_cloud_access_token: str
