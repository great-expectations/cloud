from __future__ import annotations

from great_expectations.compatibility.pydantic import AnyUrl, BaseSettings
from great_expectations.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL


class GxAgentEnvVars(BaseSettings):  # type: ignore[misc] # BaseSettings is has Any type
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl = CLOUD_DEFAULT_BASE_URL
    gx_cloud_organization_id: str
    gx_cloud_access_token: str
