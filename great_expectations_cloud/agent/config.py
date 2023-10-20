from great_expectations.compatibility.pydantic import AnyUrl, BaseSettings


class GxAgentEnvVars(BaseSettings):  # type: ignore[misc] # BaseSettings is has Any type
    # pydantic will coerce this string to AnyUrl type
    gx_cloud_base_url: AnyUrl
    gx_cloud_organization_id: str
    gx_cloud_access_token: str
