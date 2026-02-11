from __future__ import annotations

from pydantic.v1 import BaseSettings


class GxCloudTestConfig(BaseSettings):
    """Test configuration for GX Cloud credentials and settings.

    Reads from environment variables with GX_CLOUD_ prefix.
    """

    gx_cloud_organization_id: str
    gx_cloud_access_token: str
    gx_cloud_workspace_id: str | None = None
    gx_cloud_base_url: str | None = None

    class Config:
        env_file = ".env"
        case_sensitive = False

    def __init__(self, **overrides: str) -> None:
        """
        Custom __init__ to prevent type error when relying on environment variables.

        TODO: once mypy fully supports annotating **kwargs with a Unpack[TypedDict], we should do that.
        https://peps.python.org/pep-0692/
        """
        super().__init__(**overrides)


class SnowflakeTestConfig(BaseSettings):
    """Test configuration for Snowflake credentials.

    Reads from environment variables with SNOWFLAKE_ prefix.
    """

    snowflake_account: str
    snowflake_user: str
    snowflake_pw: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_warehouse: str
    snowflake_role: str

    class Config:
        env_file = ".env"
        case_sensitive = False

    def __init__(self, **overrides: str) -> None:
        """
        Custom __init__ to prevent type error when relying on environment variables.

        TODO: once mypy fully supports annotating **kwargs with a Unpack[TypedDict], we should do that.
        https://peps.python.org/pep-0692/
        """
        super().__init__(**overrides)
