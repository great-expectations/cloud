from __future__ import annotations

import logging
from typing import Final

import great_expectations as gx
import pytest
from great_expectations.data_context import CloudDataContext

from tests.test_config import GxCloudTestConfig

LOGGER: Final = logging.getLogger("tests")


@pytest.fixture(scope="module")
def test_config() -> GxCloudTestConfig:
    """Load test configuration from environment variables."""
    return GxCloudTestConfig()


@pytest.fixture(scope="module")
def cloud_base_url(test_config: GxCloudTestConfig) -> str:
    return test_config.gx_cloud_base_url or "http://localhost:5000"


@pytest.fixture(scope="module")
def org_id_env_var(test_config: GxCloudTestConfig) -> str:
    return test_config.gx_cloud_organization_id


@pytest.fixture(scope="module")
def workspace_id_env_var(test_config: GxCloudTestConfig) -> str:
    assert test_config.gx_cloud_workspace_id, "No GX_CLOUD_WORKSPACE_ID env var"
    return test_config.gx_cloud_workspace_id


@pytest.fixture(scope="module")
def token_env_var(test_config: GxCloudTestConfig) -> str:
    return test_config.gx_cloud_access_token


@pytest.fixture(scope="module")
def context(
    org_id_env_var: str, workspace_id_env_var: str, token_env_var: str, cloud_base_url: str
) -> CloudDataContext:
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=cloud_base_url,
        cloud_organization_id=org_id_env_var,
        cloud_access_token=token_env_var,
        cloud_workspace_id=workspace_id_env_var,
    )
    assert isinstance(context, CloudDataContext)
    return context
