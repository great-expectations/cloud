from __future__ import annotations

import logging
import os
from typing import Final

import great_expectations as gx
import pytest
from great_expectations.data_context import CloudDataContext

LOGGER: Final = logging.getLogger("tests")


@pytest.fixture(scope="module")
def cloud_base_url() -> str:
    return "http://localhost:5000"


@pytest.fixture(scope="module")
def org_id_env_var() -> str:
    org_id = os.environ.get("GX_CLOUD_ORGANIZATION_ID")
    assert org_id, "No GX_CLOUD_ORGANIZATION_ID env var"
    return org_id


@pytest.fixture(scope="module")
def org_id_different_from_env_var() -> str:
    # This is a different org_id from the one in the env var
    org_id = "1a6b0e6b-5943-4225-a23c-d154fee937a2"
    assert org_id != os.environ.get(
        "GX_CLOUD_ORGANIZATION_ID"
    ), "org_id is the same as the one in the env var"
    return org_id


@pytest.fixture(scope="module")
def token_env_var() -> str:
    gx_token = os.environ.get("GX_CLOUD_ACCESS_TOKEN")
    assert gx_token, "No GX_CLOUD_ACCESS_TOKEN env var"
    return gx_token


@pytest.fixture(scope="module")
def context(org_id_env_var: str, token_env_var: str) -> CloudDataContext:
    context = gx.get_context(  # type: ignore[attr-defined] # TODO: fix this
        mode="cloud",
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=org_id_env_var,
        cloud_access_token=token_env_var,
    )
    assert isinstance(context, CloudDataContext)
    return context
