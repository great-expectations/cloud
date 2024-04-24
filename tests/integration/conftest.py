from __future__ import annotations

import logging
import os
from typing import Final

import great_expectations as gx
import pytest
from great_expectations.data_context import CloudDataContext

LOGGER: Final = logging.getLogger("tests")


@pytest.fixture(scope="module")
def context() -> CloudDataContext:
    context = gx.get_context(
        cloud_mode=True,
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    return context