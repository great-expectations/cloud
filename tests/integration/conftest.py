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
        mode="cloud",
        cloud_base_url="http://localhost:5000",
        # cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_organization_id="0ccac18e-7631-4bdd-8a42-3c35cce574c6",
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    return context
