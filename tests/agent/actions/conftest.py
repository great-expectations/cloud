from __future__ import annotations

import pytest


@pytest.fixture
def org_id() -> str:
    return "1a6b0e6b-5943-4225-a23c-d154fee937a2"


@pytest.fixture
def org_id_different_from_context() -> str:
    return "f67dd790-fe65-4f8a-bbc8-2c11f20e34b8"
