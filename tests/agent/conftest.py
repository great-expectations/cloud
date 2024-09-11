from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, NamedTuple, TypedDict

import pytest
from great_expectations import (  # type: ignore[attr-defined] # TODO: fix this
    __version__ as gx_version,
)
from great_expectations.data_context import CloudDataContext
from packaging.version import Version

from great_expectations_cloud.agent.models import Event

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def mock_gx_version_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mock the response from pypi.org for the great_expectations package"""

    def _mock_get_lastest_version_from_pypi(self) -> Version:
        return Version(gx_version)

    monkeypatch.setattr(
        "great_expectations.data_context._version_checker._VersionChecker._get_latest_version_from_pypi",
        _mock_get_lastest_version_from_pypi,
        raising=True,
    )


@pytest.fixture
def mock_context(mocker: MockerFixture) -> CloudDataContext:
    """Returns a `MagicMock` of a `CloudDataContext` for testing purposes."""
    return mocker.MagicMock(autospec=CloudDataContext)  # type: ignore[no-any-return] #TODO: fix this


class FakeMessagePayload(NamedTuple):
    """
    Fake message payload for testing purposes
    The real payload is a JSON string which must be parsed into an Event
    """

    event: Event
    correlation_id: str


class DataContextConfigTD(TypedDict):
    anonymous_usage_statistics: dict[str, Any]
    checkpoint_store_name: str
    datasources: dict[str, dict[str, Any]]
    stores: dict[str, dict[str, Any]]


@pytest.fixture
def data_context_config() -> DataContextConfigTD:
    """
    Return a minimal DataContext config for testing.
    This what GET /organizations/{id}/data-contexts/{id} should return.

    See also:
    https://github.com/great-expectations/great_expectations/blob/develop/tests/datasource/fluent/_fake_cloud_api.py
    """
    return {
        "anonymous_usage_statistics": {
            "data_context_id": str(uuid.uuid4()),
            "enabled": False,
        },
        "checkpoint_store_name": "default_checkpoint_store",
        "datasources": {},
        "stores": {
            "default_evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "default_expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                    "ge_cloud_credentials": {
                        "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                        "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                    },
                    "ge_cloud_resource_type": "expectation_suite",
                    "suppress_store_backend_id": True,
                },
            },
            "default_checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                    "ge_cloud_credentials": {
                        "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                        "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                    },
                    "ge_cloud_resource_type": "checkpoint",
                    "suppress_store_backend_id": True,
                },
            },
            "default_validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "GXCloudStoreBackend",
                    "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                    "ge_cloud_credentials": {
                        "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                        "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                    },
                    "ge_cloud_resource_type": "validation_result",
                    "suppress_store_backend_id": True,
                },
            },
        },
    }
