from __future__ import annotations

import json
import logging
from typing import ClassVar

import requests
from packaging import version
from typing_extensions import TypedDict

logger = logging.getLogger(__name__)


class _PyPIPackageInfo(TypedDict):
    version: str


class _PyPIPackageData(TypedDict):
    info: _PyPIPackageInfo


# here we are. This is the file where we are going to add the code to check for the latest version of great_expectations
class _VersionChecker:
    _BASE_PYPI_URL: ClassVar[str] = "https://pypi.org/pypi"
    _PYPI_GX_AGENT_ENDPOINT: ClassVar[str] = f"{_BASE_PYPI_URL}/great-expectations-cloud/json"

    def __init__(self) -> None:
        self._user_version = version.Version("0.1.0")

    def check_if_using_latest_gx_agent(self) -> bool:
        pypi_version: version.Version | None
        pypi_version = self._get_latest_version_from_pypi()
        if not pypi_version:
            logger.debug("Could not compare with latest PyPI version; skipping check.")
        return True

    def _get_latest_version_from_pypi(self) -> version.Version | None:
        response_json: _PyPIPackageData | None = None
        try:
            response = requests.get(self._PYPI_GX_AGENT_ENDPOINT)
            response.raise_for_status()
            response_json = response.json()
        except json.JSONDecodeError as jsonError:
            logger.debug(f"Failed to parse PyPI API response into JSON: {jsonError}")
        except requests.HTTPError as http_err:
            logger.debug(f"An HTTP error occurred when trying to hit PyPI API: {http_err}")
        except requests.Timeout as timeout_exc:
            logger.debug(f"Failed to hit the PyPI API due a timeout error: {timeout_exc}")

        if not response_json:
            return None

        # Structure should be guaranteed but let's be defensive in case PyPI changes.
        info = response_json.get("info", {})
        pkg_version = info.get("version")
        print(f"hi will this is hte pkg version {pkg_version}")
        if not pkg_version:
            logger.debug("Successfully hit PyPI API but payload structure is not as expected.")
            return None

        pypi_version = version.Version(pkg_version)
        # update the _LATEST_GX_VERSION_CACHE
        return pypi_version
