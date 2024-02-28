from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import great_expectations as gx
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from packaging.version import Version
from packaging.version import parse as parse_version
from typing_extensions import override

from great_expectations_cloud.agent.actions.draft_datasource_config_action import (
    check_draft_datasource_config,
)
from great_expectations_cloud.agent.gx_core_bridge import GXCoreError

if TYPE_CHECKING:
    from great_expectations.data_context import CloudDataContext

if TYPE_CHECKING:
    from great_expectations_cloud.agent.actions import ActionResult
    from great_expectations_cloud.agent.models import DraftDatasourceConfigEvent


class VersionRunner(Protocol):
    def run_checkpoint(self) -> None:
        ...

    def run_check_datasource_config(
        self, context: CloudDataContext, event: DraftDatasourceConfigEvent, id: str
    ) -> ActionResult:
        ...


_VERSION_RUNNERS = {}


def register_version_runner(version: str, runner: VersionRunner) -> None:
    _VERSION_RUNNERS[version] = runner


def raise_with_error_code(e: Exception, error_code: str) -> None:
    raise GXCoreError(message=str(e), error_code=error_code) from e


class V0Runner(VersionRunner):
    @override
    def run_checkpoint(self) -> None:
        # ***** THIS IS WHERE THE 0.18 SPECIFIC CODE WOULD GO *****
        print("Running run_checkpoint major version 0 (0.18)")

    @override
    def run_check_datasource_config(
        self, context: CloudDataContext, event: DraftDatasourceConfigEvent, id: str
    ) -> ActionResult:
        try:
            return check_draft_datasource_config(context=context, event=event, id=id)
        except TestConnectionError as e:
            # TODO: Can we do better than string matching here?:
            if "Incorrect username or password was specified" in str(
                e
            ) and "snowflake.connector.errors.DatabaseError" in str(e):
                raise_with_error_code(e=e, error_code="snowflake-wrong-username-or-password")
            else:
                raise_with_error_code(e=e, error_code="generic-unhandled-error")


register_version_runner("0", V0Runner())


class V1Runner(VersionRunner):
    @override
    def run_checkpoint(self) -> None:
        # ***** THIS IS WHERE THE 1.0 SPECIFIC CODE WOULD GO *****
        print("Running run_checkpoint major version 1 (1.0)")

    @override
    def run_check_datasource_config(
        self, context: CloudDataContext, event: DraftDatasourceConfigEvent, id: str
    ) -> ActionResult:
        # TODO: This will need to be changed for 1.0
        return check_draft_datasource_config(context=context, event=event, id=id)


register_version_runner("1", V1Runner())


def get_major_version(version: str) -> str:
    """Get major version as a string. For example, "0.18.0" -> "0"."""
    parsed: Version = parse_version(version)
    return str(parsed.major)


GX_MAJOR_VERSION = get_major_version(gx.__version__)


class NoVersionImplementationError(Exception):
    pass


def lookup_runner(version: str = GX_MAJOR_VERSION) -> VersionRunner:
    try:
        return _VERSION_RUNNERS[version]
    except KeyError as e:
        raise NoVersionImplementationError(f"Version {version} not supported") from e


### TODO: This is example usage, implement on specific actions instead:
class ExampleAgentActionCheckpoint:
    def __init__(self) -> None:
        pass

    def run(self) -> None:
        runner = lookup_runner()
        runner.run_checkpoint()


class ExampleAgentActionDatasourceConfig:
    def __init__(self) -> None:
        pass

    def run(self) -> None:
        runner = lookup_runner()
        runner.run_check_datasource_config()


if __name__ == "__main__":
    print("GX_MAJOR_VERSION:", GX_MAJOR_VERSION)
    action = ExampleAgentActionCheckpoint()
    action.run()
    datasource_action = ExampleAgentActionDatasourceConfig()
    datasource_action.run()
