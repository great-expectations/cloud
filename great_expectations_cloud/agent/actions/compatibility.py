from __future__ import annotations

from typing import Protocol

import great_expectations as gx
from packaging.version import Version
from packaging.version import parse as parse_version
from typing_extensions import override


class VersionRunner(Protocol):
    def run_checkpoint(self) -> None:
        ...

    def run_check_datasource_config(self) -> None:
        ...


_VERSION_RUNNERS = {}


def register_version_runner(version: str, runner: VersionRunner) -> None:
    _VERSION_RUNNERS[version] = runner


class V0Runner(VersionRunner):
    @override
    def run_checkpoint(self) -> None:
        # ***** THIS IS WHERE THE 0.18 SPECIFIC CODE WOULD GO, MOVED FROM THE ACTION *****
        print("Running run_checkpoint major version 0 (0.18)")

    @override
    def run_check_datasource_config(self) -> None:
        # ***** THIS IS WHERE THE 0.18 SPECIFIC CODE WOULD GO, MOVED FROM THE ACTION *****
        print("Running run_check_datasource_config major version 0 (0.18)")


register_version_runner("0", V0Runner())


class V1Runner(VersionRunner):
    @override
    def run_checkpoint(self) -> None:
        # ***** THIS IS WHERE THE 1.0 SPECIFIC CODE WOULD GO, MOVED FROM THE ACTION *****
        print("Running run_checkpoint major version 1 (1.0)")

    @override
    def run_check_datasource_config(self) -> None:
        # ***** THIS IS WHERE THE 1.0 SPECIFIC CODE WOULD GO, MOVED FROM THE ACTION *****
        print("Running run_check_datasource_config major version 1 (1.0)")


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
