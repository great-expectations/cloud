from __future__ import annotations

import logging
import pathlib
import re
import warnings
from dataclasses import dataclass
from pprint import pformat as pf
from typing import Any, Final, Iterable, Mapping

import pytest
import tomlkit
from packaging.version import Version
from pytest import param
from ruamel.yaml import YAML
from tasks import bump_version  # local invoke tasks.py module

yaml = YAML(typ="safe")

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

PROJECT_ROOT: Final = pathlib.Path(__file__).parent.parent
PYPROJECT_TOML: Final = PROJECT_ROOT / "pyproject.toml"
CODECOV_YML: Final = PROJECT_ROOT / "codecov.yml"


@pytest.fixture
def min_gx_version() -> Version:
    # TODO: add this back once gx is pinned again
    # pyproject_dict = tomli.loads(PYPROJECT_TOML.read_text())
    # gx_version: str = pyproject_dict["tool"]["poetry"]["dependencies"][
    #     "great-expectations"
    # ].replace("^", "")
    # return Version(gx_version)
    return Version("0.17.19")


def test_great_expectations_is_installed(min_gx_version):
    import great_expectations

    assert Version(great_expectations.__version__) >= min_gx_version


@pytest.fixture
def pre_commit_config_repos() -> Mapping[str, dict[str, Any]]:
    """
    Extract the repos from the pre-commit config file and return a dict with the
    repo source url as the key
    """
    pre_commit_config = PROJECT_ROOT / ".pre-commit-config.yaml"
    yaml_dict = yaml.load(pre_commit_config.read_bytes())
    LOGGER.info(f".pre-commit-config.yaml ->\n {pf(yaml_dict, depth=1)}")
    return {repo.pop("repo"): repo for repo in yaml_dict["repos"]}


@pytest.fixture
def poetry_lock_packages() -> Mapping[str, dict[str, Any]]:
    poetry_lock = PROJECT_ROOT / "poetry.lock"
    toml_doc = tomlkit.loads(poetry_lock.read_text())
    LOGGER.info(f"poetry.lock ->\n {pf(toml_doc, depth=1)[:1000]}...")
    packages: list[dict[str, Any]] = toml_doc["package"].unwrap()  # type: ignore[assignment] # values are always list[dict]
    return {pkg.pop("name"): pkg for pkg in packages}


def test_pre_commit_versions_are_in_sync(
    pre_commit_config_repos: Mapping[str, dict[str, Any]],
    poetry_lock_packages: Mapping[str, dict[str, Any]],
):
    repo_package_lookup = {
        "https://github.com/astral-sh/ruff-pre-commit": "ruff",
    }
    for repo, package in repo_package_lookup.items():
        pre_commit_version = Version(pre_commit_config_repos[repo]["rev"])
        poetry_lock_version = Version(poetry_lock_packages[package]["version"])
        print(f"{package} ->\n  {pre_commit_version=}\n  {poetry_lock_version=}\n")
        assert pre_commit_version == poetry_lock_version, (
            f"{package} Version mismatch."
            " Make sure the .pre-commit config and poetry versions are in sync."
        )


@dataclass
class BumpVersionParams:
    id: str
    version_initial: Version
    expected_version: Version
    pre_release: bool
    latest_version: Version
    latest_pre_release_version: Version
    current_date: str

    def params(self):
        return param(
            self.version_initial,
            self.expected_version,
            self.pre_release,
            self.latest_version,
            self.latest_pre_release_version,
            self.current_date,
            id=self.id,
        )


# TODO: Clean up comments
# 1. mock _get_current_date with a fixed date
# 2. test bump_version with standard release
# 3. test bump_version with pre-release
# 4. test bump_version for standard release to pre-release
# 5. test bump_version for second standard release to pre-release
# 6. test bump_version for second standard release from pre-release
# 7. test bump_version for second standard release from release
# 8. test bump_version for transition from semver style to date based versioning
@pytest.mark.parametrize(
    [
        "version_initial",
        "expected_version",
        "pre_release",
        "latest_version",
        "latest_pre_release_version",
        "current_date",
    ],
    [
        # TODO: Old tests, make sure to cover these cases and then remove them
        # param(Version("0.0.1"), Version("0.0.2.dev0"), True, id="pre-release 0.0.1 -> 0.0.2.dev0"),
        # param(
        #     Version("0.0.1.dev1"),
        #     Version("0.0.1.dev2"),
        #     True,
        #     id="pre-release 0.0.1.dev1 -> 0.0.1.dev2",
        # ),
        # param(Version("0.0.1.dev1"), Version("0.0.1"), False, id="standard 0.0.1.dev1 -> 0.0.1"),
        # param(Version("0.0.1"), Version("0.0.2"), False, id="standard 0.0.1 -> 0.0.2"),
        # New tests
        # 2. test bump_version with standard release
        BumpVersionParams(
            # TODO: Add more descriptive id
            id="standard 20240410 -> 20240411 SHOULD WORK",
            version_initial=Version("20240410"),
            expected_version=Version("20240411"),
            pre_release=False,
            latest_version=Version("20240410"),
            latest_pre_release_version=Version("20240409.dev0"),
            current_date="20240411",
        ).params(),
        # TODO: Enable the below after adding the missing params
        # param(Version("20240410"), Version("20240411"), False, id="standard 20240410 -> 20240411"),
        # param(
        #     Version("20240411.dev0"),
        #     Version("20240411"),
        #     False,
        #     id="standard 20240411.dev0 -> 20240411",
        # ),
        # # 3. test bump_version with pre-release
        # param(
        #     Version("20240411.dev0"),
        #     Version("20240411.dev1"),
        #     True,
        #     id="pre-release 20240411.dev0 -> 20240411.dev1",
        # ),
        # param(
        #     Version("20240410.dev3"),
        #     Version("20240411.dev0"),
        #     True,
        #     id="pre-release 20240410.dev3 -> 20240411.dev0",
        # ),
        # # 4. test bump_version for standard release to pre-release
        # param(
        #     Version("20240410"),
        #     Version("20240411.dev0"),
        #     True,
        #     id="pre-release 20240410 -> 20240411.dev0",
        # ),
        # # 5. test bump_version for second standard release to pre-release
        # param(
        #     Version("20240411.1"),
        #     Version("20240411.2.dev0"),
        #     True,
        #     id="pre-release 20240411.1 -> 20240411.2.dev0",
        # ),
        # param(
        #     Version("20240411"),
        #     Version("20240411.1.dev0"),
        #     True,
        #     id="pre-release 20240411 -> 20240411.1.dev0",
        # ),
        # # 6. test bump_version for second standard release from pre-release
        # param(
        #     Version("20240411.1.dev2"),
        #     Version("20240411.2"),
        #     False,
        #     id="standard 20240411.1.dev2 -> 20240411.2",
        # ),
        # # 7. test bump_version for second standard release from release
        # param(
        #     Version("20240411.1"),
        #     Version("20240411.2"),
        #     False,
        #     id="standard 20240411.1 -> 20240411.2",
        # ),
        # # 8. test bump_version for transition from semver style to date based versioning
        # param(
        #     Version("0.0.1"),
        #     Version("20240411"),
        #     False,
        #     id="standard 0.0.1 style to date based -> 20240411",
        # ),
        # param(
        #     Version("0.0.1.dev1"),
        #     Version("20240411"),
        #     False,
        #     id="release 0.0.1.dev1 style to date based -> 20240411",
        # ),
        # param(
        #     Version("0.0.1"),
        #     Version("20240411.dev0"),
        #     True,
        #     id="pre-release 0.0.1 style to date based -> 20240411.dev0",
        # ),
        # param(
        #     Version("0.0.1.dev1"),
        #     Version("20240411.dev0"),
        #     True,
        #     id="pre-release 0.0.1.dev1 style to date based -> 20240411.dev0",
        # ),
    ],
)
def test_bump_version(
    version_initial: Version,
    expected_version: Version,
    pre_release: bool,
    latest_version: Version,
    latest_pre_release_version: Version,
    current_date: str,
):
    bumped_version = bump_version(
        version_=version_initial,
        latest_version=latest_version,
        latest_pre_release_version=latest_pre_release_version,
        pre_release=pre_release,
        current_date=current_date,
    )
    assert (
        bumped_version > version_initial
    ), "bumped version should be greater than the initial version"
    assert bumped_version == expected_version, f"Expected {expected_version}, got {bumped_version}"


@pytest.fixture
def lock_file_poetry_version() -> Version:
    poetry_lock = PROJECT_ROOT / "poetry.lock"
    captured_version: re.Match[str] | None = re.search(
        r"#.*generated by Poetry (?P<version>\d\.\d\.\d)", poetry_lock.read_text().splitlines()[0]
    )
    assert captured_version, "could not parse poetry.lock version"
    return Version(captured_version.group("version"))


class PoetryVersionOutdated(UserWarning):
    pass


@pytest.fixture
def latest_poetry_version(lock_file_poetry_version: Version) -> Version:
    import requests

    response = requests.get(
        "https://api.github.com/repos/python-poetry/poetry/releases/latest", timeout=5
    )
    response.raise_for_status()
    latest_version = Version(response.json()["tag_name"])

    if lock_file_poetry_version < latest_version:
        # warning instead of error because we don't want to break the build whenever a new version of poetry is released
        # but we do want to know about it.
        warnings.warn(
            f"The latest version of poetry is {latest_version} but the poetry.lock file was generated using {lock_file_poetry_version}."
            " Consider upgrading poetry and regenerating the lockfile.",
            category=PoetryVersionOutdated,
            stacklevel=1,
        )
    return latest_version


def test_lockfile_poetry_version(lock_file_poetry_version: Version, latest_poetry_version: Version):
    """
    This test ensures that the poetry.lock file was generated using a recent version of poetry.
    This is important because the lockfile format or dependency resolving strategy could change between versions.
    """
    print(f"{lock_file_poetry_version=}")
    print(f"{latest_poetry_version=}")
    assert lock_file_poetry_version >= Version(
        "1.7.1"
    ), "poetry.lock was generated using an older version of poetry"


class TestCoverageSettings:
    def test_flags_are_valid_markers(self):
        """
        This test ensures the codecov.yml flags are valid pytest markers from the pyproject.toml file.
        The python versions flags are an exception here 3.8, 3.10, etc. do not need to pytest markers.
        """
        pytest_markers: Iterable[str] = tomlkit.loads(PYPROJECT_TOML.read_text())["tool"]["pytest"][  # type: ignore[index,assignment]
            "ini_options"
        ]["markers"]
        codecov_dict = yaml.load(CODECOV_YML.read_text())

        invalid_flags: set[str] = set()

        for details in codecov_dict["flag_management"]["individual_flags"]:
            flag = details["name"]
            if flag not in pytest_markers:
                invalid_flags.add(flag)

        assert not invalid_flags, (
            "The following flags do not have a corresponding marker in "
            f" {PYPROJECT_TOML.name} -> `tool.pytest.ini_options`: {invalid_flags}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "-rEf"])
