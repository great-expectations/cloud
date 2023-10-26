import logging
import pathlib
from pprint import pformat as pf
from typing import Final, Mapping

import pytest
import tomlkit

# import tomli
from packaging.version import Version
from ruamel.yaml import YAML

yaml = YAML(typ="safe")

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

PROJECT_ROOT: Final = pathlib.Path(__file__).parent.parent
PYPROJECT_TOML: Final = PROJECT_ROOT / "pyproject.toml"


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
def pre_commit_config_repos() -> Mapping[str, dict]:
    """
    Extract the repos from the pre-commit config file and return a dict with the
    repo source url as the key
    """
    pre_commit_config = PROJECT_ROOT / ".pre-commit-config.yaml"
    yaml_dict = yaml.load(pre_commit_config.read_bytes())
    LOGGER.info(f".pre-commit-config.yaml ->\n {pf(yaml_dict, depth=1)}")
    return {repo.pop("repo"): repo for repo in yaml_dict["repos"]}


@pytest.fixture
def poetry_lock_packages() -> Mapping[str, dict]:
    poetry_lock = PROJECT_ROOT / "poetry.lock"
    toml_doc = tomlkit.loads(poetry_lock.read_text())
    LOGGER.info(f"poetry.lock ->\n {pf(toml_doc, depth=1)[:1000]}...")
    packages = toml_doc["package"].unwrap()
    return {pkg.pop("name"): pkg for pkg in packages}


def test_pre_commit_versions_are_in_sync(
    pre_commit_config_repos: Mapping, poetry_lock_packages: Mapping
):
    repo_package_lookup = {
        "https://github.com/psf/black": "black",
        "https://github.com/charliermarsh/ruff-pre-commit": "ruff",
    }
    for repo, package in repo_package_lookup.items():
        pre_commit_version = Version(pre_commit_config_repos[repo]["rev"])
        poetry_lock_version = Version(poetry_lock_packages[package]["version"])
        print(f"{package} ->\n  {pre_commit_version=}\n  {poetry_lock_version=}\n")
        assert pre_commit_version == poetry_lock_version, (
            f"{package} Version mismatch."
            " Make sure the .pre-commit config and poetry versions are in sync."
        )


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "-rEf"])
