from __future__ import annotations

import functools
import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, Final, Literal, MutableMapping

import invoke
import requests
import tomlkit
from packaging.version import Version
from tomlkit import TOMLDocument

if TYPE_CHECKING:
    from invoke.context import Context

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

PROJECT_ROOT: Final[pathlib.Path] = pathlib.Path(__file__).parent
PYPROJECT_TOML: Final[pathlib.Path] = PROJECT_ROOT / "pyproject.toml"
DOCKERFILE_PATH: Final[str] = "great_expectations_cloud/agent/Dockerfile"


@functools.lru_cache(maxsize=8)
def _get_pyproject_tool_dict(
    tool_key: Literal["poetry", "black", "ruff", "mypy"] | None = None
) -> MutableMapping:
    """
    Load the pyproject.toml file as a dict. Optional return the config of a specific tool.
    Caches each tool's config for faster access.
    """
    assert PYPROJECT_TOML.exists()
    pyproject_doc: TOMLDocument = tomlkit.loads(PYPROJECT_TOML.read_text())
    LOGGER.debug(f"pyproject.toml ->\n {pf(pyproject_doc, depth=2)}")
    tool_doc = pyproject_doc["tool"]
    assert isinstance(tool_doc, MutableMapping), f"got {type(tool_doc)}"
    if tool_key:
        return tool_doc[tool_key]  # type: ignore[return-value] # always Mapping type
    return tool_doc


@invoke.task
def python_build(ctx: Context, check: bool = False) -> None:
    """Build Python distibution files"""

    cmds = ["poetry", "build"]
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task
def fmt(ctx: Context, check: bool = False) -> None:
    """Format code with black"""
    cmds = ["black", "."]
    if check:
        cmds.append("--check")
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task
def lint(ctx: Context, check: bool = False) -> None:
    """Lint and fix code with ruff"""
    cmds = ["ruff", "."]
    if not check:
        cmds.append("--fix")
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task
def docker(ctx: Context, check: bool = False, tag: str = "greatexpectations/agent:develop") -> None:
    """Lint Dockerfile using hadolint tool"""
    if check:
        cmds = [
            "docker",
            "run",
            "--rm",
            "-i",
            "hadolint/hadolint",
            "hadolint",
            "--failure-threshold",
            "warning",
            "--ignore",
            "DL3029",  # Revisit support for arm platform builds https://github.com/hadolint/hadolint/wiki/DL3029
            "-",
            "<",
            DOCKERFILE_PATH,
        ]
    else:
        cmds = ["docker", "build", "-f", DOCKERFILE_PATH, "-t", tag, "."]
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(
    aliases=["types"],
)
def type_check(ctx: Context, install_types: bool = False, check: bool = False) -> None:
    """Type check code with mypy"""
    cmds = ["mypy"]
    if install_types:
        cmds.append("--install-types")
    if check:
        cmds.extend(["--pretty", "--warn-unused-ignores"])
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(aliases=["sync"])
def deps(ctx: Context) -> None:
    """Sync dependencies with poetry lock file"""
    # using --with dev incase poetry changes the default behavior
    cmds = ["poetry", "install", "--sync", "--with", "dev"]
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task
def build(ctx: Context) -> None:
    """Build GX Agent Image"""
    cmds = [
        "docker",
        "buildx",
        "build",
        "-t",
        "greatexpectations/agent",
        "-f",
        DOCKERFILE_PATH,
        ".",
    ]
    ctx.run(" ".join(cmds), echo=True, pty=True)


def _get_local_version() -> Version:
    return Version(_get_pyproject_tool_dict("poetry")["version"])


@functools.lru_cache(maxsize=1)
def _get_latest_version() -> Version:
    r = requests.get("https://pypi.org/pypi/great-expectations-cloud/json")
    r.raise_for_status()
    version = Version(r.json()["info"]["version"])
    return version


def _bump_version(version_: Version, pre_release: bool) -> Version:
    if not pre_release:
        # standard release
        new_version = Version(f"{version_.major}.{version_.minor}.{version_.micro + 1}")
    elif version_.dev:
        new_version = Version(
            f"{version_.major}.{version_.minor}.{version_.micro}.dev{version_.dev + 1}"
        )
    else:
        new_version = Version(f"{version_.major}.{version_.minor}.{version_.micro}.dev1")

    # check that the number of components is correct
    expected_components: int = 4 if new_version.is_prerelease else 3
    components = str(new_version).split(".")
    assert (
        len(components) == expected_components
    ), f"expected {expected_components} components; got {components}"
    return new_version


def _update_version(version_: Version | str) -> None:
    """
    Modify the pyproject.toml version.
    """
    if not isinstance(version_, Version):
        version_ = Version(version_)
    # TODO: open file once
    with open(PYPROJECT_TOML, "rb") as f_in:
        toml_doc = tomlkit.load(f_in)
    with open(PYPROJECT_TOML, "w") as f_out:
        toml_doc["tool"]["poetry"]["version"] = str(version_)  # type: ignore[index] # always a str
        tomlkit.dump(toml_doc, f_out)


@invoke.task(
    help={
        "pre": "Bump the pre-release version (Default)",
        "standard": "Bump the non pre-release micro version",
    }
)
def version_bump(ctx: Context, pre: bool = False, standard: bool = False) -> None:
    """Bump project version."""
    local_version = _get_local_version()
    print(f"local: \t\t{local_version}")
    latest_version = _get_latest_version()
    print(f"pypi latest: \t{latest_version}")

    if standard:
        pre = False
    elif not pre:
        # if not explicitly set to standard release, default to pre-release
        pre = True

    print("\n  bumping version ...", end=" ")
    new_version = _bump_version(local_version, pre_release=pre)
    _update_version(new_version)
    print(f"\nnew version: \t{new_version}")
    print(f"\n✅ {new_version} will be published to pypi on next merge to main")
