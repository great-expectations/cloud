from __future__ import annotations

import functools
import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, Final, Literal

import invoke
import requests
import tomli
import tomli_w
from packaging.version import Version

if TYPE_CHECKING:
    from invoke.context import Context

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

PROJECT_ROOT: Final[pathlib.Path] = pathlib.Path(__file__).parent
PYPROJECT_TOML: Final[pathlib.Path] = PROJECT_ROOT / "pyproject.toml"
DOCKERFILE_PATH: Final[pathlib.Path] = PROJECT_ROOT / "agent" / " Dockerfile"


@functools.lru_cache(maxsize=8)
def _get_pyproject_tool_dict(
    tool_key: Literal["poetry", "black", "ruff", "mypy"] | None = None
) -> dict:
    """
    Load the pyproject.toml file as a dict. Optional return the config of a specific tool.
    Caches each tool's config for faster access.
    """
    assert PYPROJECT_TOML.exists()
    pyproject_dict = tomli.loads(PYPROJECT_TOML.read_text())
    LOGGER.warning(f"pyproject.toml ->\n {pf(pyproject_dict, depth=2)}")
    tool_dict = pyproject_dict["tool"]
    if tool_key:
        return tool_dict[tool_key]
    return tool_dict


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
            str(DOCKERFILE_PATH),
        ]
    else:
        cmds = ["docker", "build", "-f", str(DOCKERFILE_PATH), "-t", tag, "."]
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


@invoke.task
def deps(ctx: Context) -> None:
    """Sync dependencies with poetry lock file"""
    cmds = ["poetry", "install", "--sync"]
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


def _get_latest_version() -> Version:
    r = requests.get("https://pypi.org/pypi/great-expectations-cloud/json")
    r.raise_for_status()
    version = Version(r.json()["info"]["version"])
    return version


def _bump_version(version_: Version, full_release: bool = False) -> Version:
    if full_release:
        new_version = Version(f"{version_.major}.{version_.minor}.{version_.micro + 1}")
    elif version_.dev:
        new_version = Version(
            f"{version_.major}.{version_.minor}.{version_.micro}dev{version_.dev + 1}"
        )
    else:
        raise NotImplementedError
    return new_version


def _update_version(version_: Version | str) -> None:
    """
    Modify the pyproject.toml version.
    """
    if not isinstance(version_, Version):
        version_ = Version(version_)
    # TODO: open file once
    # TODO: use tomlkit to preserve comments and formatting
    with open(PYPROJECT_TOML, "rb") as f_in:
        full_toml_dict = tomli.load(f_in)
    with open(PYPROJECT_TOML, "wb") as f_out:
        full_toml_dict["tool"]["poetry"]["version"] = str(version_)
        tomli_w.dump(full_toml_dict, f_out)


@invoke.task
def version(ctx: Context, bump: bool = False, full_release: bool = False) -> None:
    """Bump the version of the project."""
    local_version = _get_local_version()
    print(f"local: \t\t{local_version}")
    latest_version = _get_latest_version()
    print(f"pypi latest: \t{latest_version}")

    if bump:
        print("\n  bumping version", end=" ")
        new_version = _bump_version(local_version, full_release=full_release)
        _update_version(new_version)
        print(f"\nnew version: \t{new_version}")
