from __future__ import annotations

import datetime
import functools
import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Final, Literal, MutableMapping

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
DOCKERFILE_PATH: Final[str] = "./Dockerfile"


@functools.lru_cache(maxsize=8)
def _get_pyproject_tool_dict(
    tool_key: Literal["poetry", "black", "ruff", "mypy"] | None = None,
) -> MutableMapping[str, Any]:
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
    """Format code with ruff format"""
    cmds = ["ruff", "format", "."]
    if check:
        cmds.append("--check")
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(
    help={
        "check": "Check code without fixing it",
        "unsafe-fixes": "Apply 'un-safe' fixes. See https://docs.astral.sh/ruff/linter/#fix-safety",
    }
)
def lint(ctx: Context, check: bool = False, unsafe_fixes: bool = False) -> None:
    """Lint and fix code with ruff"""
    cmds = ["ruff", "."]
    if not check:
        cmds.append("--fix")
    if unsafe_fixes:
        cmds.extend(["--unsafe-fixes", "--show-fixes"])
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(
    aliases=("build",),
    help={
        "check": "Lint Dockerfile using hadolint tool",
        "run": "Run the Docker container. Inject .env file",
    },
)
def docker(
    ctx: Context,
    check: bool = False,
    run: bool = False,
    tag: str = "greatexpectations/agent:develop",
) -> None:
    """
    Docker tasks

    If no options are provided, the default behavior is to build the Docker image.
    """
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
    elif run:
        cmds = ["docker", "run", "--env-file .env", "--rm", "-t", tag]
    else:
        cmds = ["docker", "build", "-f", DOCKERFILE_PATH, "-t", tag, "."]

    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(
    aliases=("types",),
)
def type_check(ctx: Context, install_types: bool = False, check: bool = False) -> None:
    """Type check code with mypy"""
    cmds = ["mypy"]
    if install_types:
        cmds.append("--install-types")
    if check:
        cmds.extend(["--pretty", "--warn-unused-ignores"])
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(aliases=("sync",))
def deps(ctx: Context) -> None:
    """Sync dependencies with poetry lock file"""
    # using --with dev incase poetry changes the default behavior
    cmds = ["poetry", "install", "--sync", "--with", "dev"]
    ctx.run(" ".join(cmds), echo=True, pty=True)


def _get_local_version() -> Version:
    return Version(_get_pyproject_tool_dict("poetry")["version"])


@invoke.task(aliases=("version",))
def get_version(ctx: Context) -> None:
    """Print the current package version and exit."""
    print(_get_local_version())


@functools.lru_cache(maxsize=1)
def _get_latest_version() -> Version:
    r = requests.get("https://pypi.org/pypi/great-expectations-cloud/json", timeout=10)
    r.raise_for_status()
    version = Version(r.json()["info"]["version"])
    return version


@functools.lru_cache(maxsize=2)
def _get_all_versions() -> list[Version]:
    r = requests.get("https://pypi.org/pypi/great-expectations-cloud/json", timeout=10)
    r.raise_for_status()
    return [Version(v) for v in r.json()["releases"].keys()]


@functools.lru_cache(maxsize=2)
def _get_latest_versions() -> tuple[Version, Version]:
    all_versions = _get_all_versions()
    pre_releases = sorted(v for v in all_versions if v.is_prerelease)
    releases = sorted(v for v in all_versions if not v.is_prerelease)
    return max(pre_releases), max(releases)


def _get_current_date() -> str:
    return datetime.date.today().strftime("%Y%m%d")  # noqa: DTZ011 # timezone agnostic, local time OK.


def _new_release_version(
    version_: Version,
    latest_version: Version,
    latest_pre_release_version: Version,
    current_date: str,
) -> Version:
    raise NotImplementedError


def _new_pre_release_version(
    version_: Version,
    latest_version: Version,
    latest_pre_release_version: Version,
    current_date: str,
) -> Version:
    raise NotImplementedError


def bump_version(
    version_: Version,
    latest_version: Version,
    latest_pre_release_version: Version,
    pre_release: bool,
    current_date: str,
) -> Version:
    if pre_release:
        new_version = _new_pre_release_version(
            version_, latest_version, latest_pre_release_version, current_date
        )
    else:
        new_version = _new_release_version(
            version_, latest_version, latest_pre_release_version, current_date
        )

    # if not pre_release:
    #     # standard release - remove the dev component if it exists
    #     if version_.dev:
    #         new_version = Version(f"{version_.major}.{version_.minor}.{version_.micro}")
    #     else:
    #         new_version = Version(f"{version_.major}.{version_.minor}.{version_.micro + 1}")
    # elif version_.dev:
    #     # bump an existing pre-release version
    #     new_version = Version(
    #         f"{version_.major}.{version_.minor}.{version_.micro}.dev{version_.dev + 1}"
    #     )
    # else:
    #     # create the first pre-release version
    #     new_version = Version(f"{version_.major}.{version_.minor}.{version_.micro + 1}.dev0")

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


def _version_bump(ctx: Context, pre: bool = False, standard: bool = False) -> None:
    """Bump project version and release to pypi."""
    local_version = _get_local_version()
    print(f"local: \t\t{local_version}")
    latest_pre_release_version, latest_release_version = _get_latest_versions()
    print(f"pypi latest: \t{latest_release_version}")

    if standard:
        pre = False
    elif not pre:
        # if not explicitly set to standard release, default to pre-release
        pre = True

    print("\n  bumping version ...", end=" ")

    new_version = bump_version(
        local_version,
        pre_release=pre,
        latest_version=latest_release_version,
        latest_pre_release_version=latest_pre_release_version,
        current_date=_get_current_date(),
    )
    _update_version(new_version)
    print(f"\nnew version: \t{new_version}")
    print(f"\n✅ {new_version} will be published to pypi on next merge to main")


@invoke.task(
    help={
        "pre": "Bump the pre-release version (Default)",
        "standard": "Bump the non pre-release micro version",
    },
)
def version_bump(ctx: Context, pre: bool = False, standard: bool = False) -> None:
    """Bump project version and release to pypi."""
    _version_bump(ctx, pre=pre, standard=standard)


@invoke.task(name="release", aliases=("version-bump --standard",))
def release(ctx: Context) -> None:
    """Bump project release version and release to pypi."""
    _version_bump(ctx, pre=False, standard=True)


@invoke.task(name="pre-release", aliases=("version-bump --pre",))
def prerelease(ctx: Context) -> None:
    """Bump project pre-release version and release to pypi."""
    _version_bump(ctx, pre=True, standard=False)
