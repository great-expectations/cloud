# cloud

[![PyPI](https://img.shields.io/pypi/v/great_expectations_cloud)](https://pypi.org/project/great-expectations_cloud/#history)
[![Docker Pulls](https://img.shields.io/docker/pulls/greatexpectations/agent)](https://hub.docker.com/r/greatexpectations/agent)
[![ci](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml/badge.svg?event=schedule)](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/great-expectations/cloud/main.svg)](https://results.pre-commit.ci/latest/github/great-expectations/cloud/main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

## Dev Setup

1. [Install `poetry`](https://python-poetry.org/docs/#installation)
   - [`pipx install poetry`](https://python-poetry.org/docs/#installing-with-pipx)
2. Set up virtual environment and install dependencies.
   - `poetry install --sync`
3. Activate your virtual environment.
   - `poetry shell`
4. Set up precommit hooks
   - `pre-commit install`

### Developer Tasks

Common developer tasks are available via `invoke` (defined in `tasks.py`)

`invoke --list` to see available tasks.

#### Synchronize Dependencies

To ensure you are using the latest version of the core and development dependencies run `poetry install --sync`.
Also available as an invoke task.
```console
invoke deps
```

#### Release to PyPI

To release a new version to PyPI the version must be incremented.
New versions are automatically published to PyPI when merging to `main`.
```console
invoke version-bump
```

#### Building and Running the GX Agent Image

To build the GX Agent Docker image, run the following in the root dir:

```
invoke build
```

Running the GX Agent:

```
docker run --env GX_CLOUD_ACCESS_TOKEN="<GX_TOKEN>" --env GX_CLOUD_ORGANIZATION_ID="<GX_ORG_ID>" gx/agent
```

Now go into GX Cloud and issue commands for the GX Agent to run, such as generating an Expectation Suite for a Data Source.

> Note if you are pushing out a new image update the image tag version in `containerize-agent.yaml`. The image will be built and pushed out via GitHub Actions.
