# cloud

[![PyPI](https://img.shields.io/pypi/v/great_expectations_cloud)](https://pypi.org/project/great-expectations_cloud/#history)
[![ci](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml/badge.svg?event=schedule)](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/great-expectations/cloud/main.svg)](https://results.pre-commit.ci/latest/github/great-expectations/cloud/main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

## Dev Setup

1. [Install `poetry`](https://python-poetry.org/docs/#installation)
   - [`pipx install poetry`](https://python-poetry.org/docs/#installing-with-pipx)
2. Setup virtual environment and install dependencies.
   - `poetry install --sync`
3. Activate your virtual environment.
   - `poetry shell`
4. Setup precommit hooks
   - `pre-commit install`

### Developer Tasks

Common developer tasks are available via `invoke` (defined in `tasks.py`)

`invoke --list` to see available tasks.

#### Release to Pypi

To release a new version to pypi the version must be incremented.
New versions are automatically published to pypi when merging to `main`.
```console
invoke version-bump
```

#### Building and Running the GX Agent Image

In order to to build the GX Agent docker image run the following in the root dir:

```
invoke build
```

Running the agent:

```
docker run --env GX_CLOUD_ACCESS_TOKEN="<GX_TOKEN>" --env GX_CLOUD_ORGANIZATION_ID="<GX_ORG_ID>" gx/agent
```

Now go into GX Cloud and issue commands for the agent to run such as generating an Expectation Suite for a DataSource.

> Note if you are pushing out a new image update the image tag version in `containerize-agent.yaml`. The image will be built and pushed out via GitHub Actions.
