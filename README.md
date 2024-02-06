# cloud

[![PyPI](https://img.shields.io/pypi/v/great_expectations_cloud)](https://pypi.org/project/great-expectations_cloud/#history)
[![Docker Pulls](https://img.shields.io/docker/pulls/greatexpectations/agent)](https://hub.docker.com/r/greatexpectations/agent)
[![ci](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml/badge.svg?event=schedule)](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/great-expectations/cloud/main.svg)](https://results.pre-commit.ci/latest/github/great-expectations/cloud/main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

## Quick Start

### Python

#### Install
```console
pip install great_expectations_cloud
```
##### Optional Dependencies
```console
pip install 'great_expectations_cloud[sql]'
```

```console
$ gx-agent --help
usage: gx-agent [-h] [--log-level LOG_LEVEL] [--skip-log-file SKIP_LOG_FILE] [--log-cfg-file LOG_CFG_FILE] [--version]

optional arguments:
  -h, --help            show this help message and exit
  --log-level LOG_LEVEL
                        Level of logging to use. Defaults to WARNING.
  --skip-log-file SKIP_LOG_FILE
                        Skip writing debug logs to a file. Defaults to False. Does not affect logging to stdout/stderr.
  --log-cfg-file LOG_CFG_FILE
                        Path to a logging configuration json file. Supersedes --log-level and --skip-log-file.
  --version             Show the gx agent version.
```

#### Set env variables

`GX_CLOUD_ACCESS_TOKEN`
`GX_CLOUD_ORGANIZATION_ID`

### Start the Agent

If you intend to run the agent against local services (Cloud backend or datasources) run the agent outside of the container.

```
gx-agent
```

### Docker

[Building and running the Agent with Docker](#building-and-running-the-gx-agent-image)

## Dev Setup

See also [CONTRIBUTING.md](/CONTRIBUTING.MD)

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

#### Updating `poetry.lock` dependencies

The dependencies installed in our CI and the docker build step are determined by the [poetry.lock file](https://python-poetry.org/docs/basic-usage/#installing-with-poetrylock).

[To update only a specific dependency](https://python-poetry.org/docs/cli/#update) (such as `great_expectations`) ...
```console
poetry update great_expectations
```

[To resolve and update all dependencies ...](https://python-poetry.org/docs/cli/#lock)
```console
poetry lock
```

In either case, the updated `poetry.lock` file must be committed and merged to main.


#### Release to PyPI and Docker

To release a new version to PyPI the version must be incremented.
New versions are automatically published to PyPI when merging to `main`.
```console
invoke version-bump
```

A new docker tag will also be generated and pushed to [Docker Hub](https://hub.docker.com/r/greatexpectations/agent).

#### Building and Running the GX Agent Image

To build the GX Agent Docker image, run the following in the root dir:

```
invoke docker
```

Running the GX Agent:

```
invoke docker --run
```
or
```
docker run --env GX_CLOUD_ACCESS_TOKEN="<GX_TOKEN>" --env GX_CLOUD_ORGANIZATION_ID="<GX_ORG_ID>" gx/agent
```

Now go into GX Cloud and issue commands for the GX Agent to run, such as generating an Expectation Suite for a Data Source.

> Note if you are pushing out a new image update the image tag version in `containerize-agent.yaml`. The image will be built and pushed out via GitHub Actions.


#### Example Data
The contents from [/examples/agent/data](/examples/agent/data/) will be copied to `/data` for the docker container.
