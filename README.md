# GX cloud

[![PyPI](https://img.shields.io/pypi/v/great_expectations_cloud)](https://pypi.org/project/great-expectations_cloud/#history)
[![Docker Pulls](https://img.shields.io/docker/pulls/greatexpectations/agent)](https://hub.docker.com/r/greatexpectations/agent)
[![ci](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml/badge.svg?event=schedule)](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/great-expectations/cloud/main.svg)](https://results.pre-commit.ci/latest/github/great-expectations/cloud/main)
[![codecov](https://codecov.io/gh/great-expectations/cloud/graph/badge.svg?token=8WNA5ti8nm)](https://codecov.io/gh/great-expectations/cloud)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

## Quick Start

To use the GX Agent, you will need to have a Great Expectations Cloud account. You can sign up for free at [https://app.greatexpectations.io](https://app.greatexpectations.io).

Deployment instructions for the GX Agent can be found in the [GX Cloud documentation](https://docs.greatexpectations.io/docs/cloud/deploy/deploy_gx_agent).

## Dev Setup

The following instructions are for those who are contributing to the GX Agent, to deploy and use the GX agent please see the Quick Start section above.

See also [CONTRIBUTING.md](https://github.com/great-expectations/cloud/blob/main/CONTRIBUTING.md)

1. [Install or upgrade `poetry` to the latest version](https://python-poetry.org/docs/#installing-with-pipx)
   - `pipx install poetry` or `pipx upgrade poetry`
2. Set up virtual environment and install dependencies
   - `poetry sync`
3. Activate your virtual environment
   - `eval $(poetry env activate)`
4. Set up precommit hooks
   - `pre-commit install`

### Troubleshooting

If you run into issues, you can try `pipx reinstall-all`

### Running locally for development

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
  --version             Show the GX Agent version.
```

#### Set ENV variables

`GX_CLOUD_ACCESS_TOKEN`
`GX_CLOUD_ORGANIZATION_ID`

### Start the GX Agent

If you intend to run the GX Agent against local services (Cloud backend or datasources) run the Agent outside of the container.

```
gx-agent
```

### Developer Tasks

Common developer tasks are available via `invoke` (defined in `tasks.py`).

`invoke --list` to see available tasks.

#### Synchronize Dependencies

To ensure you are using the latest version of the core and development dependencies run `poetry install --sync`.
Also available as an invoke task.

```console
invoke deps
```

#### Updating `poetry.lock` dependencies

Use the latest version of poetry
```console
pipx upgrade poetry
```

The dependencies installed in our CI and the Docker build step are determined by the [poetry.lock file](https://python-poetry.org/docs/basic-usage/#installing-with-poetrylock).

[To update only a specific dependency](https://python-poetry.org/docs/cli/#update) (such as `great_expectations`) ...

```console
poetry update great_expectations
```

[To resolve and update all dependencies ...](https://python-poetry.org/docs/cli/#lock)

```console
poetry lock
```

In either case, the updated `poetry.lock` file must be committed and merged to main.

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

The contents from [/examples/agent/data](https://github.com/great-expectations/cloud/tree/main/examples/agent/data) will be copied to `/data` for the Docker container.

#### Adding an action to the Agent

1. Make a new action in `great_expectations_cloud/agent/actions/` in a separate file.
2. Register your action in the file it was created in using `great_expectations_cloud.agent.event_handler.register_event_action()`. Register for the major version of GX Core that the action applies to, e.g. `register_event_action("1", RunCheckpointEvent, RunCheckpointAction)` registers the action for major version 1 of GX Core (e.g. 1.0.0).
3. Import your action in `great_expectations_cloud/agent/actions/__init__.py`

Note: The Agent is core-version specific but this registration mechanism allows us to preemptively work on actions for future versions of GX Core while still supporting the existing latest major version.

### Release Process

#### Versioning

This is the version that will be used for the Docker image tag as well.

_Standard Release_:
The versioning scheme is `YYYYMMDD.{release_number}` where:

- the date is the date of the release
- the release number starts at 0 for the first release of the day
- the release number is incremented for each release within the same day

For example: `20240402.0`

_Pre-release_:
The versioning scheme is `YYYYMMDD.{release_number}.dev{dev_number}`

- the date is the date of the release
- the dev number starts at 0 for the first pre-release of the day
- the dev number is incremented for each pre-release within the same day
- the release number is the release that this pre-release is for

For example: `20240403.0.dev0` is the first pre-release for the `20240403.0` release.

For example, imagine the following sequence of releases given for a day with two releases:

- `20240403.0.dev0`
- `20240403.0.dev1`
- `20240403.0`
- `20240403.1.dev0`
- `20240403.1`

There can be days with no standard releases, only pre-releases or days with no pre-release or standard release at all.

#### Pre-releases

Pre-releases are completed automatically with each merge to the `main` branch.
The version is updated in `pyproject.toml` and a pre-release is created on PyPi.
A new Docker tag will also be generated and pushed to [Docker Hub](https://hub.docker.com/r/greatexpectations/agent)

**Manual Pre-releases**

NOTE: CI will automatically create pre-releases on merges to `main`. Instead of manually creating pre-releases, consider using the CI process. This is only for exceptional cases.

To manually create a pre-release, run the following command to update the version in `pyproject.toml` and then merge it to `main` in a standalone PR:

```console
invoke pre-release
```

This will create a new pre-release version. On the next merge to `main`, the release will be uploaded to PyPi.
A new Docker tag will also be generated and pushed to [Docker Hub](https://hub.docker.com/r/greatexpectations/agent)

#### Releases

Releases will be completed on a regular basis by the maintainers of the project and with any release of [GX Core](https://github.com/great-expectations/great_expectations)

For maintainers, to create a release, run the following command to update the version in `pyproject.toml` and then
merge it to `main` in a standalone PR:

```console
invoke release
```

This will create a new release version. On the next merge to `main`, the release will be uploaded to PyPi.
A new Docker tag will also be generated and pushed to [Docker Hub](https://hub.docker.com/r/greatexpectations/agent). In addition, releases will be tagged with `stable` and `latest` tags.

#### GitHub Workflow for releasing

We use the GitHub Actions workflow to automate the release and pre-release process. There are two workflows involved:

1. [CI](https://github.com/great-expectations/cloud/blob/main/.github/workflows/ci.yaml) - This workflow runs on each pull request and will update the version in `pyproject.toml` to the pre-release version if the version is not already manually updated in the PR. It will also run the tests and linting.

2. [Containerize Agent](https://github.com/great-expectations/cloud/blob/main/.github/workflows/containerize-agent.yaml) - This workflows runs on merge with `main` and will create a new Docker image and push it to Docker Hub and PyPi. It uses the version in `pyproject.toml`.

A visual representation of the workflow is shown [here](https://github.com/great-expectations/cloud/blob/main/.github/workflows/agent_release_workflows.png)

### Dependabot and Releases/Pre-releases

GitHub's Dependabot regularly checks our dependencies for vulnerabilty-based updates and proposes PRs to update dependency version numbers accordingly.

Dependabot may only update the `poetry.lock` file. If only changes to `poetry.lock` are made, this may be done in a pre-release.

For changes to the `pyproject.toml` file:

- If the version of a tool in the `[tool.poetry.group.dev.dependencies]` group is updated, this may be done without any version bump.
  - While doing this, make sure any version references in the pre-commit config `.pre-commit-config.yaml` are kept in sync (e.g., ruff).
- For other dependency updates or package build metadata changes, a new release should be orchestrated. This includes updates in the following sections:
  - `[tool.poetry.dependencies]`
  - `[tool.poetry.group.*.dependencies]` where `*` is the name of the group (not including the `dev` group)
- To stop the auto-version bump add the `no version bump` label to the PR. Use this when:
  - Only modifying dev dependencies.
  - Only modifying tests that do not change functionality.

NOTE: Dependabot does not have permissions to access secrets in our CI. You may notice that integration tests fail on PRs that dependabot creates. If you add a commit (as a GX member) to the PR, the tests will run again and pass because they now have access to the secrets. That commit can be anything, including an empty commit e.g. `git commit -m "some message" --allow-empty`.
