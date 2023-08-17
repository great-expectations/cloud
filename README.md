# cloud

[![ci](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml/badge.svg)](https://github.com/great-expectations/cloud/actions/workflows/ci.yaml?query=branch%3Adevelop)
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
