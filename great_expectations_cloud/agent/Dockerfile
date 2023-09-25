# Build Context: root of repo

# This is utilizing a multi-stage build:
#
# +------------------+----------------------+--------------+
# |      Stage       |         Deps         | Src Included |
# +------------------+----------------------+--------------+
# | base             | apt, reqs            | No           |
# | snowflake        | apt, reqs[-dev]      | No           |
# +------------------+----------------------+--------------+
# https://docs.docker.com/build/building/multi-stage/
FROM --platform=linux/amd64 python:3.10.12-slim as base
WORKDIR /app/

RUN mkdir -p great_expectations_cloud/agent
COPY great_expectations_cloud/agent great_expectations_cloud/agent
COPY pyproject.toml pyproject.toml
COPY README.md README.md

RUN pip install poetry
RUN poetry install --without dev --sync

CMD ["poetry", "run", "gx-agent"]

from base as snowflake
RUN poetry install --extras "snowflake"
