FROM python:3.10.14-slim
WORKDIR /app/

# File Structure:
#
# /app
# ├── great_expectations_cloud/
# │         ├── agent/
# │         └── ...
# ├── examples/
# │         └── ...
# ├── pyproject.toml
# ├── poetry.lock
# └── README.md

# Disable in-memory buffering of application logs
#   https://docs.python.org/3/using/cmdline.html#envvar-PYTHONUNBUFFERED
ENV PYTHONUNBUFFERED=1
ENV POETRY_CACHE_DIR=/tmp/pypoetry

# Required for arm64, for building psutil
RUN apt-get update && apt-get install --no-install-recommends python3-dev=3.11.2-1+b1 gcc=4:12.2.0-3 -y && rm -rf /var/lib/apt/lists/*

RUN pip --no-cache-dir install poetry==1.8.2

COPY pyproject.toml poetry.lock ./

# Recommended approach for caching build layers with poetry
#   --no-root: skips project source, --no-directory: skips local dependencies
#   https://python-poetry.org/docs/faq
RUN poetry install --with sql --without dev --no-root --no-directory

COPY README.md README.md
COPY great_expectations_cloud great_expectations_cloud
COPY examples/agent/data data

RUN poetry install --only-root && rm -rf POETRY_CACHE_DIR

ENTRYPOINT ["poetry", "run", "gx-agent"]
