FROM python:3.11-slim
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
ENV ACCEPT_EULA=Y
ENV DEBIAN_FRONTEND=noninteractive

# Linux deps and why:
#   python3-dev: required by build tools
#   gcc: required to build psutil for arm64
#   curl: required to download microsoft distribution lifts
#   gnupg: required to verify the list
#   unixodbc: required for odbc driver datasources (i.e. Microsoft SQL Server)
#   msodbcsql18: specific SQL Server odbc driver
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN apt-get update && apt-get install --no-install-recommends -y \
      python3-dev=3.13.5-1 gcc=4:14.2.0-1 \
      curl=8.14.1-2 gnupg=2.4.7-21 ca-certificates=20250419 unixodbc=2.3.12-2 && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg && \
    echo "deb [arch=amd64,arm64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    apt-get install -y msodbcsql18=18.5.1.1-1 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip --no-cache-dir install setuptools==80.9.0 poetry==2.1.2

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

# Recommended approach for caching build layers with poetry
#   --no-root: skips project source, --no-directory: skips local dependencies
#   https://python-poetry.org/docs/faq
RUN poetry install --without dev --no-root --no-directory

COPY README.md README.md
COPY great_expectations_cloud great_expectations_cloud
COPY examples/agent/data data

RUN poetry install --no-cache --only-root && rm -rf POETRY_CACHE_DIR

# Clean up all non-runtime linux deps
RUN apt-get remove -y \
    python3-dev \
    gcc \
    gcc-14 \
    cpp-14 \
    cpp \
    curl \
    gnupg

# Disable analytics in OSS
ENV GX_ANALYTICS_ENABLED=false

# Disable progress bars
ENV ENABLE_PROGRESS_BARS=false

ENTRYPOINT ["poetry", "run", "gx-agent"]
