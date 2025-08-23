# =========================
# Builder stage
# =========================
FROM python:3.11-slim AS builder
WORKDIR /app

# Runtime/logging + poetry cache
ENV PYTHONUNBUFFERED=1 \
    POETRY_CACHE_DIR=/tmp/pypoetry

# Build-time tools only (kept out of final image)
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3-dev gcc ca-certificates curl gnupg && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry (kept in builder only)
RUN pip --no-cache-dir install setuptools==80.9.0 poetry==2.1.2 poetry-plugin-export
RUN poetry self add poetry-plugin-export

# Copy lockfiles first to maximize layer caching
COPY pyproject.toml poetry.lock ./

# Prepare dependency env via Poetry without project source (faster caching)
# - export to requirements and install into a dedicated venv
RUN poetry export --without dev --format requirements.txt --without-hashes -o /tmp/requirements.txt && \
    python -m venv /opt/venv && \
    /opt/venv/bin/pip --no-cache-dir install -r /tmp/requirements.txt

# Now add the project code and data
COPY README.md README.md
COPY great_expectations_cloud great_expectations_cloud
COPY examples/agent/data data

# Install the project itself into the venv
RUN /opt/venv/bin/pip --no-cache-dir install .

# Optional: ensure gx-agent runs
RUN /opt/venv/bin/gx-agent --help

# ---- Prepare Microsoft repo artifacts here (so final stage doesn't need curl/gnupg)
# NOTE: python:3.11-slim is Debian-based; use Debian 12 (bookworm) repo path.
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg && \
    echo "deb [arch=amd64,arm64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/mssql-release.list

# =========================
# Final (runtime) stage
# =========================
FROM python:3.11-slim
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    GX_ANALYTICS_ENABLED=false \
    ENABLE_PROGRESS_BARS=false \
    ACCEPT_EULA=Y \
    DEBIAN_FRONTEND=noninteractive \
    PATH="/opt/venv/bin:${PATH}" \
    POETRY_CACHE_DIR=/tmp/pypoetry

# Bring in Microsoft repo key/list from builder so no curl/gnupg needed here
COPY --from=builder /etc/apt/keyrings/microsoft.gpg /etc/apt/keyrings/microsoft.gpg
COPY --from=builder /etc/apt/sources.list.d/mssql-release.list /etc/apt/sources.list.d/mssql-release.list

# Install:
# - unixodbc: th the odbc driver driver
# - tini: init system that will forward signals to our process
# - msodbcsql18: the SQL Server specific odbc driver
# - libgssapi-krb5-2: required by msodbcsql18 for kerberos auth
# - libcurl4t64: includes various protocols required for successful usage of the odbc drivers
RUN apt-get update && apt-get install --no-install-recommends -y \
    libcurl4t64 unixodbc msodbcsql18 ca-certificates  tini && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the ready-to-run virtualenv and any runtime data your app expects
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/data /app/data

ENTRYPOINT ["tini", "--"]
CMD ["gx-agent"]
