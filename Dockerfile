FROM --platform=linux/amd64 python:3.10.12-slim
WORKDIR /app/

# File Structure
# /app
# ├── great_expectations_cloud/
# │         ├── agent/
# │         └── ...
# ├── pyproject.toml
# ├── poetry.lock
# └── README.md

# Disable in-memory buffering of application logs
#   https://docs.python.org/3/using/cmdline.html#envvar-PYTHONUNBUFFERED
ENV PYTHONUNBUFFERED=1
ENV POETRY_CACHE_DIR=/tmp/pypoetry

RUN pip --no-cache-dir install poetry==1.6.1
COPY pyproject.toml poetry.lock .

# Recommended approach for caching build layers with poetry
#   --no-root: skips project source, --no-directory: skips local dependencies
#   https://python-poetry.org/docs/faq
RUN poetry install --with sql --without dev --no-root --no-directory

COPY README.md README.md
COPY great_expectations_cloud .

RUN poetry install --with sql --without dev --sync && rm -rf POETRY_CACHE_DIR

CMD ["poetry", "run", "gx-agent"]
