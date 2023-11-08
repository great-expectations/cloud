FROM --platform=linux/amd64 python:3.10.12-slim
WORKDIR /app/

# Disable in-memory buffering of application logs
#   https://docs.python.org/3/using/cmdline.html#envvar-PYTHONUNBUFFERED
ENV PYTHONUNBUFFERED=1

RUN pip --no-cache-dir install poetry==1.6.1
COPY pyproject.toml poetry.lock .

# Recommended approach for caching build layers with poetry
#   --no-root: skips project source, --no-directory: skips local dependencies
#   https://python-poetry.org/docs/faq
RUN poetry install --with sql --without dev --no-root --no-directory

COPY great_expectations_cloud great_expectations_cloud

RUN poetry install -vv --with sql --without dev --sync
RUN tree
CMD ["poetry", "run", "gx-agent"]
