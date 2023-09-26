FROM --platform=linux/amd64 python:3.10.12-slim
WORKDIR /app/

RUN mkdir -p great_expectations_cloud/agent
COPY great_expectations_cloud/agent great_expectations_cloud/agent
COPY pyproject.toml pyproject.toml
COPY README.md README.md

RUN pip --no-cache-dir install poetry==1.6.1
RUN poetry install --with sql --without dev --sync

CMD ["poetry", "run", "gx-agent"]
