FROM --platform=linux/amd64 python:3.10.12-slim as base
WORKDIR /app/
COPY . .

RUN pip install poetry
RUN poetry install --sync

CMD ["gx-agent"]
