# Build context is the repo root (see docker-compose.yml): this image needs
# both the shared glasspipe_common package and the SQLMesh project it shells
# out to (transform/).
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY packages/glasspipe_common /tmp/glasspipe_common
COPY orchestration/pyproject.toml ./
COPY orchestration/orchestration ./orchestration
COPY transform /opt/transform

RUN pip install --no-cache-dir /tmp/glasspipe_common \
    && pip install --no-cache-dir "sqlmesh[clickhouse]" .

ENV DAGSTER_HOME=/opt/dagster_home
RUN mkdir -p /opt/dagster_home
COPY orchestration/dagster.yaml /opt/dagster_home/dagster.yaml
COPY orchestration/workspace.yaml /opt/dagster_home/workspace.yaml

RUN mkdir -p /opt/transform/.state \
    && useradd --create-home glasspipe \
    && chown -R glasspipe /app /opt/dagster_home /opt/transform
USER glasspipe

EXPOSE 3000
