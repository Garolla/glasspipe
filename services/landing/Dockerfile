# Build context is the repo root (see docker-compose.yml) so this Dockerfile
# can reach the shared glasspipe_common package as a local path dependency.
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY packages/glasspipe_common /tmp/glasspipe_common
COPY services/landing/pyproject.toml ./
COPY services/landing/landing ./landing

RUN pip install --no-cache-dir /tmp/glasspipe_common && pip install --no-cache-dir .

RUN useradd --create-home glasspipe && mkdir -p /data/parquet && chown -R glasspipe /data
USER glasspipe

ENTRYPOINT ["python", "-m", "landing.main"]
