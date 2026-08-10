FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY bridge ./bridge

RUN pip install --no-cache-dir .

RUN useradd --create-home glasspipe \
    && mkdir -p /data/bridge \
    && chown -R glasspipe:glasspipe /data/bridge
USER glasspipe

ENTRYPOINT ["python", "-m", "bridge.main"]
