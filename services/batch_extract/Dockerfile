FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml ./
COPY batch_extract ./batch_extract

RUN pip install --no-cache-dir .

RUN useradd --create-home glasspipe
USER glasspipe

ENTRYPOINT ["python", "-m", "batch_extract.main"]
