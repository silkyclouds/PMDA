# Dockerfile
FROM python:3.11-slim

ENV PMDA_CONFIG_DIR=/config

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ffmpeg \
      sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir \
      flask \
      requests \
      openai

EXPOSE 6000

ENTRYPOINT ["python", "pmda.py"]
