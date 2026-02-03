# ─── Stage 1: build frontend (React/Vite) ─────────────────────────────────────
FROM node:20-alpine AS frontend-build
WORKDIR /fe
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# ─── Stage 2: PMDA backend + integrated frontend ─────────────────────────────
FROM python:3.11-slim

ENV PMDA_CONFIG_DIR=/config

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ffmpeg \
      sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
# Override frontend dist with the built assets from stage 1
COPY --from=frontend-build /fe/dist /app/frontend/dist

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5005

ENTRYPOINT ["python", "pmda.py"]
