# ─── Stage 1: build frontend (React/Vite) ─────────────────────────────────────
# Build frontend on the builder host architecture only; output is static assets
# and can be reused for all target runtime architectures.
FROM --platform=$BUILDPLATFORM node:20-alpine AS frontend-build
WORKDIR /fe
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# ─── Stage 2: PMDA backend + integrated frontend ─────────────────────────────
FROM python:3.11-slim
ARG TARGETARCH
ARG TARGETVARIANT

ENV PMDA_CONFIG_DIR=/config
ENV PMDA_PG_VERSION=15
ENV PMDA_PGDATA=/config/postgres-data
ENV PMDA_PG_HOST=127.0.0.1
ENV PMDA_PG_PORT=5432
ENV PMDA_PG_DB=pmda
ENV PMDA_PG_USER=pmda
ENV PMDA_PG_PASSWORD=pmda
ENV PMDA_REDIS_HOST=127.0.0.1
ENV PMDA_REDIS_PORT=6379
ENV PMDA_REDIS_DB=0

# libchromaprint-tools provides fpcalc for pyacoustid (AcousticID fingerprinting)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ffmpeg \
      sqlite3 \
      libchromaprint-tools \
      postgresql \
      postgresql-contrib \
      redis-server \
    && rm -rf /var/lib/apt/lists/*

# arm/v7 lacks wheels for some Python deps (e.g. Pillow/cffi), so compile toolchain is needed.
RUN if [ "$TARGETARCH" = "arm" ] && [ "$TARGETVARIANT" = "v7" ]; then \
      apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        python3-dev \
        libffi-dev \
        libjpeg-dev \
        zlib1g-dev \
        libopenjp2-7-dev \
        libtiff-dev \
        libfreetype6-dev \
        liblcms2-dev \
        libwebp-dev \
        libharfbuzz-dev \
        libfribidi-dev \
      && rm -rf /var/lib/apt/lists/*; \
    fi

WORKDIR /app
COPY . /app
# Override frontend dist with the built assets from stage 1
COPY --from=frontend-build /fe/dist /app/frontend/dist

RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x /app/scripts/entrypoint_allinone.sh

EXPOSE 5005

ENTRYPOINT ["/app/scripts/entrypoint_allinone.sh"]
