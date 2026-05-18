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
ENV PMDA_REDIS_HOST=127.0.0.1
ENV PMDA_REDIS_PORT=6379
ENV PMDA_REDIS_DB=0

# libchromaprint-tools provides fpcalc for pyacoustid (AcousticID fingerprinting).
# Split heavy Debian installs so multi-arch buildx jobs do not exhaust APT archive space
# on smaller builder roots, especially for arm64.
RUN mkdir -p /etc/postgresql-common && \
    printf 'create_main_cluster = false\n' > /etc/postgresql-common/createcluster.conf && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      git \
      curl \
      ffmpeg \
      git \
      sqlite3 \
      docker-cli \
      docker-compose \
      libchromaprint-tools \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      tesseract-ocr \
      tesseract-ocr-eng \
      tesseract-ocr-fra \
      tesseract-ocr-deu \
      tesseract-ocr-spa \
      tesseract-ocr-ita \
      postgresql \
      postgresql-contrib \
      redis-server \
      docker.io \
      docker-compose \
      zstd \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/bin \
    && curl -fsSL https://ollama.com/download/ollama-linux-amd64.tar.zst \
      | zstd -d \
      | tar -xOf - bin/ollama > /usr/local/bin/ollama \
    && chmod +x /usr/local/bin/ollama

RUN set -eux; \
    case "${TARGETARCH}${TARGETVARIANT:+/${TARGETVARIANT}}" in \
      amd64) docker_arch='x86_64' ;; \
      arm64) docker_arch='aarch64' ;; \
      arm/v7) docker_arch='armhf' ;; \
      *) echo "Unsupported Docker CLI architecture: ${TARGETARCH}${TARGETVARIANT:+/${TARGETVARIANT}}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://download.docker.com/linux/static/stable/${docker_arch}/docker-26.1.4.tgz" \
      | tar -xz -C /tmp docker/docker; \
    mv /tmp/docker/docker /usr/local/bin/docker; \
    chmod +x /usr/local/bin/docker; \
    rm -rf /tmp/docker

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
RUN npm install -g @openai/codex
RUN chmod +x /app/scripts/entrypoint_allinone.sh

EXPOSE 5005

ENTRYPOINT ["/app/scripts/entrypoint_allinone.sh"]
