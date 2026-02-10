#!/usr/bin/env bash
set -euo pipefail

PMDA_PG_VERSION="${PMDA_PG_VERSION:-15}"
PMDA_PGDATA="${PMDA_PGDATA:-/config/postgres-data}"
PMDA_PG_HOST="${PMDA_PG_HOST:-127.0.0.1}"
PMDA_PG_PORT="${PMDA_PG_PORT:-5432}"
PMDA_PG_DB="${PMDA_PG_DB:-pmda}"
PMDA_PG_USER="${PMDA_PG_USER:-pmda}"
PMDA_PG_PASSWORD="${PMDA_PG_PASSWORD:-pmda}"

PMDA_REDIS_HOST="${PMDA_REDIS_HOST:-127.0.0.1}"
PMDA_REDIS_PORT="${PMDA_REDIS_PORT:-6379}"
PMDA_REDIS_DB="${PMDA_REDIS_DB:-0}"
PMDA_REDIS_PASSWORD="${PMDA_REDIS_PASSWORD:-}"
PMDA_PID=""
PMDA_CLEANED_UP=0

cleanup_services() {
  if [ "${PMDA_CLEANED_UP}" = "1" ]; then
    return
  fi
  PMDA_CLEANED_UP=1

  set +e
  if [ -n "${PMDA_REDIS_PASSWORD}" ]; then
    redis-cli -h "${PMDA_REDIS_HOST}" -p "${PMDA_REDIS_PORT}" -a "${PMDA_REDIS_PASSWORD}" shutdown nosave >/dev/null 2>&1 || true
  else
    redis-cli -h "${PMDA_REDIS_HOST}" -p "${PMDA_REDIS_PORT}" shutdown nosave >/dev/null 2>&1 || true
  fi
  su -s /bin/sh postgres -c "pg_ctl -D '${PMDA_PGDATA}' -m fast stop" >/dev/null 2>&1 || true
}

forward_signal() {
  if [ -n "${PMDA_PID}" ] && kill -0 "${PMDA_PID}" >/dev/null 2>&1; then
    kill -TERM "${PMDA_PID}" >/dev/null 2>&1 || true
    wait "${PMDA_PID}" >/dev/null 2>&1 || true
  fi
  cleanup_services
  exit 0
}

trap forward_signal INT TERM
trap cleanup_services EXIT

if [ ! -d "/usr/lib/postgresql/${PMDA_PG_VERSION}/bin" ]; then
  DETECTED_PG_VERSION="$(ls -1 /usr/lib/postgresql 2>/dev/null | sort -V | tail -n 1 || true)"
  if [ -n "${DETECTED_PG_VERSION}" ] && [ -d "/usr/lib/postgresql/${DETECTED_PG_VERSION}/bin" ]; then
    PMDA_PG_VERSION="${DETECTED_PG_VERSION}"
  fi
fi

PG_BIN_DIR="/usr/lib/postgresql/${PMDA_PG_VERSION}/bin"
if [ -d "${PG_BIN_DIR}" ]; then
  export PATH="${PG_BIN_DIR}:${PATH}"
else
  echo "PostgreSQL binaries not found under /usr/lib/postgresql/${PMDA_PG_VERSION}/bin" >&2
  exit 1
fi

mkdir -p "${PMDA_PGDATA}"
chown -R postgres:postgres "${PMDA_PGDATA}"

if [ ! -s "${PMDA_PGDATA}/PG_VERSION" ]; then
  su -s /bin/sh postgres -c "initdb -D '${PMDA_PGDATA}' --encoding=UTF8 --locale=C.UTF-8 --username=postgres --auth-local=trust --auth-host=scram-sha-256"
fi

if ! grep -Eq "^[#[:space:]]*listen_addresses[[:space:]]*=" "${PMDA_PGDATA}/postgresql.conf"; then
  echo "listen_addresses = '${PMDA_PG_HOST}'" >> "${PMDA_PGDATA}/postgresql.conf"
fi
if ! grep -Eq "^[#[:space:]]*port[[:space:]]*=" "${PMDA_PGDATA}/postgresql.conf"; then
  echo "port = ${PMDA_PG_PORT}" >> "${PMDA_PGDATA}/postgresql.conf"
fi
if ! grep -q "127.0.0.1/32" "${PMDA_PGDATA}/pg_hba.conf"; then
  cat >> "${PMDA_PGDATA}/pg_hba.conf" <<'EOF'
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ::1/128                 scram-sha-256
EOF
fi

su -s /bin/sh postgres -c "pg_ctl -D '${PMDA_PGDATA}' -w start"

if ! su -s /bin/sh postgres -c "psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='${PMDA_PG_USER}'\"" | grep -q 1; then
  su -s /bin/sh postgres -c "psql -v ON_ERROR_STOP=1 -c \"CREATE ROLE \\\"${PMDA_PG_USER}\\\" LOGIN PASSWORD '${PMDA_PG_PASSWORD}'\""
else
  su -s /bin/sh postgres -c "psql -v ON_ERROR_STOP=1 -c \"ALTER ROLE \\\"${PMDA_PG_USER}\\\" LOGIN PASSWORD '${PMDA_PG_PASSWORD}'\""
fi

if ! su -s /bin/sh postgres -c "psql -tAc \"SELECT 1 FROM pg_database WHERE datname='${PMDA_PG_DB}'\"" | grep -q 1; then
  su -s /bin/sh postgres -c "createdb -O \"${PMDA_PG_USER}\" \"${PMDA_PG_DB}\""
fi

REDIS_ARGS=(--bind "${PMDA_REDIS_HOST}" --port "${PMDA_REDIS_PORT}" --daemonize yes --save "" --appendonly no)
if [ -n "${PMDA_REDIS_PASSWORD}" ]; then
  REDIS_ARGS+=(--requirepass "${PMDA_REDIS_PASSWORD}")
fi
redis-server "${REDIS_ARGS[@]}"

export PMDA_PGDATA PMDA_PG_HOST PMDA_PG_PORT PMDA_PG_DB PMDA_PG_USER PMDA_PG_PASSWORD
export PMDA_REDIS_HOST PMDA_REDIS_PORT PMDA_REDIS_DB PMDA_REDIS_PASSWORD

python /app/pmda.py &
PMDA_PID="$!"
wait "${PMDA_PID}"
