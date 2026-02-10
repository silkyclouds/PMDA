#!/usr/bin/env bash
set -euo pipefail

PMDA_PG_VERSION="${PMDA_PG_VERSION:-15}"
PMDA_PGDATA="${PMDA_PGDATA:-/config/postgres-data}"
PMDA_PG_HOST="${PMDA_PG_HOST:-127.0.0.1}"
PMDA_PG_PORT="${PMDA_PG_PORT:-5432}"
PMDA_PG_DB="${PMDA_PG_DB:-pmda}"
PMDA_PG_USER="${PMDA_PG_USER:-pmda}"
PMDA_PG_PASSWORD="${PMDA_PG_PASSWORD:-pmda}"
PMDA_PERF_PROFILE="${PMDA_PERF_PROFILE:-aggressive}"

PMDA_REDIS_HOST="${PMDA_REDIS_HOST:-127.0.0.1}"
PMDA_REDIS_PORT="${PMDA_REDIS_PORT:-6379}"
PMDA_REDIS_DB="${PMDA_REDIS_DB:-0}"
PMDA_REDIS_PASSWORD="${PMDA_REDIS_PASSWORD:-}"
PMDA_PID=""
PMDA_CLEANED_UP=0

to_int() {
  local v="${1:-0}"
  if [ -z "${v}" ]; then
    echo 0
    return
  fi
  echo "${v}" | awk '{printf("%d\n", $1)}'
}

clamp_int() {
  local value min max
  value="$(to_int "${1:-0}")"
  min="$(to_int "${2:-0}")"
  max="$(to_int "${3:-0}")"
  if [ "${value}" -lt "${min}" ]; then
    echo "${min}"
  elif [ "${value}" -gt "${max}" ]; then
    echo "${max}"
  else
    echo "${value}"
  fi
}

detect_memory_mb() {
  local raw=""
  if [ -r /sys/fs/cgroup/memory.max ]; then
    raw="$(cat /sys/fs/cgroup/memory.max 2>/dev/null || true)"
    if [ -n "${raw}" ] && [ "${raw}" != "max" ]; then
      echo $(( raw / 1024 / 1024 ))
      return
    fi
  fi
  if [ -r /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
    raw="$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || true)"
    if [ -n "${raw}" ] && [ "${raw}" -lt 9223372036854771712 ]; then
      echo $(( raw / 1024 / 1024 ))
      return
    fi
  fi
  raw="$(awk '/MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || true)"
  if [ -n "${raw}" ]; then
    echo $(( raw / 1024 ))
    return
  fi
  echo 2048
}

detect_cpu_count() {
  local n
  n="$(nproc 2>/dev/null || echo 2)"
  n="$(to_int "${n}")"
  if [ "${n}" -lt 1 ]; then
    echo 1
  else
    echo "${n}"
  fi
}

set_pg_conf() {
  local key="${1}"
  local value="${2}"
  local conf_file="${PMDA_PGDATA}/postgresql.conf"
  if grep -Eq "^[#[:space:]]*${key}[[:space:]]*=" "${conf_file}"; then
    sed -i -E "s|^[#[:space:]]*${key}[[:space:]]*=.*|${key} = ${value}|g" "${conf_file}"
  else
    echo "${key} = ${value}" >> "${conf_file}"
  fi
}

apply_pg_tuning() {
  local total_mem_mb cpu_count
  total_mem_mb="$(detect_memory_mb)"
  cpu_count="$(detect_cpu_count)"
  total_mem_mb="$(clamp_int "${total_mem_mb}" 1024 1048576)"

  local shared_buffers_mb effective_cache_mb work_mem_mb maintenance_mb wal_buffers_mb max_wal_mb min_wal_mb
  shared_buffers_mb="$(clamp_int $(( total_mem_mb / 4 )) 256 8192)"
  effective_cache_mb="$(clamp_int $(( total_mem_mb * 60 / 100 )) 512 65536)"
  work_mem_mb="$(clamp_int $(( total_mem_mb / 96 )) 8 64)"
  maintenance_mb="$(clamp_int $(( total_mem_mb / 10 )) 128 2048)"
  wal_buffers_mb="$(clamp_int $(( shared_buffers_mb / 16 )) 16 64)"
  max_wal_mb="$(clamp_int $(( total_mem_mb / 2 )) 2048 16384)"
  min_wal_mb="$(clamp_int $(( max_wal_mb / 8 )) 512 4096)"

  set_pg_conf "listen_addresses" "'${PMDA_PG_HOST}'"
  set_pg_conf "port" "${PMDA_PG_PORT}"
  set_pg_conf "max_connections" "120"
  set_pg_conf "shared_buffers" "${shared_buffers_mb}MB"
  set_pg_conf "effective_cache_size" "${effective_cache_mb}MB"
  set_pg_conf "work_mem" "${work_mem_mb}MB"
  set_pg_conf "maintenance_work_mem" "${maintenance_mb}MB"
  set_pg_conf "wal_buffers" "${wal_buffers_mb}MB"
  set_pg_conf "min_wal_size" "${min_wal_mb}MB"
  set_pg_conf "max_wal_size" "${max_wal_mb}MB"
  set_pg_conf "checkpoint_timeout" "15min"
  set_pg_conf "checkpoint_completion_target" "0.9"
  set_pg_conf "effective_io_concurrency" "256"
  set_pg_conf "random_page_cost" "1.1"
  set_pg_conf "default_statistics_target" "200"
  set_pg_conf "vacuum_cost_limit" "2000"
  set_pg_conf "jit" "off"
  set_pg_conf "wal_compression" "on"
  set_pg_conf "track_io_timing" "on"
  set_pg_conf "huge_pages" "off"
  set_pg_conf "max_worker_processes" "$(clamp_int "${cpu_count}" 2 16)"
  set_pg_conf "max_parallel_workers" "$(clamp_int "${cpu_count}" 2 16)"
  set_pg_conf "max_parallel_workers_per_gather" "$(clamp_int $(( cpu_count / 2 )) 2 8)"
  set_pg_conf "max_parallel_maintenance_workers" "$(clamp_int $(( cpu_count / 2 )) 2 8)"

  if [ "${PMDA_PERF_PROFILE}" = "aggressive" ]; then
    set_pg_conf "synchronous_commit" "off"
  else
    set_pg_conf "synchronous_commit" "on"
  fi

  echo "[PMDA] PostgreSQL perf profile=${PMDA_PERF_PROFILE} mem=${total_mem_mb}MB cpu=${cpu_count} shared_buffers=${shared_buffers_mb}MB work_mem=${work_mem_mb}MB"
}

build_redis_args() {
  local total_mem_mb cpu_count redis_maxmemory_mb io_threads
  total_mem_mb="$(detect_memory_mb)"
  cpu_count="$(detect_cpu_count)"
  total_mem_mb="$(clamp_int "${total_mem_mb}" 1024 1048576)"
  redis_maxmemory_mb="$(clamp_int $(( total_mem_mb * 15 / 100 )) 128 8192)"
  io_threads="$(clamp_int $(( cpu_count - 1 )) 1 8)"

  REDIS_ARGS=(
    --bind "${PMDA_REDIS_HOST}"
    --port "${PMDA_REDIS_PORT}"
    --daemonize yes
    --save ""
    --appendonly no
    --maxmemory "${redis_maxmemory_mb}mb"
    --maxmemory-policy allkeys-lru
    --lazyfree-lazy-eviction yes
    --lazyfree-lazy-expire yes
    --lazyfree-lazy-server-del yes
    --hz 50
  )
  if [ -n "${PMDA_REDIS_PASSWORD}" ]; then
    REDIS_ARGS+=(--requirepass "${PMDA_REDIS_PASSWORD}")
  fi
  if [ "${cpu_count}" -gt 2 ]; then
    REDIS_ARGS+=(--io-threads "${io_threads}" --io-threads-do-reads yes)
  fi
  echo "[PMDA] Redis tuned maxmemory=${redis_maxmemory_mb}MB policy=allkeys-lru io_threads=${io_threads}"
}

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

apply_pg_tuning

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

build_redis_args
redis-server "${REDIS_ARGS[@]}"

export PMDA_PGDATA PMDA_PG_HOST PMDA_PG_PORT PMDA_PG_DB PMDA_PG_USER PMDA_PG_PASSWORD
export PMDA_REDIS_HOST PMDA_REDIS_PORT PMDA_REDIS_DB PMDA_REDIS_PASSWORD

python /app/pmda.py &
PMDA_PID="$!"
wait "${PMDA_PID}"
