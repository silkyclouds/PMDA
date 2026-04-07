#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Provision a full MusicBrainz mirror on Unraid using the official musicbrainz-docker project.

Usage:
  provision_musicbrainz_mirror_unraid.sh [options]

Options:
  --install-root PATH       Where the official musicbrainz-docker repo will live.
                            Default: /mnt/user/appdata/musicbrainz-docker
  --data-root PATH          Persistent bind-mounted data root.
                            Default: /mnt/user/appdata/musicbrainz-mirror
  --hot-root PATH           Override hot data root (pgdata/solrdata/mqdata).
                            Defaults to --data-root.
  --dump-root PATH          Override cold archive root (dbdump/solrdump).
                            Defaults to --data-root.
  --host NAME               Public/base host used by MusicBrainz itself.
                            Default: localhost
  --port PORT               Published web port on Unraid.
                            Default: 5000
  --server-processes N      MusicBrainz web workers.
                            Default: 10
  --db-shared-buffers SIZE  PostgreSQL shared_buffers value.
                            Default: 2048MB
  --solr-heap SIZE          Solr heap.
                            Default: 2g
  --replication-cron SPEC   Cron line used inside the container.
                            Default: 0 3 * * * /usr/local/bin/replication.sh
  --token-file PATH         Path to a file containing the MetaBrainz replication token.
  --skip-build              Skip docker compose build.
  --skip-createdb           Skip initial dump import.
  --skip-materialized       Skip building MusicBrainz materialized tables.
  --skip-search-bootstrap   Skip initial search index bootstrap.
  --skip-replication        Do not configure replication, even if a token file exists.
  --local-reindex           Build search indexes locally instead of loading prebuilt archives.
  --tag TAG                 Force a specific musicbrainz-docker release tag.
  --help                    Show this help.

Environment overrides:
  MUSICBRAINZ_REPLICATION_TOKEN   Replication token content (alternative to --token-file)
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

container_path_to_host() {
  local container_path="$1"
  local self_name="${HOSTNAME:-}"
  if [[ -z "$self_name" || -z "$container_path" ]]; then
    printf '%s\n' "$container_path"
    return 0
  fi
  local mounts
  mounts="$(docker inspect "$self_name" --format '{{range .Mounts}}{{println .Destination "|" .Source}}{{end}}' 2>/dev/null || true)"
  if [[ -z "$mounts" ]]; then
    printf '%s\n' "$container_path"
    return 0
  fi
  while IFS='|' read -r raw_dest raw_src; do
    local dest src suffix
    dest="$(trim "$raw_dest")"
    src="$(trim "$raw_src")"
    if [[ -z "$dest" || -z "$src" ]]; then
      continue
    fi
    if [[ "$container_path" == "$dest" ]]; then
      printf '%s\n' "$src"
      return 0
    fi
    if [[ "$container_path" == "$dest/"* ]]; then
      suffix="${container_path#"$dest"}"
      printf '%s%s\n' "$src" "$suffix"
      return 0
    fi
  done <<< "$mounts"
  printf '%s\n' "$container_path"
}

latest_release_tag() {
  curl -fsSL https://api.github.com/repos/metabrainz/musicbrainz-docker/releases/latest 2>/dev/null \
    | tr -d '\n' \
    | sed -nE 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p'
}

cached_release_tag() {
  local repo_root="$1"
  if [[ -d "$repo_root/.git" ]]; then
    git -C "$repo_root" tag --list 'v*' --sort=-v:refname | head -n1
  fi
}

write_file_if_changed() {
  local target="$1"
  local tmp
  tmp="$(mktemp)"
  cat >"$tmp"
  if [[ -f "$target" ]] && cmp -s "$tmp" "$target"; then
    rm -f "$tmp"
    return 0
  fi
  mkdir -p "$(dirname "$target")"
  mv "$tmp" "$target"
}

INSTALL_ROOT="/mnt/user/appdata/musicbrainz-docker"
DATA_ROOT="/mnt/user/appdata/musicbrainz-mirror"
HOT_ROOT=""
DUMP_ROOT=""
WEB_HOST="localhost"
WEB_PORT="5000"
SERVER_PROCESSES="10"
DB_SHARED_BUFFERS="2048MB"
SOLR_HEAP="2g"
REPLICATION_CRON="0 3 * * * /usr/local/bin/replication.sh"
TOKEN_FILE=""
SKIP_BUILD=0
SKIP_CREATEDB=0
SKIP_MATERIALIZED=0
SKIP_SEARCH_BOOTSTRAP=0
SKIP_REPLICATION=0
USE_LOCAL_REINDEX=0
FORCED_TAG=""
DEFAULT_TAG="v-2026-02-12.0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-root) INSTALL_ROOT="$2"; shift 2 ;;
    --data-root) DATA_ROOT="$2"; shift 2 ;;
    --hot-root) HOT_ROOT="$2"; shift 2 ;;
    --dump-root) DUMP_ROOT="$2"; shift 2 ;;
    --host) WEB_HOST="$2"; shift 2 ;;
    --port) WEB_PORT="$2"; shift 2 ;;
    --server-processes) SERVER_PROCESSES="$2"; shift 2 ;;
    --db-shared-buffers) DB_SHARED_BUFFERS="$2"; shift 2 ;;
    --solr-heap) SOLR_HEAP="$2"; shift 2 ;;
    --replication-cron) REPLICATION_CRON="$2"; shift 2 ;;
    --token-file) TOKEN_FILE="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-createdb) SKIP_CREATEDB=1; shift ;;
    --skip-materialized) SKIP_MATERIALIZED=1; shift ;;
    --skip-search-bootstrap) SKIP_SEARCH_BOOTSTRAP=1; shift ;;
    --skip-replication) SKIP_REPLICATION=1; shift ;;
    --local-reindex) USE_LOCAL_REINDEX=1; shift ;;
    --tag) FORCED_TAG="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_cmd curl
require_cmd git
require_cmd docker

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose v2 is required on the Unraid host." >&2
  exit 1
fi

TAG="${FORCED_TAG:-}"
if [[ -z "$TAG" ]]; then
  TAG="$(latest_release_tag || true)"
fi
if [[ -z "$TAG" ]]; then
  TAG="$(cached_release_tag "$INSTALL_ROOT" || true)"
fi
if [[ -z "$TAG" ]]; then
  TAG="$DEFAULT_TAG"
fi
if [[ -z "$TAG" ]]; then
  echo "Unable to resolve latest musicbrainz-docker release tag." >&2
  exit 1
fi

if [[ -z "$HOT_ROOT" ]]; then
  HOT_ROOT="$DATA_ROOT"
fi
if [[ -z "$DUMP_ROOT" ]]; then
  DUMP_ROOT="$DATA_ROOT"
fi

HOST_DATA_ROOT="$(container_path_to_host "$DATA_ROOT")"
HOST_HOT_ROOT="$(container_path_to_host "$HOT_ROOT")"
HOST_DUMP_ROOT="$(container_path_to_host "$DUMP_ROOT")"

mkdir -p "$INSTALL_ROOT" \
  "$HOT_ROOT"/{mqdata,pgdata,solrdata} \
  "$DUMP_ROOT"/{dbdump,solrdump}

# Acknowledge the official MetaBrainz download terms up front so first-run
# dump/index bootstrap works non-interactively inside Docker on Unraid.
touch \
  "$DUMP_ROOT"/dbdump/.for-non-commercial-use \
  "$DUMP_ROOT"/solrdump/.for-non-commercial-use

if [[ ! -d "$INSTALL_ROOT/.git" ]]; then
  rm -rf "$INSTALL_ROOT"
  git clone https://github.com/metabrainz/musicbrainz-docker.git "$INSTALL_ROOT"
fi

cd "$INSTALL_ROOT"
git fetch --tags origin >/dev/null 2>&1 || echo "==> Could not refresh remote tags; using cached checkout data"
git checkout -f "$TAG"

write_file_if_changed ".env" <<EOF
MUSICBRAINZ_WEB_SERVER_HOST=${WEB_HOST}
MUSICBRAINZ_WEB_SERVER_PORT=${WEB_PORT}
MUSICBRAINZ_SERVER_PROCESSES=${SERVER_PROCESSES}
MUSICBRAINZ_CRONTAB_PATH=./local/replication.cron
MB_UNRAID_DATA_ROOT=${HOST_DATA_ROOT}
MB_UNRAID_HOT_ROOT=${HOST_HOT_ROOT}
MB_UNRAID_DUMP_ROOT=${HOST_DUMP_ROOT}
MB_POSTGRES_SHARED_BUFFERS=${DB_SHARED_BUFFERS}
MB_SOLR_HEAP=${SOLR_HEAP}
COMPOSE_FILE=docker-compose.yml:local/compose/unraid-bindings.yml:local/compose/memory-settings.yml
EOF

write_file_if_changed "local/compose/unraid-bindings.yml" <<'EOF'
# Description: Persist official MusicBrainz Docker named volumes to Unraid appdata.

volumes:
  mqdata:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: ${MB_UNRAID_HOT_ROOT}/mqdata
  pgdata:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: ${MB_UNRAID_HOT_ROOT}/pgdata
  solrdata:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: ${MB_UNRAID_HOT_ROOT}/solrdata
  dbdump:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: ${MB_UNRAID_DUMP_ROOT}/dbdump
  solrdump:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: ${MB_UNRAID_DUMP_ROOT}/solrdump
EOF

write_file_if_changed "local/compose/memory-settings.yml" <<'EOF'
# Description: Memory tuning overlay for a full mirror with search.

services:
  db:
    command: postgres -c "shared_buffers=${MB_POSTGRES_SHARED_BUFFERS:-2048MB}" -c "shared_preload_libraries=pg_amqp.so"
  search:
    environment:
      - SOLR_HEAP=${MB_SOLR_HEAP:-2g}
EOF

write_file_if_changed "local/replication.cron" <<EOF
SHELL=/bin/bash
BASH_ENV=/noninteractive.bash_env
${REPLICATION_CRON}
EOF

mkdir -p local/secrets
if [[ -n "${MUSICBRAINZ_REPLICATION_TOKEN:-}" ]]; then
  printf '%s\n' "${MUSICBRAINZ_REPLICATION_TOKEN}" > local/secrets/metabrainz_access_token
elif [[ -n "$TOKEN_FILE" ]]; then
  cp "$TOKEN_FILE" local/secrets/metabrainz_access_token
fi

admin/configure add local/compose/unraid-bindings.yml >/dev/null
admin/configure add local/compose/memory-settings.yml >/dev/null

if [[ -f local/secrets/metabrainz_access_token ]] && [[ "$SKIP_REPLICATION" -eq 0 ]]; then
  admin/configure add replication-token >/dev/null
  admin/configure add replication-cron >/dev/null
fi

echo "==> 5% Using musicbrainz-docker release: $TAG"
echo "==> Install root: $INSTALL_ROOT"
echo "==> Data root:    $DATA_ROOT"
echo "==> Hot root:     $HOT_ROOT"
echo "==> Dump root:    $DUMP_ROOT"
echo "==> Host data:    $HOST_DATA_ROOT"
echo "==> Host hot:     $HOST_HOT_ROOT"
echo "==> Host dump:    $HOST_DUMP_ROOT"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  echo "==> 15% Building docker images"
  docker compose build
fi

if [[ "$SKIP_CREATEDB" -eq 0 ]]; then
  echo "==> 40% Importing latest full MusicBrainz dumps (this is long and disk-intensive)"
  docker compose run --rm -T musicbrainz createdb.sh -fetch
fi

if [[ "$SKIP_CREATEDB" -eq 0 ]] && [[ "$SKIP_MATERIALIZED" -eq 0 ]]; then
  echo "==> 72% Building materialized tables for faster local queries"
  docker compose run --rm musicbrainz bash -lc 'carton exec -- ./admin/BuildMaterializedTables --database=MAINTENANCE all'
fi

echo "==> 82% Starting search/bootstrap services"
docker compose up -d musicbrainz search

if [[ "$SKIP_SEARCH_BOOTSTRAP" -eq 0 ]]; then
  if [[ "$USE_LOCAL_REINDEX" -eq 1 ]]; then
    echo "==> 90% Building search indexes locally"
    docker compose up -d indexer
    docker compose exec indexer python -m sir reindex
  else
    echo "==> 90% Loading prebuilt search indexes from the latest dump"
    docker compose exec -T search fetch-backup-archives
    docker compose exec search load-backup-archives
    docker compose exec search remove-backup-archives || true
  fi
fi

echo "==> 97% Starting full stack"
docker compose up -d

if [[ -f local/secrets/metabrainz_access_token ]] && [[ "$SKIP_REPLICATION" -eq 0 ]]; then
  echo "==> Running replication once to catch up with current data"
  bash -lc 'docker compose exec musicbrainz replication.sh >/dev/null 2>&1 &' || true
  echo "Replication cron is enabled inside the musicbrainz container."
else
  echo "Replication token not configured. Mirror will stay at dump snapshot state until you add local/secrets/metabrainz_access_token and enable replication."
fi

cat <<EOF

100% Provisioning complete.

Official release used: ${TAG}
MusicBrainz base URL:   http://${WEB_HOST}:${WEB_PORT}
Install root:           ${INSTALL_ROOT}
Data root:              ${DATA_ROOT}
Hot root:               ${HOT_ROOT}
Dump root:              ${DUMP_ROOT}

Recommended next checks:
  cd ${INSTALL_ROOT}
  docker compose ps
  docker compose exec musicbrainz tail --follow mirror.log

For PMDA:
  MUSICBRAINZ_MIRROR_ENABLED=true
  MUSICBRAINZ_BASE_URL=http://${WEB_HOST}:${WEB_PORT}
  MUSICBRAINZ_MIRROR_NAME=Unraid MB
EOF
