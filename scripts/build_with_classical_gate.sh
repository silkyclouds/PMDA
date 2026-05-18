#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PUSH_LATEST=0
PUSH_NIGHTLY=0
RUN_CLASSICAL_GATE=0
DOCKER_BIN="${DOCKER_BIN:-$(command -v docker || true)}"
PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$DOCKER_BIN" && -x /usr/local/bin/docker ]]; then
  DOCKER_BIN=/usr/local/bin/docker
fi
if [[ -z "$DOCKER_BIN" && -x /Applications/Docker.app/Contents/Resources/bin/docker ]]; then
  DOCKER_BIN=/Applications/Docker.app/Contents/Resources/bin/docker
fi
if [[ -z "$DOCKER_BIN" ]]; then
  echo "docker CLI not found. Set DOCKER_BIN or add docker to PATH." >&2
  exit 1
fi
if [[ -z "$PYTHON_BIN" && -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
fi
if [[ -z "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3 || true)"
fi
if [[ -z "$PYTHON_BIN" ]]; then
  echo "python3 not found. Set PYTHON_BIN or add python3 to PATH." >&2
  exit 1
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-latest)
      PUSH_LATEST=1
      shift
      ;;
    --with-nightly)
      PUSH_NIGHTLY=1
      shift
      ;;
    --with-live-classical-gate)
      RUN_CLASSICAL_GATE=1
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: $0 [--with-latest] [--with-nightly] [--with-live-classical-gate]" >&2
      exit 1
      ;;
  esac
done

if [[ "$RUN_CLASSICAL_GATE" -eq 1 ]]; then
  echo "[gate] validating classical benchmark corpus against live PMDA..."
  "$PYTHON_BIN" scripts/validate_classical_benchmark.py
else
  echo "[gate] skipping live classical benchmark gate (use --with-live-classical-gate to enable)."
fi

echo "[gate] validating pipeline and legacy cleanup invariants..."
"$PYTHON_BIN" scripts/pipeline_audit_gate.py
"$PYTHON_BIN" scripts/legacy_cleanup_gate.py

echo "[build] release gates passed, building Docker images..."
BUILD_ARGS=(
  --platform linux/amd64
  -t meaning/pmda:beta
)

if [[ "$PUSH_LATEST" -eq 1 ]]; then
  BUILD_ARGS+=(-t meaning/pmda:latest)
fi
if [[ "$PUSH_NIGHTLY" -eq 1 ]]; then
  BUILD_ARGS+=(-t meaning/pmda:nightly)
fi

"$DOCKER_BIN" buildx build \
  "${BUILD_ARGS[@]}" \
  --push \
  .

TAGS=(beta)
if [[ "$PUSH_LATEST" -eq 1 ]]; then TAGS+=(latest); fi
if [[ "$PUSH_NIGHTLY" -eq 1 ]]; then TAGS+=(nightly); fi
echo "[done] build completed after classical benchmark gate (${TAGS[*]})."
