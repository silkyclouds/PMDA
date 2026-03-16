#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "[gate] validating classical benchmark corpus..."
python3 scripts/validate_classical_benchmark.py

echo "[build] classical benchmark passed, building Docker images..."
docker buildx build \
  --platform linux/amd64 \
  -t meaning/pmda:beta \
  -t meaning/pmda:latest \
  --push \
  .

echo "[done] build completed after classical benchmark gate."
