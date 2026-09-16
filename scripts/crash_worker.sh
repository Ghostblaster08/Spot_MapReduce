#!/usr/bin/env bash
# crash_worker.sh — Simulates sudden ungraceful worker crash (SIGKILL, no drain)

set -euo pipefail

WORKER_NAME="${1:-}"

if [ -z "$WORKER_NAME" ]; then
    WORKER_NAME=$(docker ps --filter "name=worker" --format "{{.Names}}" | head -n 1)
fi

if [ -z "$WORKER_NAME" ]; then
    echo "[ERROR] No worker container found to kill!"
    exit 1
fi

echo "==> [CRASH] Sending SIGKILL to worker ${WORKER_NAME} (no warning, no drain)..."
docker kill "${WORKER_NAME}"
echo "==> [CRASH] Worker killed instantly. Orchestrator lease timeout should trigger in ~5s."
