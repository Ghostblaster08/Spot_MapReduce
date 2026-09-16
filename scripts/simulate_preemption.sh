#!/usr/bin/env bash
# simulate_preemption.sh — Simulates spot instance preemption notice
# Sends SIGTERM to a worker container, giving it grace period to drain state to RSS.

set -euo pipefail

WORKER_NAME="${1:-}"
GRACE="${2:-30}"

if [ -z "$WORKER_NAME" ]; then
    # Pick first running worker container
    WORKER_NAME=$(docker ps --filter "name=spot_mapreduce-worker" --filter "name=worker" --format "{{.Names}}" | head -n 1)
fi

if [ -z "$WORKER_NAME" ]; then
    echo "[ERROR] No running worker container found!"
    exit 1
fi

echo "==> [SENTINEL] Sending SIGTERM to worker: ${WORKER_NAME} (grace period: ${GRACE}s)..."
docker stop --time "${GRACE}" "${WORKER_NAME}"
echo "==> [SENTINEL] Worker ${WORKER_NAME} stopped cleanly. Check orchestrator logs for drain & requeue."
