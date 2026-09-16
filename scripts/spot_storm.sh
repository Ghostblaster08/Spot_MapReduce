#!/usr/bin/env bash
# spot_storm.sh — Simulates correlated spot storm by stopping 40% of running workers

set -euo pipefail

KILL_PERCENT="${1:-40}"
GRACE="${2:-10}"

WORKERS=$(docker ps --filter "name=worker" --format "{{.Names}}")
TOTAL=$(echo "$WORKERS" | grep -v '^$' | wc -l)

if [ "$TOTAL" -eq 0 ]; then
    echo "[ERROR] No worker containers running!"
    exit 1
fi

COUNT=$(( (TOTAL * KILL_PERCENT + 99) / 100 ))
echo "==> [SPOT STORM] Total workers: ${TOTAL}. Evicting ${COUNT} workers (${KILL_PERCENT}%)..."

echo "$WORKERS" | shuf | head -n "${COUNT}" | while read -r W; do
    echo "    Stopping worker ${W} (grace: ${GRACE}s)..."
    docker stop --time "${GRACE}" "${W}" &
done

wait
echo "==> [SPOT STORM] Eviction storm finished. Orchestrator should requeue tasks."
