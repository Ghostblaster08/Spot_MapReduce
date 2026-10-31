#!/usr/bin/env bash
# verify_output.sh — Verifies MapReduce correctness against single-node ground truth baseline

set -euo pipefail

JOB_ID="${1:-}"
INPUT_FILE="${2:-job-client/jobs/large_input.txt}"
if [ ! -f "$INPUT_FILE" ] && [ -f "job-client/jobs/sample_input.txt" ]; then
    INPUT_FILE="job-client/jobs/sample_input.txt"
fi
OUTPUT_DIR="${3:-data/output}"

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

# If output dir doesn't exist locally or is empty, try extracting from orchestrator container
if [ -z "$JOB_ID" ]; then
    if docker compose ps orchestrator >/dev/null 2>&1; then
        LATEST_DOCKER_JOB=$(docker compose exec orchestrator ls -t /data/output 2>/dev/null | grep -v '^_SUCCESS' | head -n 1 | tr -d '\r')
        if [ -n "$LATEST_DOCKER_JOB" ]; then
            JOB_ID="$LATEST_DOCKER_JOB"
            mkdir -p "${TMP_DIR}/${JOB_ID}"
            docker compose exec orchestrator tar -cf - -C "/data/output/${JOB_ID}" . | tar -xf - -C "${TMP_DIR}/${JOB_ID}"
            JOB_PATH="${TMP_DIR}/${JOB_ID}"
        fi
    fi
    if [ -z "$JOB_ID" ] && [ -d "$OUTPUT_DIR" ]; then
        JOB_ID=$(ls -t "$OUTPUT_DIR" | grep -v '^_SUCCESS' | head -n 1)
        JOB_PATH="${OUTPUT_DIR}/${JOB_ID}"
    fi
else
    if [ -d "${OUTPUT_DIR}/${JOB_ID}" ]; then
        JOB_PATH="${OUTPUT_DIR}/${JOB_ID}"
    elif docker compose ps orchestrator >/dev/null 2>&1; then
        mkdir -p "${TMP_DIR}/${JOB_ID}"
        docker compose exec orchestrator tar -cf - -C "/data/output/${JOB_ID}" . | tar -xf - -C "${TMP_DIR}/${JOB_ID}"
        JOB_PATH="${TMP_DIR}/${JOB_ID}"
    else
        JOB_PATH="${OUTPUT_DIR}/${JOB_ID}"
    fi
fi

if [ -z "${JOB_ID:-}" ] || [ ! -d "${JOB_PATH:-}" ]; then
    echo "[ERROR] No completed job found!"
    echo "Usage: ./scripts/verify_output.sh <JOB_ID> [INPUT_FILE] [OUTPUT_DIR]"
    exit 1
fi

echo "==> Verifying output for Job: ${JOB_ID} against ${INPUT_FILE}..."

# Compute ground truth
python3 -c "
import sys, re
from collections import Counter, defaultdict

with open('${INPUT_FILE}', 'r', encoding='utf-8') as f:
    sample = [f.readline() for _ in range(5)]
    is_csv = any(',' in l and len(l.split(',')) >= 4 for l in sample if l.strip())

if is_csv:
    vol_by_sym = defaultdict(int)
    with open('${INPUT_FILE}', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.lower().startswith('timestamp') or line.lower().startswith('date'):
                continue
            parts = line.split(',')
            if len(parts) >= 4:
                try:
                    sym = parts[1].strip().upper()
                    vol = int(float(parts[3].strip()))
                    if sym and vol > 0:
                        vol_by_sym[sym] += vol
                except:
                    pass
    for sym in sorted(vol_by_sym.keys()):
        print(f'{sym}\t{vol_by_sym[sym]}')
else:
    with open('${INPUT_FILE}', 'r', encoding='utf-8') as f:
        text = f.read().lower()
    words = re.findall(r'[a-zA-Z0-9]+', text)
    counts = Counter(words)
    for k, v in sorted(counts.items()):
        print(f'{k}\t{v}')
" > "${TMP_DIR}/expected.tsv"

# Combine actual output partitions and normalize
python3 -c "
import glob, re

for fname in sorted(glob.glob('${JOB_PATH}/part-*.txt')):
    with open(fname, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            parts = line.split('\t')
            if len(parts) >= 2:
                key = parts[0].strip()
                rest = parts[1]
                m = re.search(r'VOL:\s*([\d,]+)', rest)
                if m:
                    vol = int(m.group(1).replace(',', ''))
                    print(f'{key}\t{vol}')
                else:
                    print(f'{key}\t{parts[1].strip()}')
" | sort > "${TMP_DIR}/actual.tsv"

if [ ! -s "${TMP_DIR}/actual.tsv" ]; then
    echo "[FAIL] Output partition files in ${JOB_PATH} are empty or missing!"
    exit 1
fi

if diff -u "${TMP_DIR}/expected.tsv" "${TMP_DIR}/actual.tsv" > "${TMP_DIR}/diff.txt"; then
    TOTAL_ITEMS=$(wc -l < "${TMP_DIR}/actual.tsv")
    echo "==> [PASS] Output matches ground truth baseline with 0 discrepancies! (${TOTAL_ITEMS} keys verified)"
    exit 0
else
    echo "==> [FAIL] Differences detected between expected and actual output:"
    cat "${TMP_DIR}/diff.txt" | head -n 30
    exit 1
fi
