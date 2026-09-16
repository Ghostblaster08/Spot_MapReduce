#!/usr/bin/env bash
# verify_output.sh — Verifies MapReduce WordCount correctness against single-node baseline

set -euo pipefail

JOB_ID="${1:-}"
INPUT_FILE="${2:-job-client/jobs/sample_input.txt}"
OUTPUT_DIR="${3:-data/output}"

if [ -z "$JOB_ID" ]; then
    # Find most recent job output directory
    if [ -d "$OUTPUT_DIR" ]; then
        JOB_ID=$(ls -t "$OUTPUT_DIR" | grep -v '^_SUCCESS' | head -n 1)
    fi
fi

if [ -z "$JOB_ID" ]; then
    echo "[ERROR] No completed job found in ${OUTPUT_DIR}!"
    echo "Usage: ./scripts/verify_output.sh <JOB_ID> [INPUT_FILE] [OUTPUT_DIR]"
    exit 1
fi

JOB_PATH="${OUTPUT_DIR}/${JOB_ID}"
echo "==> Verifying output for Job: ${JOB_ID} in ${JOB_PATH} against ${INPUT_FILE}..."

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

# Compute ground truth
python3 -c "
import re
from collections import Counter

with open('${INPUT_FILE}', 'r', encoding='utf-8') as f:
    text = f.read().lower()
words = re.findall(r'[a-zA-Z0-9]+', text)
counts = Counter(words)
for k, v in sorted(counts.items()):
    print(f'{k}\t{v}')
" > "${TMP_DIR}/expected.tsv"

# Combine actual output partitions
cat ${JOB_PATH}/part-*.txt 2>/dev/null | sort > "${TMP_DIR}/actual.tsv"

if [ ! -s "${TMP_DIR}/actual.tsv" ]; then
    echo "[FAIL] Output partition files in ${JOB_PATH} are empty or missing!"
    exit 1
fi

if diff -u "${TMP_DIR}/expected.tsv" "${TMP_DIR}/actual.tsv" > "${TMP_DIR}/diff.txt"; then
    TOTAL_WORDS=$(wc -l < "${TMP_DIR}/actual.tsv")
    echo "==> [PASS] WordCount output matches ground truth perfectly! (${TOTAL_WORDS} unique words verified)"
    exit 0
else
    echo "==> [FAIL] Differences detected between expected and actual output:"
    cat "${TMP_DIR}/diff.txt" | head -n 30
    exit 1
fi
