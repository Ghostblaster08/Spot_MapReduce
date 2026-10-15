#!/usr/bin/env bash
# Drives a live job through /flow and captures a screenshot at each stage.
OUT=/home/ghostblaster/.gemini/antigravity-cli/brain/1fa9a734-840f-4d5a-8e61-e33651ae6f39
shot() { chromium --headless --no-sandbox --disable-gpu --hide-scrollbars --window-size=1500,950 \
  --virtual-time-budget=3500 --screenshot="$OUT/flow_$1.png" "http://localhost:8000/flow" >/dev/null 2>&1; echo "shot $1"; }

curl -s -X POST localhost:8000/dashboard/reset >/dev/null; sleep 3
shot 1_idle

curl -s -X POST "localhost:8000/dashboard/submit-job?dataset=large&num_mappers=3&num_reducers=3" >/dev/null
sleep 5
shot 2_running

# preempt node 1 (SIGTERM, graceful) in the background; it blocks until the drain finishes
curl -s -X POST "localhost:8000/dashboard/simulate-preemption?grace_seconds=15&auto_spawn=true&target_node=spot_mapreduce-worker-1" >/dev/null &
sleep 2
shot 3_preempt
sleep 3
curl -s -X POST "localhost:8000/dashboard/simulate-crash?auto_spawn=true&target_node=spot_mapreduce-worker-2" >/dev/null &
sleep 2
shot 4_crash
sleep 2
shot 5_recovering
for i in $(seq 1 40); do
  st=$(curl -s localhost:8000/dashboard/state | python3 -c "import sys,json; j=json.load(sys.stdin).get('active_job') or {}; print(j.get('state'))")
  [ "$st" = "COMPLETED" ] && break; sleep 2
done
echo "final state: $st"
shot 6_done
