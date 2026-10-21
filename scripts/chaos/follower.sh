#!/usr/bin/env bash
# Stop exactly one follower, commit while it is down, then check its local catch-up.
set -euo pipefail
cd "$(dirname "$0")/../.."
CTL=${CTL:-./bin/raftctl}; PROBE=${PROBE:-./bin/raftprobe}
ADDRESSES=${ADDRESSES:-127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053,127.0.0.1:50054,127.0.0.1:50055}
"$PROBE" -addresses "$ADDRESSES" -mode ready
leader=$("$CTL" -addresses "$ADDRESSES" status | python -c 'import json,sys;print(next(s["id"] for s in map(json.loads,sys.stdin) if s["state"]=="Leader"))')
follower=raftdb-node5; port=${GRPC5:-50055}
if [[ $leader == node5 ]]; then follower=raftdb-node4; port=${GRPC4:-50054}; fi
trap 'docker compose start "$follower"' EXIT
expected="committed-$(date -u +%s)-$$"
docker compose stop "$follower"
"$CTL" -addresses "$ADDRESSES" put follower-crash-check "$expected"
docker compose start "$follower"
"$PROBE" -addresses "$ADDRESSES" -mode ready
observed=''
for attempt in $(seq 1 30); do
  observed=$("$CTL" -addresses "127.0.0.1:$port" local follower-crash-check 2>/dev/null) || true
  if [[ $observed == "$expected" ]]; then break; fi
  sleep 0.1
done
[[ $observed == "$expected" ]] || { echo 'Follower failed to catch up' >&2; exit 1; }
echo 'Single-follower crash, write availability, and local catch-up passed'
