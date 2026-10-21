#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
CTL=${CTL:-./bin/raftctl}; PROBE=${PROBE:-./bin/raftprobe}
ADDRESSES=${ADDRESSES:-127.0.0.1:${GRPC1:-50051},127.0.0.1:${GRPC2:-50052},127.0.0.1:${GRPC3:-50053},127.0.0.1:${GRPC4:-50054},127.0.0.1:${GRPC5:-50055}}
ctl() { "$CTL" -addresses "$ADDRESSES" "$@"; }
project=${COMPOSE_PROJECT_NAME:-$(docker compose config --format json | python -c 'import json,sys;print(json.load(sys.stdin)["name"])')}
probe() { "$PROBE" -addresses "$ADDRESSES" -project "$project" "$@"; }
trap 'bash scripts/chaos/network.sh heal; docker compose start raftdb-node1 raftdb-node2 raftdb-node3 raftdb-node4 raftdb-node5' EXIT
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up ${BUILD_FLAG:---build} -d --wait
probe -mode ready
ctl -timeout 180s seed 10000
probe -mode validate
# A: repeated SIGKILL leader failures, committed-data preservation, restart.
mkdir -p benchmarks/results
probe -mode recovery -trials "${TRIALS:-5}" > "${RECOVERY_REPORT:-benchmarks/results/fault-suite-recovery.jsonl}"
probe -mode ready
# B: a single follower restarts and catches up after an offline write.
bash scripts/chaos/follower.sh
# C: two stopped voters leave a functioning majority.
docker compose stop raftdb-node4 raftdb-node5
sleep 3
ctl put chaos-majority safe
test "$(ctl get chaos-majority)" = safe
# D: only two remain. Every live voter must reject consensus operations.
docker compose stop raftdb-node3
sleep 3
"$PROBE" -addresses "127.0.0.1:${GRPC1:-50051},127.0.0.1:${GRPC2:-50052}" -mode refuse

docker compose start raftdb-node3 raftdb-node4 raftdb-node5
probe -mode ready
# E: true packet-filter partition, with a fixed three-voter majority.
bash scripts/chaos/network.sh partition
sleep 3
bash scripts/chaos/network.sh check
"$CTL" -addresses 127.0.0.1:${GRPC1:-50051},127.0.0.1:${GRPC2:-50052},127.0.0.1:${GRPC3:-50053} put partition majority
"$PROBE" -addresses "127.0.0.1:${GRPC4:-50054},127.0.0.1:${GRPC5:-50055}" -mode refuse

bash scripts/chaos/network.sh heal
probe -mode ready
test "$(ctl get partition)" = majority
# F: deterministic divergence is checked using real Raft in the Go partition test.
go test -count=1 -timeout 60s ./internal/cluster -run TestPartitionDivergentUncommittedLog
# G: compact, restart and validate the complete original seed dataset.
ctl snapshot
docker compose restart raftdb-node1 raftdb-node2 raftdb-node3 raftdb-node4 raftdb-node5
probe -mode ready
probe -mode validate
echo 'Docker scenarios A..G passed'
# Delay/loss requires a Linux kernel with sch_netem; skip only explicitly.
if [[ ${SKIP_NETEM:-0} == 1 ]]; then
  echo 'SKIPPED netem delay/loss (explicit SKIP_NETEM=1); this is not a passed test'
else
  bash scripts/chaos/network.sh delay
  ctl -timeout 10s put netem tested
  bash scripts/chaos/network.sh heal
  test "$(ctl get netem)" = tested
  echo 'Netem delay/loss passed'
fi
