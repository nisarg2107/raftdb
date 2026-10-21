#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
root=${RECOVERY_ROOT:-benchmarks/results}
mkdir -p "$root"
profile=${1:-default}
files=(-f docker-compose.yml)
if [[ $profile == tuned ]]; then files+=(-f docker-compose.tuned.yml); fi
docker compose "${files[@]}" up ${BUILD_FLAG:---build} -d --wait
docker compose "${files[@]}" config > "$root/recovery-$profile-compose.yml"
for node in 1 2 3 4 5; do
  id=$(docker compose ps -q "raftdb-node$node")
  docker inspect --format '{{json .Config}}' "$id" > "$root/recovery-$profile-node$node.json"
done
${PROBE:-./bin/raftprobe} -addresses "${ADDRESSES:-127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053,127.0.0.1:50054,127.0.0.1:50055}" -project "$(docker compose config --format json | "${PYTHON:-python}" -c 'import json,sys;print(json.load(sys.stdin)["name"])')" -mode recovery -trials "${TRIALS:-10}" > "$root/recovery-$profile.jsonl"
