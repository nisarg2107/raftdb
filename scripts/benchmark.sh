#!/usr/bin/env bash
# Thin wrapper around the official ghz CLI; no application load generator.
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python}
CTL=${CTL:-./bin/raftctl}
PROBE=${PROBE:-./bin/raftprobe}
ADDRESSES=${ADDRESSES:-127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053,127.0.0.1:50054,127.0.0.1:50055}
RUN_DIR=${RUN_DIR:-benchmarks/results/$(date -u +%Y%m%dT%H%M%SZ)-${WORKLOAD:?set WORKLOAD}}
mkdir -p "$RUN_DIR"
GHZ_RUNNER=${GHZ_RUNNER:-host}
if [[ $GHZ_RUNNER != docker ]]; then command -v ghz >/dev/null; fi
# Optional official Linux ghz client avoids Windows published-port forwarding.
# It is a one-shot tool container on the current network, not another DB stack.
project=$(docker compose config --format json | "$PYTHON" -c 'import json,sys;print(json.load(sys.stdin)["name"])')
mount_root=$(pwd)
if command -v cygpath >/dev/null; then mount_root=$(cygpath -m "$mount_root"); fi
ghzrun() {
 if [[ $GHZ_RUNNER == docker ]]; then
  MSYS_NO_PATHCONV=1 docker run --rm --network "${project}_cluster" --cpus "${GHZ_CPUS:-2}" --memory "${GHZ_MEMORY:-1536m}" -e "GOMAXPROCS=${GHZ_GOMAXPROCS:-2}" -e "GOMEMLIMIT=${GHZ_GOMEMLIMIT:-1GiB}" --mount "type=bind,source=$mount_root,target=/work" -w /work "${GHZ_IMAGE:-raftdb-ghz:0.120.0}" "$@"
 else
  ghz "$@"
 fi
}

"$PROBE" -addresses "$ADDRESSES" -mode ready
if [[ ${SKIP_SEED:-0} != 1 ]]; then "$CTL" -addresses "$ADDRESSES" -timeout 180s seed 10000; fi
"$PROBE" -addresses "$ADDRESSES" -mode validate
leader=$("$CTL" -addresses "$ADDRESSES" leader)
go version > "$RUN_DIR/environment.txt"
if [[ $GHZ_RUNNER == docker ]]; then docker run --rm "${GHZ_IMAGE:-raftdb-ghz:0.120.0}" --version; else ghz --version; fi >> "$RUN_DIR/environment.txt"
printf 'load_generator=%s\n' "$GHZ_RUNNER" >> "$RUN_DIR/environment.txt"
if [[ $GHZ_RUNNER == docker ]]; then docker image inspect "${GHZ_IMAGE:-raftdb-ghz:0.120.0}" > "$RUN_DIR/ghz-image.json"; fi
if [[ $GHZ_RUNNER != docker ]]; then
  ghz_path=$(command -v ghz)
  if [[ -f $ghz_path.exe ]]; then ghz_path="$ghz_path.exe"; fi
  go version -m "$ghz_path" >> "$RUN_DIR/environment.txt" 2>&1 || true
fi
uname -a >> "$RUN_DIR/environment.txt"
{ command -v lscpu >/dev/null && lscpu; command -v free >/dev/null && free -h; } >> "$RUN_DIR/environment.txt" || true
if command -v powershell.exe >/dev/null; then
  powershell.exe -NoProfile -Command 'Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors | ConvertTo-Json; Get-CimInstance Win32_ComputerSystem | Select-Object TotalPhysicalMemory | ConvertTo-Json' >> "$RUN_DIR/environment.txt"
fi
docker info >> "$RUN_DIR/environment.txt"
docker compose config > "$RUN_DIR/compose.yml"
for node in 1 2 3 4 5; do
  id=$(docker compose ps -q "raftdb-node$node")
  docker inspect --format '{{json .Config}}' "$id" > "$RUN_DIR/node$node-config.json"
  docker inspect --format '{{json .HostConfig}}' "$id" > "$RUN_DIR/node$node-resources.json"
done
"$CTL" -addresses "$ADDRESSES" status > "$RUN_DIR/status-before.jsonl"
git rev-parse HEAD > "$RUN_DIR/revision.txt" 2>/dev/null || true
"$PYTHON" - "$RUN_DIR" <<'PY'
import hashlib, sys
from pathlib import Path
paths = [*Path('api').glob('*'), *Path('cmd').rglob('*.go'), *Path('internal').rglob('*.go'), Path('go.mod'), Path('go.sum')]
with open(Path(sys.argv[1]) / 'source-sha256.txt', 'w') as out:
    for path in sorted(paths):
        if path.is_file():
            out.write(hashlib.sha256(path.read_bytes()).hexdigest() + ' ' + str(path) + '\n')
PY
configs=(read-hot read-uniform)
if [[ $WORKLOAD == write ]]; then configs=(write-32-overwrite write-32-varied write-256-overwrite write-256-varied write-1024-overwrite write-1024-varied); fi
if [[ ${INCLUDE_LOCAL:-0} == 1 && $WORKLOAD == read ]]; then configs+=(local-hot); fi
if [[ -n ${CONFIGS:-} ]]; then read -r -a configs <<< "$CONFIGS"; fi
for config in "${configs[@]}"; do
  for concurrency in ${CONCURRENCIES:-50 100 250 500 1000}; do
    for trial in $(seq "${FIRST_TRIAL:-1}" "$(( ${FIRST_TRIAL:-1} + ${REPEATS:-3} - 1 ))"); do
      "$PROBE" -addresses "$ADDRESSES" -mode ready
      leader=$("$CTL" -addresses "$ADDRESSES" leader)
      stem="$RUN_DIR/$config-c$concurrency-r$trial"
      target="$leader"
      if [[ $GHZ_RUNNER == docker ]]; then
        leader_id=$("$CTL" -addresses "$ADDRESSES" status | "$PYTHON" -c 'import json,sys;print(next(s["id"] for s in map(json.loads,sys.stdin) if s["state"]=="Leader"))')
        [[ $leader_id =~ ^node[1-5]$ ]] || exit 1
        target="raftdb-$leader_id:50051"
      fi
      extra_flags=()
      if [[ ${GHZ_STATIC_DATA:-0} == 1 ]]; then
        case "$config" in read-hot|write-*-overwrite) extra_flags=(--disable-template-data --disable-template-functions);; *) echo 'Static template flags require a fixed-payload workload' >&2; exit 1;; esac
      fi
      printf 'trial=%s client_cpus=%s client_gomaxprocs=%s static_template_flags=%s\n' "$trial" "${GHZ_CPUS:-2}" "${GHZ_GOMAXPROCS:-2}" "${GHZ_STATIC_DATA:-0}" >> "$RUN_DIR/environment.txt"
      echo "Warmup: $config concurrency=$concurrency trial=$trial"
      # ghz --duration overrides total; the measured run defaults to 60s.
      ghzrun "${extra_flags[@]}" --connections "${CONNECTIONS:-10}" --config "benchmarks/configs/$config.json" --concurrency "$concurrency" --duration "${WARMUP:-10s}" --format json --output "$stem-warmup.json" "$target"
      "$PYTHON" scripts/reports.py "$stem-warmup.json"
      docker stats --no-stream > "$stem-resources-before.txt"
      "$CTL" -addresses "$ADDRESSES" status > "$stem-status-before.jsonl"
      curl -fsSG "${PROMETHEUS_URL:-http://127.0.0.1:9090}/api/v1/query" --data-urlencode 'query=raftdb_term' > "$stem-terms-before.json"
      echo "Measuring: $config concurrency=$concurrency trial=$trial duration=${DURATION:-60s}"
      ghzrun "${extra_flags[@]}" --connections "${CONNECTIONS:-10}" --config "benchmarks/configs/$config.json" --concurrency "$concurrency" --duration "${DURATION:-60s}" --format json --output "$stem.json" "$target"
      "$CTL" -addresses "$ADDRESSES" status > "$stem-status-after.jsonl"
      "$PYTHON" scripts/capture-metrics.py "$stem.json"
      docker stats --no-stream > "$stem-resources-after.txt"
      "$PROBE" -addresses "$ADDRESSES" -mode ready
      [[ $("$CTL" -addresses "$ADDRESSES" leader) == "$leader" ]] || { echo 'Invalid run: leadership changed' >&2; exit 1; }
      "$PYTHON" scripts/reports.py "$stem.json" --status-before "$stem-status-before.jsonl" --status-after "$stem-status-after.jsonl" > "$stem-summary.json"
    done
  done
done
