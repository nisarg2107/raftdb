# RaftDB

A personal learning project: a durable, linearizable key-value database in Go,
using HashiCorp Raft and gRPC. Five Docker voters replicate writes; a majority of
three must agree before a successful write returns. HashiCorp Raft runs inside
each server process as a Go library.

[Quick start](#quick-start) · [Project commands](#project-commands) ·
[Seed and demo](#seed-data-and-dashboard-demo) · [Dashboards](#watch-the-metrics) ·
[Code flow](#read-the-code) · [Benchmarks](#benchmarks-and-measured-results)

## What the tools do

| Tool | Job |
|---|---|
| Go | Database, map FSM, clients, tests and command-line tools |
| HashiCorp Raft | Elections, ordered log replication, commitment and snapshot coordination |
| gRPC/protobuf | Client/server API and message definitions |
| WAL / Bolt | Durable Raft storage; Linux uses WAL, native Windows supports Bolt |
| Docker | Packages the executables and dependencies into containers |
| Docker Compose | Starts five database voters, their network/volumes, Prometheus and Grafana |
| Prometheus | Scrapes numerical metrics from each node and stores their time series |
| Grafana | Queries Prometheus and draws the provisioned dashboard |
| Official ghz 0.120.0 | Generates benchmark gRPC requests and measures successful throughput/latency |

The five voters run on **one physical machine**, sharing its CPU, RAM and disk.
Grafana and Prometheus show numerical telemetry, not the database's stored user
records. The example seed command prints those records after reading them back.

## Quick start

Install Docker with Compose v2 and Python 3.10+. If your executable is `python3`,
use that name (or `make PYTHON=python3`). Start Docker Desktop, then run
from the repository root:

```sh
python scripts/project.py demo
```

This builds/starts the existing five-voter cluster, loads
[examples/seed.json](examples/seed.json), verifies its values, and generates two
minutes of modest, real read/write activity. Open the dashboard while it runs:

- [Grafana: RaftDB - consensus and requests](http://localhost:3000/d/raftdb/raftdb-consensus-and-requests)
  — demo login **admin / raftdb-demo**.
- [Prometheus queries](http://localhost:9090/query) and
  [five scrape targets](http://localhost:9090/targets).

Docker builds the server and its client inside the image; the demo does not need
Go installed on the host. Host Go 1.23+ is needed for native builds/tests and the
benchmark's native setup/validation tools. Protobuf Go files are checked in.

## Project commands

Go already supplies `go build` and `go test`, so CMake is unnecessary here.
The Make targets are the equivalent of useful CMake custom targets. They forward
to one small, standard-library Python task runner; Windows users can use that
runner directly without installing Make.

| Make command | Cross-platform equivalent | Behavior |
|---|---|---|
| `make help` | `python scripts/project.py --help` | List project tasks |
| `make build` | `python scripts/project.py build` | Build native tools into `bin/`, adding `.exe` on Windows |
| `make up` | `python scripts/project.py up` | Build/start the same five voters and monitoring |
| `make status` | `python scripts/project.py status` | Report all five nodes |
| `make seed` | `python scripts/project.py seed` | Load and verify example records |
| `make demo` | `python scripts/project.py demo` | Seed records and generate real dashboard activity |
| `make down` | `python scripts/project.py down` | Stop the deployment, retaining data volumes |
| `make test` | `python scripts/project.py test` | Native Go tests/vet and Python checks |
| `make test-container` | `python scripts/project.py test --container` | Complete Linux race suite/vet and Python checks |
| `make benchmark` | `python scripts/project.py benchmark` | Prepare tools/images/cluster, then three 60s trials per workload |
| `make summary BENCH_OUTPUT=PATH` | `python scripts/project.py summary PATH` | Display each trial, arithmetic average, median and minimum/maximum |

A benchmark creates a fresh timestamped output directory by default. To name it,
use `make benchmark BENCH_OUTPUT=benchmarks/results/my-run`, or the Python
command's `--output` option. Reusing raw-trial filenames is refused.
`make demo DEMO_SECONDS=30` changes demo duration; `make seed SEED_FILE=PATH` selects another
seed file. All original low-level Go/Docker commands remain usable.

After `build`, try native CLI commands (append `.exe` to `raftctl` in PowerShell):

```sh
./bin/raftctl put greeting hello
./bin/raftctl get greeting
./bin/raftctl batch-get demo:project demo:user:ada missing
```

The checked-in `.env` selects the single `raftdb-verify` project and existing
named volumes. `down` preserves those volumes. Restart with `project.py up`;
`up --no-build` reuses existing images. Useful diagnostics:

```sh
docker compose ps
docker compose logs --tail 30 raftdb-node1
```

The current per-voter settings are four CPUs/GOMAXPROCS 4, 768 MiB memory limit,
512 MiB Go memory budget, write pipeline 2 and a 200us read collection window.
These limits share the host; they do not reserve five independent machines.

## Configuration variables

### Docker Compose settings

Compose reads the root `.env` file; environment variables set in your shell take
precedence. The current column includes the checked-in `.env` overrides. The
fallback applies when neither `.env` nor your shell supplies a value.

| Variable | Current value | Compose fallback | Purpose |
|---|---|---|---|
| `COMPOSE_PROJECT_NAME` | `raftdb-verify` | `raftdb` | Names the deployment and its volumes; keep it unchanged to reuse existing data |
| `RAFTDB_IMAGE` | `raftdb-verify:dev` | `raftdb:dev` | Server image tag |
| `RAFTDB_STORAGE` | `wal` | `bolt` | Durable storage backend; WAL requires Linux |
| `RAFTDB_CPUS` | `4.0` | `2.0` | CPU quota ceiling per voter |
| `RAFTDB_GOMAXPROCS` | `4` | `2` | Sets each server's Go `GOMAXPROCS` |
| `RAFTDB_WRITE_PIPELINE` | `2` | `8` | Maximum unresolved write batches; allowed 0-32, with 0 selecting 8 |
| `READ_BATCH_WINDOW` | `200us` | `200us` | Sets server `RAFTDB_READ_BATCH_WINDOW`; allowed 0-10ms |
| `BATCH_APPLY` | `false` | `false` | Sets server `RAFTDB_BATCH_APPLY`, enabling Raft apply-request buffering when true |
| `GRPC1` through `GRPC5` | `50051` through `50055` | Same | Localhost client ports, one per voter |
| `METRICS1` through `METRICS5` | `9091` through `9095` | Same | Localhost raw-metrics ports, one per voter |
| `PROMETHEUS_PORT` | `9090` | `9090` | Localhost Prometheus port |
| `GRAFANA_PORT` | `3000` | `3000` | Localhost Grafana port |

For example, add `GRAFANA_PORT=3001` to `.env`, run
`python scripts/project.py up --no-build`, and open `http://localhost:3001`.
The links above assume the default ports. Changing performance settings can
change benchmark results. The 768 MiB container limit and `GOMEMLIMIT=512MiB`
are fixed in `docker-compose.yml`, rather than exposed as `.env` variables.

### Native server settings

Native server processes do not load `.env`. They read the process environment,
then explicit command-line flags override those values. Invalid environment
values fail parsing even if a flag is also supplied. These are standalone
defaults; Compose supplies its own values from the table above.

| Environment variable | Matching flag | Standalone default / meaning |
|---|---|---|
| `RAFTDB_PEERS` | `-peers` | `node1=127.0.0.1:7001=127.0.0.1:50051`; comma-separated `id=raft-host:port=grpc-host:port` entries |
| `RAFTDB_STORAGE` | `-storage` | `bolt`; also accepts `wal` on Linux |
| `RAFTDB_WRITE_PIPELINE` | `-write-pipeline` | `8`; range 0-32, with 0 selecting 8 |
| `RAFTDB_BATCH_APPLY` | `-batch-apply` | `false`; enable Raft apply-request buffering |
| `RAFTDB_READ_BATCH_WINDOW` | `-read-batch-window` | `0s`; collection delay before a fresh quorum check, range 0-10ms |
| `RAFTDB_PPROF` | `-pprof` | Empty (disabled); optional private profiling address, e.g. `127.0.0.1:6060` |
| `RAFTDB_PROFILE` | `-heartbeat`, `-election`, `-lease` | Unset: `1s`, `1s`, `500ms`; `tuned`: `350ms`, `350ms`, `175ms`; individual flags override these defaults |

Run `./bin/raftdb -help` (or `./bin/raftdb.exe -help` on Windows) for all
server flags, including node addresses, data directory, timeouts and snapshot
settings. Compose supplies the five-node peer list directly. Adding other
native-server variables to `.env` alone does not pass them into containers;
they must also be listed in the Compose service's `environment` mapping.

### Make command variables

Pass these as `make NAME=value target`; they configure the task, not the database.

| Variable | Default | Example |
|---|---|---|
| `PYTHON` | `python` | `make PYTHON=python3 help` |
| `SEED_FILE` | `examples/seed.json` | `make seed SEED_FILE=examples/seed.json` |
| `DEMO_SECONDS` | `120` | `make demo DEMO_SECONDS=30` |
| `BENCH_OUTPUT` | Empty: a fresh timestamped directory for `benchmark` | `make benchmark BENCH_OUTPUT=benchmarks/results/my-run`; use that path with `make summary BENCH_OUTPUT=benchmarks/results/my-run` |
| `CONFIRM_RESET` | Unset: reset refused | `make reset CONFIRM_RESET=delete-all-raftdb-data` **deletes the deployment's data volumes**; use `make down` to retain data |

## Seed data and dashboard demo

The human-readable [seed file](examples/seed.json) contains eight records such as:

```json
{"key": "demo:user:ada", "value": "Ada Lovelace"}
```

`seed` validates the entire file, writes it in one atomic BatchPut, then verifies
all values with BatchGet. Rerunning it overwrites only those named keys.
Custom files use a JSON array of unique string key/value objects, with the same
1-128 entry / 256 KiB aggregate limit as the API.

For an already running cluster:

```sh
python scripts/project.py seed
python scripts/project.py demo --no-setup --duration 120
```

The demo updates `demo:counter` and reads the example records in a paced loop.
Every read is checked against expected values. It uses normal database APIs;
metrics and graphs are real, not injected or precomputed. Ctrl+C stops traffic
and leaves the database/monitoring running. After it ends, historical graphs
remain while live rates fall toward zero. Start another demo to see activity again.
**Stop demo traffic before benchmarking** so it cannot contaminate measurements.

## Watch the metrics

In Grafana choose **Last 5 minutes**; auto-refresh is two seconds. The main read
and write panels count acknowledged logical operations, including batches. The
**Logical operation throughput** panel separates methods. Also inspect p99 RPC
latency, errors, leader identity and durable storage-call duration. Use **Last
15 minutes** to review all six long benchmark trials after the run finishes.

In Prometheus choose **Graph** and paste:

```promql
# Successful logical reads/sec (excludes stale GetLocal)
sum(rate(raftdb_operations_total{method=~"Get|BatchGet"}[1m]))

# Successful logical writes/sec, including deletes
sum(rate(raftdb_operations_total{method=~"Put|BatchPut|Delete"}[1m]))

# Successful batch API calls/sec, not individual keys/sec
sum by (method) (rate(raftdb_requests_total{method=~"BatchGet|BatchPut",code="OK"}[1m]))
```

All five [Prometheus targets](http://localhost:9090/targets) should be **UP**.
[Node 1 raw metrics](http://localhost:9091/metrics) use port 9091; other nodes use
9092-9095. Graph rates are moving averages over the query window; seeding and
warmups appear there too. ghz reports only its measured interval. Do not multiply
cluster operation totals by five: replication is not another client operation.

## Read the code

| Location | Responsibility |
|---|---|
| `api/raftdb.proto` | API messages; skip generated `.pb.go` files on a first read |
| `cmd/raftctl` | CLI commands and benchmark dataset setup |
| `internal/server` | gRPC translation, errors and request instrumentation |
| `internal/cluster/reads.go`, `writes.go` | Read fencing and bounded write collection/completion |
| `internal/store` | Validation, binary encoding, deterministic map changes and snapshots |
| `internal/cluster/lifecycle.go`, `storage*.go` | Startup/shutdown, storage ownership and migration |
| `internal/config`, `internal/client` | Settings, reusable connections and leader discovery |
| `internal/metrics`, `internal/telemetry` | Registry and shared FSM/storage measurements |
| `cmd/raftprobe`, `tests/integration` | Readiness and real temporary-cluster correctness checks |
| `scripts/project.py`, `scripts/bench.py`, `scripts/benchlib` | Project tasks, official-ghz orchestration and reporting |

### Follow one request

First run the demo, then follow a matching handler from `cmd/raftctl/commands.go`
through `internal/server/grpc.go` to `Node.Write` or `Node.Read`:

```text
PUT -> validate -> acquire capacity -> queue -> bounded binary batch
    -> durable Raft replication -> quorum commit -> FSM map update -> response

GET -> fresh quorum confirmation -> wait for actual FSM application
    -> read the map -> recheck leadership/term -> response
```

In `writes.go`, read `enqueueWrite`, `batchWrites` and `finishWrites`. In
`reads.go`, follow `readFence`, `completeReadBatch` and `verifyAndDrain`. Then read
`FSM.ApplyBatch` / `applyLocked` in `store/fsm.go` and `GetMany` in `store/batch.go`.
Explore encoding, snapshots and lifecycle last.

Goroutines run work concurrently. Channels pass requests/completion signals;
closing `done` announces an already-written result. Mutexes protect shared data;
an RWMutex allows concurrent map readers or one writer. A Raft future represents
unfinished work; its `Error()` waits. An application fence waits for the actual
map to contain the required committed prefix.

The first read in a leader term initializes a committed fence. Later groups share
a fresh quorum check without logging each read. Later arrivals never reuse an old
completed check. BatchGet uses one map lock; each caller's ordered BatchPut stays
atomic. Read results are capped at 1 MiB; requests at 128 operations / 256 KiB.

The code uses concrete structs and focused packages. Some concurrency is essential:
submitted writes retain their admission capacity if the RPC cancels; timeouts do
not undo writes. The client retries mutations only on explicit follower rejection.
Separate unary/batch paths avoid extra unary allocations. Repeated validation at
different trust boundaries is intentional. `GetLocal` explicitly permits stale reads.
These safeguards remain even though this is a teaching project.

### Why the read number is high

Rates below count **key operations/sec**, not separate API calls. BatchGet shares
RPC/scheduling costs, a fresh read fence and a map lock across 32 key lookups.
Overlapping RPCs can share a fresh quorum round too. Reads access an in-memory map
and repeatedly use a small hot-key set; after initialization, read-only traffic
adds no log entry per group. Writes additionally require durable replication.

```text
logical operations/sec = successful RPCs/sec x operations per RPC
```

This is a favorable hot-key batch workload, not random disk I/O or hundreds of
thousands of unary Get RPCs/sec across independent physical servers.

## Benchmarks and measured results

The load generator is **official ghz 0.120.0**, built by `benchmarks/Dockerfile`.
Our scripts set up fixtures, invoke ghz and validate results; the demo loop is
never used as a benchmark. To reproduce the three-trial run:

```sh
python scripts/project.py benchmark --output benchmarks/results/my-three-trials
python scripts/project.py summary benchmarks/results/my-three-trials
```

The command builds native Go tools, starts the cluster and builds the pinned ghz
image before measuring. With an already prepared cluster, use `--no-setup`.
The direct equivalent, after building the tools/cluster/ghz image, is:

```sh
python scripts/bench.py run --output benchmarks/results/my-three-trials --repeats 3 --duration 60 --warmup 10
```

| Setting | Batch reads | Batch writes |
|---|---|---|
| RPC | BatchGet | BatchPut |
| Operations per RPC | 32 distinct keys | 64 distinct keys |
| Concurrent RPCs / reused connections | 32 / 4 | 16 / 4 |
| Outstanding logical operations | 1,024 | 1,024 |
| Workload | Fixed hot-key reads | Fixed hot-key overwrites, 32-byte values |
| Warmup / measured time per trial | 10s / 60s | 10s / 60s |
| Trials | 3 | 3 |

The driver seeds 10,000 keys and validates them on every voter before/after each
case, reusing current durable volumes. The ghz container has four CPUs, 2 GiB RAM,
GOMAXPROCS 4 and a 1 GiB Go memory budget. Every operation executes; writes wait
for durable quorum commitment and FSM application. A valid trial requires zero
errors, stable leadership/term, correct values and exact acknowledged-operation
counter agreement. Rejected attempts remain recorded.

Verified on 2026-10-20 on the current machine, with **three 60-second trials
per workload**, each after a ten-second warmup:

| Trial | Batched reads (operations/sec) | Committed batched writes (operations/sec) |
|---|---:|---:|
| 1 | 418,282.2 | 49,344.8 |
| 2 | 391,599.4 | 24,124.9 |
| 3 | 404,724.0 | 31,726.6 |
| **Arithmetic average** | **404,868.5** | **35,065.4** |
| Minimum | 391,599.4 | 24,124.9 |

All six trials had zero errors, stable leadership, validated data and exact
acknowledged-operation counts. The average read/write RPC rates were
12,652.1 / 547.9 calls/sec; each call held 32 reads or 64 writes.

Writes varied substantially. Mean StoreLogs call durations for trials 1-3 were
6.17 ms, 13.22 ms, 10.01 ms. These include storage waits; the physical cause was not isolated.
The same server image/settings were used throughout. The 35.1K write average
does not establish sustained 41K/48K writes/sec: the minimum was 24.1K.
The prior ~397K read / 43.9K write numbers were single trials, not part of this average.

Read and write workloads run separately; matching trial numbers are not
simultaneous mixed-load measurements.

The arithmetic average is `(trial 1 rate + trial 2 rate + trial 3 rate) / 3`.
The median is the middle value; the minimum is the conservative sustained rate.
Average per-trial p99 values are not a pooled p99. Results depend on hardware,
Docker resource limits and other host activity; exact rates are not guaranteed.

Full local evidence is retained under Git-ignored `benchmarks/results`. The compact
[verified results](benchmarks/verified-results.json) are intended for GitHub; large
raw ghz files remain local and should be shared separately when needed. The
summary includes trial rates, arithmetic averages, median and minimum/maximum.

`--read-batch-size 1 --write-batch-size 1` on `bench.py run` selects unary calls.
`screen` is a bounded diagnostic that restores the original pipeline. Short
screens/smokes never substitute for the three 60-second measurements.

## Tests and sharing

```sh
python scripts/project.py test --container
```

This runs the complete Linux race suite and vet, followed by Python tooling tests.
Integration tests use temporary storage and cover atomic batches, cancellation,
minority refusal, leader changes, snapshots/restart and follower catch-up. Fault
tools and optional profiling are separate from normal benchmark runs. Health is
process liveness; `raftprobe -mode ready` also checks commitment and linearizable
reads. A snapshot RPC can exceed its three-second wait and still finish afterward.

Before pushing, review `git status`: source, this README, the seed example and
compact verified results belong in Git. `.tools`, binaries and large raw results
are ignored. There is one project guide and one normal RaftDB deployment; no
production-readiness or independent-machine benchmark claim is made.
