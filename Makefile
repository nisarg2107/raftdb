.PHONY: help build up down status seed demo test test-tools test-integration test-container test-faults benchmark summary benchmark-matrix proto reset
PYTHON ?= python
BENCH_OUTPUT ?=
DEMO_SECONDS ?= 120
SEED_FILE ?= examples/seed.json
help:
	$(PYTHON) scripts/project.py --help
build:
	$(PYTHON) scripts/project.py build
up:
	$(PYTHON) scripts/project.py up
down:
	$(PYTHON) scripts/project.py down
status:
	$(PYTHON) scripts/project.py status
seed:
	$(PYTHON) scripts/project.py seed --file "$(SEED_FILE)"
demo:
	$(PYTHON) scripts/project.py demo --duration "$(DEMO_SECONDS)"
test:
	$(PYTHON) scripts/project.py test
test-tools:
	$(PYTHON) -m unittest discover -s scripts -p '*test*.py'
test-integration:
	go test -race -timeout 180s ./tests/integration ./internal/cluster
test-container:
	$(PYTHON) scripts/project.py test --container
test-faults:
	bash scripts/chaos/scenarios.sh
benchmark:
	$(PYTHON) scripts/project.py benchmark $(if $(BENCH_OUTPUT),--output "$(BENCH_OUTPUT)")
summary:
	$(PYTHON) scripts/project.py summary $(if $(BENCH_OUTPUT),"$(BENCH_OUTPUT)")
benchmark-matrix:
	$(PYTHON) scripts/bench.py matrix --workload read
	$(PYTHON) scripts/bench.py matrix --workload write
proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/raftdb.proto
reset:
	@test "$(CONFIRM_RESET)" = "delete-all-raftdb-data" || (echo 'Set CONFIRM_RESET=delete-all-raftdb-data'; exit 1)
	docker compose down --volumes
