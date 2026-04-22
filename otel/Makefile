# OTEL Testing & Validation Infrastructure
# See README.md for usage docs.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT_DIR   := $(shell cd .. && pwd)
FLUREE_DIR := $(CURDIR)/.fluree
STORAGE    := $(FLUREE_DIR)/storage
GEN_DIR    := $(CURDIR)/_data/generated
PID_FILE   := $(FLUREE_DIR)/server.pid
LOG_FILE   := $(FLUREE_DIR)/server.log
SERVER_BIN := $(ROOT_DIR)/target/release/fluree-server
CLI_BIN    := $(ROOT_DIR)/target/release/fluree

# ── Configurable ─────────────────────────────────────────────────────────────
PORT       ?= 8090
BASE_URL   ?= http://localhost:$(PORT)
LEDGER     ?= otel-test:main
ENTITIES   ?= 100000
PROPS      ?= 6
INDEXING   ?= true

STRESS_PRODUCTS ?= 50000
STRESS_BATCH    ?= 500

RUST_LOG   ?= info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug,fluree_db_api=debug

# ── Infrastructure lifecycle ─────────────────────────────────────────────────

.PHONY: up down reset ui jaeger-diag

up: ## Start Jaeger via docker compose
	docker compose up -d
	@echo "Jaeger UI: http://localhost:16686"

down: ## Stop and remove Jaeger container
	docker compose down

reset: ## Restart Jaeger (clears all trace data)
	docker compose down
	docker compose up -d
	@echo "Jaeger restarted — trace data cleared"

ui: ## Open Jaeger UI in browser
	@open http://localhost:16686 2>/dev/null || xdg-open http://localhost:16686 2>/dev/null || echo "Open http://localhost:16686"

jaeger-diag: ## Check Jaeger container health, memory, and API status
	@echo "── Container status ──"
	@docker inspect fluree-jaeger --format='Running={{.State.Running}}  OOMKilled={{.State.OOMKilled}}  ExitCode={{.State.ExitCode}}' 2>/dev/null || echo "  Container not found"
	@echo ""
	@echo "── Memory usage ──"
	@docker stats fluree-jaeger --no-stream --format='  {{.MemUsage}}  ({{.MemPerc}} of limit)' 2>/dev/null || echo "  Container not running"
	@echo ""
	@echo "── API health ──"
	@curl -sf "http://localhost:16686/api/services" \
		| python3 -c "import json,sys; d=json.load(sys.stdin); svcs=d.get('data',[]); print(f'  Services: {svcs}  ({len(svcs)} total)')" \
		2>/dev/null || echo "  API unreachable"

# ── Build ────────────────────────────────────────────────────────────────────

.PHONY: build

build: ## Build server and CLI binaries with otel feature (release)
	cargo build -p fluree-db-server --features otel --release
	cargo build -p fluree-db-cli --features otel --release
	@echo "Built: $(SERVER_BIN)"
	@echo "Built: $(CLI_BIN)"

# ── Project initialization ───────────────────────────────────────────────────

.PHONY: init config stress-config

init: ## Initialize .fluree/ project directory with config
	@if [ ! -x $(CLI_BIN) ]; then echo "Binary not found: $(CLI_BIN)"; echo "Run 'make build' first."; exit 1; fi
	@if [ ! -d .fluree ]; then \
		$(CLI_BIN) init; \
		echo "Initialized .fluree/ project directory"; \
	fi
	@$(MAKE) --no-print-directory config

config: ## Apply OTEL-specific server configuration via CLI
	@$(CLI_BIN) config set server.storage_path ".fluree/storage"
	@$(CLI_BIN) config set server.listen_addr "0.0.0.0:$(PORT)"
	@$(CLI_BIN) config set server.indexing.enabled $(INDEXING)
	@$(CLI_BIN) config set server.indexing.reindex_min_bytes 100000
	@$(CLI_BIN) config set server.indexing.reindex_max_bytes 1000000000

stress-config: config ## Alias: pre-configure high novelty limits (1GB) for stress testing

# ── Server lifecycle ─────────────────────────────────────────────────────────

.PHONY: server server-stop server-logs

server: init ## Start fluree-server in background with OTEL export
	@if [ ! -x $(SERVER_BIN) ]; then echo "Binary not found: $(SERVER_BIN)"; echo "Run 'make build' first."; exit 1; fi
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Server already running (PID $$(cat $(PID_FILE)))"; \
		exit 0; \
	fi
	@OTEL_SERVICE_NAME=fluree-server \
	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
	RUST_LOG=$(RUST_LOG) \
	$(SERVER_BIN) \
		> $(LOG_FILE) 2>&1 & \
	echo $$! > $(PID_FILE) && \
	echo "Server starting (PID $$(cat $(PID_FILE))), logs: $(LOG_FILE)" && \
	./scripts/wait-for-server.sh $(BASE_URL) 30

server-stop: ## Stop the background server
	@if [ -f $(PID_FILE) ]; then \
		PID=$$(cat $(PID_FILE)); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID; \
			echo "Server stopped (PID $$PID)"; \
		else \
			echo "Server not running (stale PID $$PID)"; \
		fi; \
		rm -f $(PID_FILE); \
	else \
		echo "No PID file found"; \
	fi

server-logs: ## Tail server stdout/stderr
	@if [ -f $(LOG_FILE) ]; then \
		tail -f $(LOG_FILE); \
	else \
		echo "No log file found at $(LOG_FILE)"; \
	fi

# ── Scenarios ────────────────────────────────────────────────────────────────

.PHONY: seed transact query index import smoke cycle stress stress-query

seed: ## Create and populate the test ledger (required before transact/query/index)
	./scripts/seed-ledger.sh $(BASE_URL) $(LEDGER)

transact: ## Minimal transaction scenarios (insert, upsert, update, turtle, SPARQL UPDATE)
	./scripts/transact-smoke.sh $(BASE_URL) $(LEDGER)

query: ## Seed data + query via FQL and SPARQL
	./scripts/query-smoke.sh $(BASE_URL) $(LEDGER)

index: ## Insert enough data to trigger indexing, then query indexed data
	./scripts/index-smoke.sh $(BASE_URL) $(LEDGER)

import: generate init ## Bulk import via CLI with OTEL tracing (no server needed)
	@if [ ! -x $(CLI_BIN) ]; then echo "Binary not found: $(CLI_BIN)"; echo "Run 'make build' first."; exit 1; fi
	./scripts/import-smoke.sh $(CLI_BIN) $(GEN_DIR)/data.ttl $(CURDIR) $(LEDGER)

smoke: ## Full cycle: seed, transact, query, index — end-to-end span waterfall
	./scripts/seed-ledger.sh $(BASE_URL) $(LEDGER)
	./scripts/full-cycle.sh $(BASE_URL) $(LEDGER)

stress: init ## High-volume stress test: 50K inserts with backpressure + expensive queries
	SERVER_BIN=$(SERVER_BIN) FLUREE_DIR=$(FLUREE_DIR) PID_FILE=$(PID_FILE) LOG_FILE=$(LOG_FILE) \
	PORT=$(PORT) RUST_LOG='$(RUST_LOG)' INDEXING=$(INDEXING) \
	./scripts/stress-test.sh $(BASE_URL) $(LEDGER) $(STRESS_PRODUCTS) $(STRESS_BATCH)

stress-query: ## Re-run only the expensive query battery (assumes stress data already loaded)
	./scripts/stress-query.sh $(BASE_URL) $(LEDGER)

cycle: ## Sustained transact + query workload triggering multiple index cycles
	@echo "Running 3 full cycles to trigger multiple index rebuilds..."
	@for i in 1 2 3; do \
		echo ""; \
		echo "=== Cycle $$i/3 ==="; \
		./scripts/full-cycle.sh $(BASE_URL) $(LEDGER); \
		sleep 2; \
	done
	@echo ""
	@echo "Done. Check Jaeger for multi-cycle trace patterns: http://localhost:16686"

# ── Data generation & cleanup ────────────────────────────────────────────────

.PHONY: generate clean clean-all nuke

generate: ## Generate TTL data files (configurable: ENTITIES, PROPS)
	./scripts/generate-data.sh $(ENTITIES) $(PROPS) $(GEN_DIR)

clean: server-stop ## Remove .fluree/ and _data/ (all state + generated data)
	rm -rf $(FLUREE_DIR) $(CURDIR)/_data
	@echo "Cleaned: $(FLUREE_DIR) and _data/"

clean-all: clean down ## clean + stop Docker

nuke: clean-all ## clean-all + remove compiled binaries
	rm -f $(SERVER_BIN) $(CLI_BIN)
	@echo "Removed compiled binaries"

# ── Convenience combos ───────────────────────────────────────────────────────

.PHONY: all fresh

all: up build init server smoke ## Full setup: Jaeger + build + init + server + smoke test
	@echo ""
	@echo "All done. Open Jaeger: http://localhost:16686"
	@echo "Search for service: fluree-server"

fresh: reset clean build init server smoke ## Clean-slate re-run: reset Jaeger + rebuild + smoke
	@echo ""
	@echo "Fresh run complete. Open Jaeger: http://localhost:16686"

# ── Help ─────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@echo "OTEL Testing Infrastructure"
	@echo ""
	@echo "Usage: make [target] [VAR=value ...]"
	@echo ""
	@echo "Configurable variables:"
	@echo "  PORT=$(PORT)            Server listen port (applied via fluree config)"
	@echo "  LEDGER=$(LEDGER)    Ledger alias"
	@echo "  ENTITIES=$(ENTITIES)       Entity count for data generation"
	@echo "  INDEXING=$(INDEXING)         Enable background indexing (applied via fluree config)"
	@echo "  RUST_LOG=...            Log filter (see README.md)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'
