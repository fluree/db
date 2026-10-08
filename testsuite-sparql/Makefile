# testsuite-sparql/Makefile
#
# Developer targets for the W3C SPARQL compliance test suite.
# Run `make help` to see available targets.
#
# Model: `cargo test` runs EVERY registered W3C suite (no #[ignore]). A suite
# is green when each test passes or appears in tests/registers/mod.rs, and
# check_testsuite fails on unexpected failures AND stale register entries.
# CI invokes `cargo test` directly (identical to `make ci`).

CARGO := cargo
RDF_TESTS := rdf-tests

# W3C URL prefix → local path prefix
W3C_PREFIX := https://w3c.github.io/rdf-tests/
LOCAL_PREFIX := $(RDF_TESTS)/

.PHONY: help test ci test-cat report-cat report-10-json \
        summary classify failures-cat \
        investigate show-query check clean update-submodule

help: ## Show this help
	@echo "W3C SPARQL Compliance Test Suite"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-24s %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make test                           Run the full compliance suite (same as CI)"
	@echo "  make test-cat CAT=sparql11_functions   Run one suite"
	@echo "  make report-cat CAT=sparql11_update_tests   JSON report for one suite"
	@echo "  make summary REPORT=report-sparql11_update_tests.json"
	@echo "  make investigate TEST=pp16          Find files/queries for a test"
	@echo ""
	@echo "Suite names = test fn names in tests/w3c_sparql.rs (sparql10_*,"
	@echo "sparql11_*, sparql12_*)."

# ---------------------------------------------------------------------------
# Running tests
# ---------------------------------------------------------------------------

test: ## Run the FULL W3C compliance suite (all suites; green-gated)
	$(CARGO) test -- --nocapture 2>&1

ci: ## CI target: identical to `make test` (CI runs plain `cargo test`)
	$(CARGO) test 2>&1

test-cat: ## Run one suite (CAT=<test fn name, e.g. sparql11_functions>)
ifndef CAT
	@echo "Usage: make test-cat CAT=<suite fn name>"
	@echo "e.g.   make test-cat CAT=sparql11_functions"
	@grep -oE '^fn (sparql[0-9]+_[a-z0-9_]+)' tests/w3c_sparql.rs 2>/dev/null | sed 's/^fn /  /' || true
	@exit 1
endif
	$(CARGO) test --test w3c_sparql $(CAT) -- --exact --nocapture 2>&1

# ---------------------------------------------------------------------------
# Reports and analysis
#
# Each suite writes its own JSON when W3C_REPORT_JSON is set. Reports are
# per-suite: generate one with report-cat, then analyze with summary/classify.
# ---------------------------------------------------------------------------

report-cat: ## JSON report for one suite (CAT=<suite fn name>)
ifndef CAT
	@echo "Usage: make report-cat CAT=<suite fn name>"
	@exit 1
endif
	@echo "Running $(CAT) (JSON output → report-$(CAT).json)..."
	@W3C_REPORT_JSON=report-$(CAT).json $(CARGO) test --test w3c_sparql $(CAT) \
		-- --exact > /dev/null 2>&1; true
	@python3 -c "import json; r=json.load(open('report-$(CAT).json')); print(f'  Total: {r[\"total\"]}  Passed: {r[\"passed\"]}  Ignored: {r[\"ignored\"]}  Failed: {r[\"failed\"]}  Rate: {r[\"pass_rate\"]}')" 2>/dev/null || true

report-10-json: ## JSON report for the SPARQL 1.0 eval suite
	@$(MAKE) report-cat CAT=sparql10_query_eval_tests

ANALYZE := python3 scripts/analyze_report.py
REPORT ?=

summary: ## Per-category breakdown of a JSON report (REPORT=<file.json>)
ifndef REPORT
	@echo "Usage: make summary REPORT=report-<suite>.json  (generate with report-cat)"
	@exit 1
endif
	@$(ANALYZE) summary $(REPORT)

classify: ## Group failures in a JSON report by error type (REPORT=<file.json>)
ifndef REPORT
	@echo "Usage: make classify REPORT=report-<suite>.json  (generate with report-cat)"
	@exit 1
endif
	@$(ANALYZE) classify $(REPORT)

failures-cat: ## List failures in a JSON report (REPORT=<file.json>)
ifndef REPORT
	@echo "Usage: make failures-cat REPORT=report-<suite>.json"
	@exit 1
endif
	@$(ANALYZE) failures $(REPORT)

# ---------------------------------------------------------------------------
# Investigating specific tests
# ---------------------------------------------------------------------------

investigate: ## Show files matching a test (TEST=<test id fragment>)
ifndef TEST
	@echo "Usage: make investigate TEST=<test_id or fragment, e.g. pp16>"
	@exit 1
endif
	@echo "=== Test: $(TEST) ==="
	@echo ""
	@echo "--- Register entries ---"
	@grep -n "$(TEST)" tests/registers/mod.rs 2>/dev/null || echo "(not in any skip register)"
	@echo ""
	@echo "--- Matching test files ---"
	@find $(RDF_TESTS) -name "*$(TEST)*" 2>/dev/null || echo "(no matching files found)"

show-query: ## Print the .rq/.ru file for a test (TEST=<path fragment>)
ifndef TEST
	@echo "Usage: make show-query TEST=<filename>"
	@echo "Example: make show-query TEST=syntax-select-expr-04.rq"
	@exit 1
endif
	@find $(RDF_TESTS) \( -name "*$(TEST)*" \) -exec echo "--- {} ---" \; -exec cat {} \; 2>/dev/null || \
		echo "No file matching '$(TEST)' found in $(RDF_TESTS)/"

# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------

update-submodule: ## Update the W3C rdf-tests submodule to latest
	cd $(RDF_TESTS) && git pull origin main
	@echo ""
	@echo "Submodule updated. Run 'make test' — new tests fail loudly until"
	@echo "triaged (fixed or added to tests/registers/mod.rs with rationale)."

check: ## Run cargo check on the testsuite crate
	$(CARGO) check

clean: ## Remove generated report files
	rm -f report*.json report*.txt failures.txt
