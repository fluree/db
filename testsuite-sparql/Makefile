# testsuite-sparql/Makefile
#
# Developer targets for the W3C SPARQL compliance test suite.
# Run `make help` to see available targets.

CARGO := cargo
REPORT := report.txt
FAILURES_REPORT := failures.txt
RDF_TESTS := rdf-tests

# W3C URL prefix → local path prefix
W3C_PREFIX := https://w3c.github.io/rdf-tests/
LOCAL_PREFIX := $(RDF_TESTS)/

.PHONY: help test test-all test-syntax11 test-syntax10 test-syntax-update \
        test-eval test-eval-cat test-eval10 test-update test-fed \
        test-results test-entailment \
        report report-eval report-json report-eval-json report-10-json \
        failures failures-list count count-eval \
        summary classify failures-eval cat-json \
        investigate investigate-eval show-query check clean update-submodule

help: ## Show this help
	@echo "W3C SPARQL Compliance Test Suite"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-24s %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make test                           Run syntax tests (CI-safe)"
	@echo "  make test-all                       Run ALL tests including ignored"
	@echo "  make test-eval                      Run full 1.1 eval suite (~5 min)"
	@echo "  make test-eval-cat CAT=functions    Run one eval category"
	@echo "  make test-eval10                    Run SPARQL 1.0 eval tests"
	@echo "  make count-eval                     Quick pass/fail counts for eval"
	@echo "  make report-eval-json               Generate JSON report for 1.1 eval"
	@echo "  make summary                        Per-category pass/fail breakdown"
	@echo "  make classify                       Group failures by error type"
	@echo "  make cat-json CAT=functions         JSON report for a single category"
	@echo "  make investigate TEST=...           Show context for a specific test"
	@echo "  make investigate-eval TEST=...      Show context in eval report"
	@echo "  make show-query TEST=...            Print the .rq file for a test"

# ---------------------------------------------------------------------------
# Running tests
# ---------------------------------------------------------------------------

test: ## Run all non-ignored tests (syntax; CI-safe)
	$(CARGO) test -- --nocapture 2>&1

test-all: ## Run ALL registered W3C tests including ignored
	$(CARGO) test -- --nocapture --include-ignored 2>&1

test-syntax11: ## Run SPARQL 1.1 query syntax tests only
	$(CARGO) test sparql11_syntax_query_tests -- --nocapture 2>&1

test-syntax10: ## Run SPARQL 1.0 syntax tests only
	$(CARGO) test sparql10_syntax_tests -- --nocapture 2>&1

test-syntax-update: ## Run SPARQL 1.1 update syntax tests
	$(CARGO) test sparql11_syntax_update -- --nocapture 2>&1

test-eval: ## Run full SPARQL 1.1 query evaluation suite (~5 min)
	$(CARGO) test sparql11_query_w3c_testsuite -- --nocapture --include-ignored 2>&1

test-eval-cat: ## Run one eval category (CAT=aggregates|bind|functions|...)
ifndef CAT
	@echo "Usage: make test-eval-cat CAT=<category>"
	@echo ""
	@echo "SPARQL 1.1 categories:"
	@echo "  aggregates, bind, bindings, cast, construct, exists,"
	@echo "  functions, grouping, negation, project_expression,"
	@echo "  property_path, subquery"
	@exit 1
endif
	$(CARGO) test sparql11_$(CAT) -- --nocapture --include-ignored 2>&1

test-eval10: ## Run SPARQL 1.0 query evaluation tests
	$(CARGO) test sparql10_query_eval -- --nocapture --include-ignored 2>&1

test-update: ## Run SPARQL 1.1 update tests (syntax + eval)
	$(CARGO) test sparql11_update -- --nocapture --include-ignored 2>&1

test-fed: ## Run SPARQL 1.1 federation tests (syntax + service)
	$(CARGO) test sparql11_federation -- --nocapture --include-ignored 2>&1
	$(CARGO) test sparql11_service -- --nocapture --include-ignored 2>&1

test-results: ## Run SPARQL 1.1 result format tests (JSON + CSV/TSV)
	$(CARGO) test sparql11_json_result -- --nocapture --include-ignored 2>&1
	$(CARGO) test sparql11_csv_tsv -- --nocapture --include-ignored 2>&1

test-entailment: ## Run SPARQL 1.1 entailment tests
	$(CARGO) test sparql11_entailment -- --nocapture --include-ignored 2>&1

# ---------------------------------------------------------------------------
# Reports and analysis
# ---------------------------------------------------------------------------

report: ## Run syntax tests and save full output to report.txt
	@echo "Running W3C SPARQL syntax tests (output → $(REPORT))..."
	@$(CARGO) test -- --nocapture > $(REPORT) 2>&1; true
	@grep -E "(=== Test Summary|Total:|Passed:|Ignored:|Failed:)" $(REPORT) || true
	@echo "Full report saved to $(REPORT)"

report-eval: ## Run eval tests and save full output to report-eval.txt
	@echo "Running W3C SPARQL eval tests (output → report-eval.txt)..."
	@$(CARGO) test sparql11_query_w3c_testsuite -- --nocapture --include-ignored > report-eval.txt 2>&1; true
	@grep -E "(=== Test Summary|Total:|Passed:|Ignored:|Failed:)" report-eval.txt || true
	@echo "Full report saved to report-eval.txt"

report-json: ## Generate machine-readable JSON report for all tests
	@echo "Running all W3C SPARQL tests (JSON output → report.json)..."
	@echo "  (using --test-threads=1 to avoid concurrent report writes)"
	@W3C_REPORT_JSON=report.json $(CARGO) test -- --nocapture --include-ignored --test-threads=1 > /dev/null 2>&1; true
	@echo "JSON report written to report.json"
	@python3 -c "import json; r=json.load(open('report.json')); print(f'  Total: {r[\"total\"]}  Passed: {r[\"passed\"]}  Ignored: {r[\"ignored\"]}  Failed: {r[\"failed\"]}  Rate: {r[\"pass_rate\"]}')" 2>/dev/null || true

report-eval-json: ## Generate JSON report for SPARQL 1.1 eval tests
	@echo "Running W3C SPARQL 1.1 eval tests (JSON output → report-eval.json)..."
	@W3C_REPORT_JSON=report-eval.json $(CARGO) test sparql11_query_w3c_testsuite \
		-- --nocapture --include-ignored --test-threads=1 > /dev/null 2>&1; true
	@echo "JSON report written to report-eval.json"
	@python3 -c "import json; r=json.load(open('report-eval.json')); print(f'  Total: {r[\"total\"]}  Passed: {r[\"passed\"]}  Ignored: {r[\"ignored\"]}  Failed: {r[\"failed\"]}  Rate: {r[\"pass_rate\"]}')" 2>/dev/null || true

report-10-json: ## Generate JSON report for SPARQL 1.0 eval tests
	@echo "Running W3C SPARQL 1.0 eval tests (JSON output → report-10.json)..."
	@W3C_REPORT_JSON=report-10.json $(CARGO) test sparql10_query_eval \
		-- --nocapture --include-ignored --test-threads=1 > /dev/null 2>&1; true
	@echo "JSON report written to report-10.json"
	@python3 -c "import json; r=json.load(open('report-10.json')); print(f'  Total: {r[\"total\"]}  Passed: {r[\"passed\"]}  Ignored: {r[\"ignored\"]}  Failed: {r[\"failed\"]}  Rate: {r[\"pass_rate\"]}')" 2>/dev/null || true

failures: report ## Show only failing tests (extracted from report)
	@echo ""
	@awk '/failing test\(s\):/{found=1; print ""; print; next} found && /^test .*\.\.\. FAILED/{found=0; next} found{print}' $(REPORT) \
		|| echo "(no failures)"

failures-list: report ## One-line-per-failure summary (test ID + error type)
	@echo ""
	@echo "=== Failure Summary ==="
	@awk '/failing test\(s\):/{found=1; next} found && /^test .*\.\.\. FAILED/{found=0; next} found && /^http/{print}' $(REPORT) \
		|| echo "(no failures)"

count: ## Show pass/fail summary counts (syntax tests)
	@$(CARGO) test -- --nocapture 2>&1 | grep -E "(Total|Passed|Ignored|Failed|=== Test Summary)" || true

count-eval: ## Show pass/fail counts for 1.1 eval tests
	@$(CARGO) test sparql11_query_w3c_testsuite -- --nocapture --include-ignored 2>&1 | grep -E "(Total|Passed|Ignored|Failed|=== Test Summary)" || true

# ---------------------------------------------------------------------------
# Analysis (require report-eval.json — run `make report-eval-json` first)
# ---------------------------------------------------------------------------

ANALYZE := python3 scripts/analyze_report.py

summary: ## Per-category pass/fail breakdown (generates report if needed)
	@if [ ! -f report-eval.json ]; then \
		echo "No report-eval.json found — generating (this takes ~5 min)..."; \
		$(MAKE) report-eval-json; \
	fi
	@echo ""
	@echo "=== Per-Category Breakdown (SPARQL 1.1 Eval) ==="
	@echo ""
	@$(ANALYZE) summary report-eval.json

classify: ## Group eval failures by error type (generates report if needed)
	@if [ ! -f report-eval.json ]; then \
		echo "No report-eval.json found — generating (this takes ~5 min)..."; \
		$(MAKE) report-eval-json; \
	fi
	@echo ""
	@echo "=== Failure Classification (SPARQL 1.1 Eval) ==="
	@echo ""
	@$(ANALYZE) classify report-eval.json

failures-eval: ## List all eval failures with error type (--category CAT to filter)
	@if [ ! -f report-eval.json ]; then \
		echo "No report-eval.json found — generating (this takes ~5 min)..."; \
		$(MAKE) report-eval-json; \
	fi
ifdef CAT
	@$(ANALYZE) failures report-eval.json --category $(CAT)
else
	@$(ANALYZE) failures report-eval.json
endif

cat-json: ## JSON report + summary for one eval category (CAT=functions|bind|...)
ifndef CAT
	@echo "Usage: make cat-json CAT=<category>"
	@echo ""
	@echo "SPARQL 1.1 categories:"
	@echo "  aggregates, bind, bindings, cast, construct, exists,"
	@echo "  functions, grouping, negation, project_expression,"
	@echo "  property_path, subquery"
	@exit 1
endif
	@echo "Running W3C SPARQL 1.1 $(CAT) tests (JSON output → report-$(CAT).json)..."
	@W3C_REPORT_JSON=report-$(CAT).json $(CARGO) test sparql11_$(CAT) \
		-- --nocapture --include-ignored --test-threads=1 > /dev/null 2>&1; true
	@echo "JSON report written to report-$(CAT).json"
	@python3 -c "import json; r=json.load(open('report-$(CAT).json')); print(f'  Total: {r[\"total\"]}  Passed: {r[\"passed\"]}  Failed: {r[\"failed\"]}  Rate: {r[\"pass_rate\"]}')" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Investigating specific tests
# ---------------------------------------------------------------------------

investigate: ## Show all context for a specific test (TEST=<test_url_or_fragment>)
ifndef TEST
	@echo "Usage: make investigate TEST=<test_id>"
	@echo ""
	@echo "TEST can be:"
	@echo "  - Full URL: https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl#test_34"
	@echo "  - Fragment: test_34  (will search report.txt for matches)"
	@exit 1
endif
	@echo "=== Test: $(TEST) ==="
	@echo ""
	@echo "--- Searching report for test ---"
	@grep "$(TEST)" $(REPORT) 2>/dev/null || echo "(not found in report — run 'make report' first)"
	@echo ""
	@echo "--- Matching .rq files ---"
	@find $(RDF_TESTS) -name "*.rq" | xargs grep -l "$(TEST)" 2>/dev/null || \
		find $(RDF_TESTS) -name "*$(TEST)*" 2>/dev/null || \
		echo "(no matching query files found)"

investigate-eval: ## Show eval test context (TEST=<test_url_or_fragment>)
ifndef TEST
	@echo "Usage: make investigate-eval TEST=<test_id>"
	@echo ""
	@echo "TEST can be:"
	@echo "  - Full URL fragment: agg01"
	@echo "  - Test name: substring01"
	@echo "  (searches report-eval.txt — run 'make report-eval' first)"
	@exit 1
endif
	@echo "=== Eval Test: $(TEST) ==="
	@echo ""
	@echo "--- Searching eval report ---"
	@grep "$(TEST)" report-eval.txt 2>/dev/null || echo "(not found in report-eval.txt — run 'make report-eval' first)"
	@echo ""
	@echo "--- Matching test files ---"
	@find $(RDF_TESTS) -name "*$(TEST)*" 2>/dev/null || echo "(no matching files found)"

show-query: ## Print the .rq query file for a test (TEST=<path_fragment>)
ifndef TEST
	@echo "Usage: make show-query TEST=<filename>"
	@echo "Example: make show-query TEST=syntax-select-expr-04.rq"
	@exit 1
endif
	@find $(RDF_TESTS) -name "*$(TEST)*" -exec echo "--- {} ---" \; -exec cat {} \; 2>/dev/null || \
		echo "No file matching '$(TEST)' found in $(RDF_TESTS)/"

# ---------------------------------------------------------------------------
# CI integration
#
# CI should run `make ci` which only executes tests that are expected to pass
# (syntax tests). Eval tests can be added to CI incrementally by removing
# #[ignore] from individual test functions as categories become green.
# ---------------------------------------------------------------------------

ci: ## CI target: run only tests that are expected to pass
	$(CARGO) test 2>&1

# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------

update-submodule: ## Update the W3C rdf-tests submodule to latest
	cd $(RDF_TESTS) && git pull origin main
	@echo ""
	@echo "Submodule updated. Run 'make test' to check for regressions."

check: ## Run cargo check on the testsuite crate
	$(CARGO) check

clean: ## Remove generated report files
	rm -f $(REPORT) $(FAILURES_REPORT) report-eval.txt report.json report-eval.json report-10.json report-*.json
