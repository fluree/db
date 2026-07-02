# testsuite-shacl/Makefile
#
# Developer targets for the W3C SHACL core compliance test suite.
# Run `make help` to see available targets.
#
# NOTE: this crate is excluded from the workspace — run make (or cargo)
# from inside testsuite-shacl/.

CARGO := cargo
REPORT_JSON := report.json

.PHONY: help test count summary test-cat test-one report-json failures show clean update-submodule

help: ## Show this help
	@echo "W3C SHACL Core Compliance Test Suite"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make count                        # overall pass/fail numbers"
	@echo "  make summary                      # per-category table"
	@echo "  make test-cat CAT=node            # run one category"
	@echo "  make test-one TEST=minLength-001  # run matching tests, full output"
	@echo "  make failures                     # list failing tests with reasons"
	@echo "  make show TEST=node/minLength-001 # print a test file"

test: ## Run the full core suite (report-only; never fails the build)
	@$(CARGO) test --test w3c_shacl -- --nocapture

count: ## Overall pass/fail numbers
	@$(CARGO) test --test w3c_shacl -- --nocapture 2>&1 | grep -E "=== Test Summary" || true

summary: ## Per-category pass/fail table
	@$(CARGO) test --test w3c_shacl -- --nocapture 2>&1 | \
		sed -n '/=== W3C SHACL Core/,/=== Test Summary/p'

test-cat: ## Run one category: make test-cat CAT=node
	@test -n "$(CAT)" || (echo "usage: make test-cat CAT=node" && exit 2)
	@SHACL_CATEGORY=$(CAT) $(CARGO) test --test w3c_shacl -- --nocapture

test-one: ## Run tests whose name matches: make test-one TEST=minLength-001
	@test -n "$(TEST)" || (echo "usage: make test-one TEST=minLength-001" && exit 2)
	@SHACL_TEST=$(TEST) $(CARGO) test --test w3c_shacl -- --nocapture

report-json: ## Write machine-readable report to report.json
	@W3C_REPORT_JSON=$(REPORT_JSON) $(CARGO) test --test w3c_shacl -- --nocapture > /dev/null 2>&1; true
	@echo "JSON report written to $(REPORT_JSON)"
	@python3 -c "import json; r=json.load(open('$(REPORT_JSON)')); print(f'  Total: {r[\"total\"]}  Passed: {r[\"passed\"]}  Failed: {r[\"failed\"]}  Rate: {r[\"pass_rate\"]}')" 2>/dev/null || true

failures: ## List failing tests with reasons (from a fresh run)
	@$(CARGO) test --test w3c_shacl -- --nocapture 2>&1 | grep -A1 -E "^(FAIL|ERROR)" || echo "no failures"

show: ## Print a W3C test file: make show TEST=node/minLength-001
	@test -n "$(TEST)" || (echo "usage: make show TEST=node/minLength-001" && exit 2)
	@cat data-shapes/data-shapes-test-suite/tests/core/$(TEST).ttl

clean: ## Remove build artifacts and reports
	@$(CARGO) clean
	@rm -f $(REPORT_JSON)

update-submodule: ## Update the W3C data-shapes submodule to latest
	cd data-shapes && git pull origin gh-pages
	@echo ""
	@echo "Submodule updated. Run 'make count' to check for changes."
