# W3C SPARQL Compliance Test Suite

The `testsuite-sparql` crate runs **official W3C SPARQL test cases** against Fluree's parser and query engine. Every test is discovered automatically from W3C manifest files — there are zero hand-written test cases.

This guide covers how to run the suite, interpret results, and turn failures into fixes.

## Why This Exists

The W3C publishes its SPARQL test suite as RDF data. Each `manifest.ttl` file declares test entries: a query file, optional input data, and expected results. Every serious SPARQL implementation (Oxigraph, Apache Jena, Eclipse RDF4J) runs these manifests programmatically. We do the same.

The ratio is extraordinary: ~700 lines of Rust infrastructure drive 700+ W3C test cases. Each failure is a spec-backed bug report with built-in test data and expected results.

**Philosophy: failures are features.** When a test fails, the default response is to fix Fluree, not skip the test. Skip entries are reserved for documented, deliberate design divergences reviewed by the team.

## Quick Start

> **Important:** The `testsuite-sparql` crate is **excluded from the Cargo workspace** (see root `Cargo.toml`). You must `cd testsuite-sparql/` before running any `cargo` or `make` commands. Using `cargo test -p testsuite-sparql` from the workspace root will fail.

All commands below assume you are already in `testsuite-sparql/`.

### Run All Tests

```bash
cd testsuite-sparql
cargo test
```

This runs **every** registered W3C test suite: SPARQL 1.0 and 1.1 syntax,
all 1.1 query-evaluation categories, SPARQL UPDATE (syntax + evaluation),
result formats (JSON/CSV/TSV), SPARQL 1.2, and the infrastructure suites.
Every suite must be green: each test either passes or appears in that suite's
explicit skip register (see "Managing the Skip List"). CI runs exactly this
command — a new failure *or* a stale skip entry (a registered test that now
passes) fails the build.

### Run a Specific Suite

```bash
# SPARQL 1.1 syntax only
cargo test sparql11_syntax_query_tests

# SPARQL 1.0 syntax only
cargo test sparql10_syntax_tests

# Single evaluation category
cargo test sparql11_functions

# SPARQL UPDATE (syntax + evaluation)
cargo test sparql11_update_tests

# SPARQL 1.2 (RDF-star) suites
cargo test sparql12_
```

### Run With Verbose Output

```bash
cargo test -- --nocapture 2>&1
```

The suite writes progress to stderr (`Running test N: <test_id> ...`) and a summary at the end.

### Using the Makefile

The `testsuite-sparql/Makefile` provides convenience targets:

```bash
# --- Running tests ---
make test              # Full compliance suite (identical to CI)
make test-cat CAT=sparql11_functions
                       # Run one suite (fn names in tests/w3c_sparql.rs)

# --- Reports ---
make report-cat CAT=sparql11_update_tests
                       # JSON report for one suite -> report-<suite>.json
make report-10-json    # JSON report for the SPARQL 1.0 eval suite

# --- Analysis (on a report generated above) ---
make summary REPORT=report-sparql11_update_tests.json
make classify REPORT=report-sparql11_update_tests.json
make failures-cat REPORT=report-sparql11_update_tests.json

# --- Investigating specific tests ---
make investigate TEST=pp16
                       # Register entries + matching files for a test
make show-query TEST=syntax-select-expr-04.rq
                       # Print the .rq/.ru file for a test
make clean             # Remove generated report files
```

## Understanding the Output

### Test Summary

After running, the suite prints:

```
=== Test Summary ===
Total:   94
Passed:  79
Ignored: 0
Failed:  15
```

- **Total**: Number of W3C test cases discovered from manifest files
- **Passed**: Tests where Fluree's behavior matched the W3C expectation
- **Ignored**: Tests in the skip list (should be near zero)
- **Failed**: Tests where Fluree diverged from the spec — these are bugs or gaps

### Failure Messages

Each failure includes the test ID, type, and error details:

```
https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl#test_34:
  Positive syntax test failed — parser rejected valid query.
  Test: ...#test_34
  File: .../syntax-query/syntax-select-expr-04.rq
```

For syntax tests, failures fall into three categories:

| Failure Type            | What It Means                 | Example                                                 |
| ----------------------- | ----------------------------- | ------------------------------------------------------- |
| **Positive test fails** | Parser rejects valid SPARQL   | Missing feature (subqueries, property path `\|`)        |
| **Negative test fails** | Parser accepts invalid SPARQL | Missing validation (BIND scope, GROUP BY scope)         |
| **Parser timeout**      | Parser enters infinite loop   | Bug in grammar handling (mitigated by safety-net forward-progress check) |

### Test IDs

Every test has a unique IRI like:

```
https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl#test_34
```

The fragment (`#test_34`) identifies the specific test within that manifest. The path tells you the W3C category (`syntax-query`, `aggregates`, `bind`, etc.).

## Analyzing Results

### Per-Category Breakdown

Generate a per-suite JSON report, then summarize it:

```bash
make report-cat CAT=sparql10_query_eval_tests
make summary REPORT=report-sparql10_query_eval_tests.json
```

Output looks like:

```
Category                  Pass  Fail Total    Rate
----------------------------------------------------
syntax-query                80    14    94     85%
subquery                     8     6    14     57%
functions                   27    48    75     36%
...
----------------------------------------------------
TOTAL                      167   160   327   51.1%
```

### Error Classification

Use `make classify` to group a report's failures by root cause:

```bash
make classify REPORT=report-sparql10_query_eval_tests.json
```

Error types:
- **RESULT MISMATCH** — Query runs but returns wrong values
- **INTERNAL ERROR** — Execution fails with an internal error
- **PARSE/LOWERING** — SPARQL parsing or IR lowering fails
- **NEGATIVE SYNTAX** — Parser accepts a query it should reject
- **POSITIVE SYNTAX** — Parser rejects a query it should accept
- **EMPTY RESULTS** — Query returns no results when some were expected
- **NOT IMPLEMENTED** — Feature not yet implemented
- **PANIC** — Subprocess crashed (usually an index/unwrap bug)
- **TIMEOUT** — Test exceeded 5s (syntax) or 10s (eval) timeout

### Listing Failures

Use `make failures-cat` to list a report's failures with their type and first error line:

```bash
make failures-cat REPORT=report-sparql11_update_tests.json
```

### JSON Reports

For programmatic analysis, generate a JSON report:

```bash
make report-cat CAT=sparql11_bind   # → report-sparql11_bind.json
make report-10-json                 # → report-sparql10_query_eval_tests.json
```

Report format:

```json
{
  "total": 327, "passed": 167, "failed": 160, "pass_rate": "51.1%",
  "tests": [
    { "test_id": "http://...#agg01", "status": "pass", "error": null, "timeout": false },
    { "test_id": "http://...#agg02", "status": "fail", "error": "Results not isomorphic...", "timeout": false }
  ]
}
```

The analysis script at `scripts/analyze_report.py` can also be used directly:

```bash
python3 scripts/analyze_report.py summary report-<suite>.json
python3 scripts/analyze_report.py classify report-<suite>.json
python3 scripts/analyze_report.py failures report-<suite>.json --category functions
```

## From Failure to Fix: The Workflow

### Step 1: Identify the Failure Category

Run the suite and look at the failure message:

```bash
cargo test sparql11_syntax_query_tests -- --nocapture 2>&1 | tail -40
```

Determine which category:

- **Parser timeout** → Bug in `fluree-db-sparql` grammar rules causing infinite loop (mitigated by safety-net forward-progress check in `parse_group_graph_pattern()`, but can still occur in other parse entry points)
- **Positive syntax rejected** → Missing parser feature or incorrect grammar
- **Negative syntax accepted** → Missing semantic validation pass
- **Query evaluation mismatch** → Bug in query engine, data loading, or result formatting

### Step 2: Find the Test Query

Every W3C test references a `.rq` (query) or `.ru` (update) file. The failure message includes the file URL. Map it to a local path:

```
URL:   https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-04.rq
Local: testsuite-sparql/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-04.rq
```

The pattern: strip `https://w3c.github.io/rdf-tests/` and prepend `testsuite-sparql/rdf-tests/`.

Read the query to understand what SPARQL feature is being tested:

```bash
cat testsuite-sparql/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-04.rq
```

### Step 3: Reproduce in Isolation

Try parsing the query directly to see the exact error:

```rust
// Quick test in fluree-db-sparql
let output = fluree_db_sparql::parse_sparql("SELECT (1 + ?x AS ?y) WHERE { ?x ?p ?o }");
println!("has_errors: {}", output.has_errors());
for err in output.errors() {
    println!("  error: {err:?}");
}
```

If you suspect an infinite loop, the subprocess timeout will catch it automatically when run via the harness.

### Step 4: Investigate the Root Cause

For **parser issues**, the relevant code is in `fluree-db-sparql/`. Start with:

- `src/parser/` — Grammar rules and parser combinators
- `src/ast/` — AST types the parser emits

For **query evaluation issues**, the chain is:

1. `fluree-db-sparql` → parses to `SparqlAst`
2. `fluree-db-query` → evaluates the AST against a ledger
3. `fluree-db-api` → orchestrates ledger creation and query execution

### Step 5: Create an Issue

Use this template:

```markdown
## W3C SPARQL Compliance: [short description]

**Test ID:** `https://w3c.github.io/rdf-tests/sparql/sparql11/[category]/manifest.ttl#[test_name]`
**Category:** [syntax-query | aggregates | bind | etc.]
**Failure type:** [parser timeout | positive syntax rejected | negative syntax accepted | evaluation mismatch]

### Test Query

\`\`\`sparql
[paste the .rq file contents]
\`\`\`

### Expected Behavior

[For positive syntax: should parse successfully]
[For negative syntax: should be rejected]
[For evaluation: expected results from the .srx/.srj file]

### Actual Behavior

[Error message or incorrect output]

### Root Cause Analysis

[What part of the code needs to change and why]

### W3C Spec Reference

[Link to relevant section of https://www.w3.org/TR/sparql11-query/]
```

### Step 6: Fix and Verify

After making code changes:

```bash
# Verify the specific test passes (from testsuite-sparql/)
cargo test sparql11_syntax_query_tests -- --nocapture 2>&1 | grep "test_34"

# Verify you haven't regressed other tests
make test

# Run the parser's own tests (from workspace root)
cd .. && cargo test -p fluree-db-sparql

# Full CI parity check
cargo clippy -p fluree-db-sparql --all-features -- -D warnings
```

## Using Claude Code for Debugging

Claude Code is particularly effective for SPARQL compliance work because each failure is self-contained: a query file, an expected behavior, and a specific error. Here's how to give a session full context.

### Prompt Template for Parser Failures

```
I'm working on W3C SPARQL compliance in Fluree. The following test is failing:

Test ID: https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl#test_34
Category: Positive syntax test (parser should accept this query but rejects it)

The query file is at: testsuite-sparql/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-04.rq

The SPARQL parser is in fluree-db-sparql/. The parse entry point is
`parse_sparql()` which returns `ParseOutput<SparqlAst>` — check `has_errors()`.

Please:
1. Read the failing query file
2. Understand what SPARQL feature it tests
3. Find the relevant parser grammar in fluree-db-sparql/src/parser/
4. Identify why the parser rejects this input
5. Propose a fix
```

### Prompt Template for Query Evaluation Failures

```
I'm working on W3C SPARQL compliance. This query evaluation test is failing:

Test ID: https://w3c.github.io/rdf-tests/sparql/sparql11/aggregates/manifest.ttl#agg01
Test data: testsuite-sparql/rdf-tests/sparql/sparql11/aggregates/agg01.ttl
Query file: testsuite-sparql/rdf-tests/sparql/sparql11/aggregates/agg01.rq
Expected results: testsuite-sparql/rdf-tests/sparql/sparql11/aggregates/agg01.srx

The test harness creates an in-memory Fluree ledger, loads the data via
stage_owned().insert_turtle(), executes the query via query_sparql(), and
compares results.

Actual output: [paste actual output]
Expected output: [paste expected from .srx file]

Please investigate why the results differ and propose a fix.
```

### Key Files to Reference

When asking Claude Code for help, these files provide essential context:

| Context Needed                                  | File(s)                                           |
| ----------------------------------------------- | ------------------------------------------------- |
| Test harness architecture                       | `testsuite-sparql/src/lib.rs`, `src/evaluator.rs` |
| Subprocess timeout isolation                    | `testsuite-sparql/src/subprocess.rs`              |
| Subprocess worker binary                        | `testsuite-sparql/src/bin/run_w3c_test.rs`        |
| How manifests are parsed                        | `testsuite-sparql/src/manifest.rs`                |
| Syntax test handlers                            | `testsuite-sparql/src/sparql_handlers.rs`         |
| Eval test handler (data load + query + compare) | `testsuite-sparql/src/query_handler.rs`           |
| Expected result parsing (.srx/.srj)             | `testsuite-sparql/src/result_format.rs`           |
| Isomorphic result comparison                    | `testsuite-sparql/src/result_comparison.rs`       |
| SPARQL parser entry point                       | `fluree-db-sparql/src/lib.rs` (`parse_sparql()`)  |
| Parser grammar rules                            | `fluree-db-sparql/src/parser/`                    |
| SPARQL AST types                                | `fluree-db-sparql/src/ast/`                       |
| Query engine                                    | `fluree-db-query/src/`                            |
| API orchestration                               | `fluree-db-api/src/`                              |
| W3C SPARQL test categories                      | `testsuite-sparql/tests/w3c_sparql.rs`            |

### Batch Processing Tips

When multiple tests fail for the same root cause (e.g., "all BIND tests timeout"), group them:

```
These 3 tests all timeout in the parser on BIND expressions:
- test_34: SELECT (1 + ?x AS ?y)
- test_40: SELECT (CONCAT(?x, "!") AS ?label)
- test_65: subquery with SELECT expression

All are in testsuite-sparql/rdf-tests/sparql/sparql11/syntax-query/.

The parser code for BIND is in fluree-db-sparql/src/parser/. Please find the
common root cause and fix all three.
```

## Query Surface Parity (JSON-LD)

SPARQL, JSON-LD query, and Cypher (`fluree-db-cypher`) all compile to the **same intermediate representation** (`fluree-db-query/src/ir.rs`) and share the entire execution engine. Team guideline — every W3C compliance fix must classify itself as one of:

1. **IR/engine-level fix** (evaluation semantics, planner, operators, storage): the fix lands once and applies to all surfaces implicitly — the user-facing syntax of each surface is unchanged. Still add a JSON-LD regression test for the fixed behavior (see below): the W3C submodule guards the SPARQL surface, but nothing guards the same semantics through the JSON-LD surface unless we write it.

2. **Surface-syntax addition**: if we make something newly *possible in SPARQL* (new function, operator, clause, or syntax), it must also be made possible in JSON-LD query syntax as part of the same effort — with its own tests. Fluree **owns** the JSON-LD query syntax, so this is always within our power.

3. **SPARQL-only surface features**: property paths syntax, RDF-star/annotation syntax, ASK query form, and SPARQL UPDATE text forms have no JSON-LD equivalent. These don't require syntax parity — but if the underlying IR/engine capability is new (e.g., a new pattern type), consider whether JSON-LD should be able to express it and record the decision.

**Cypher is deliberately excluded from this guideline.** We do not own the openCypher grammar and do not introduce custom Cypher syntax, so SPARQL compliance work carries no Cypher syntax obligations. Cypher benefits from IR/engine-level fixes automatically; what openCypher exposes is governed separately by `docs/reference/cypher-support-matrix.md`.

**Regression-test rule:** a compliance fix is not done when the W3C test goes green — it is done when the register entry is removed AND an equivalent JSON-LD test exists for behavior that JSON-LD can express.

### Where to add parity tests

| Language | Test files |
| -------- | ---------- |
| SPARQL   | `fluree-db-api/tests/it_query_sparql.rs` |
| JSON-LD  | `fluree-db-api/tests/it_query.rs`, `it_query_analytical.rs`, `it_query_grouping.rs` |
| UPDATE / transact | `fluree-db-api/tests/it_transact_update.rs`, `it_named_graphs.rs` (SPARQL UPDATE and JSON-LD transactions share the `Txn` IR — fixes to either surface add the counterpart test here) |
| Shared   | Unit tests in `fluree-db-query/src/` modules |

### Validation after shared-code changes

```bash
# SPARQL W3C tests (from testsuite-sparql/)
make test-cat CAT=<suite fn name>

# JSON-LD query tests (from workspace root) — integration tests are grouped
# into grp_* binaries; run a whole group or filter to one file's module.
cargo test -p fluree-db-api --test grp_query
cargo test -p fluree-db-api --test grp_query_sparql
# e.g. just one file's tests: cargo test -p fluree-db-api --test grp_query it_query_jsonld
```

## Architecture Overview

### Crate Structure

```
testsuite-sparql/
├── Cargo.toml                      # Excluded from workspace, publish = false
├── Makefile                        # Developer convenience targets
├── scripts/
│   └── analyze_report.py           # JSON report analysis (summary, classify, failures)
├── src/
│   ├── lib.rs                      # check_testsuite() entry point
│   ├── vocab.rs                    # W3C namespace constants (mf:, qt:, etc.)
│   ├── files.rs                    # URL → local file path mapping
│   ├── manifest.rs                 # TestManifest: Iterator<Item=Test>
│   ├── evaluator.rs                # TestEvaluator: type → handler dispatch
│   ├── sparql_handlers.rs          # Handler registration (syntax + eval)
│   ├── query_handler.rs            # QueryEvaluationTest: load data, run query, compare
│   ├── subprocess.rs               # Subprocess isolation for timeout enforcement
│   ├── result_format.rs            # Parse .srx/.srj expected result files
│   ├── result_comparison.rs        # Isomorphic result comparison (blank node mapping)
│   ├── report.rs                   # JSON report generation
│   └── bin/
│       └── run_w3c_test.rs         # Subprocess worker binary
├── tests/
│   └── w3c_sparql.rs               # Test entry points (syntax + 12 eval categories)
└── rdf-tests/                      # Git submodule → github.com/w3c/rdf-tests
```

### How It Works

**1. Manifest Parsing** (`manifest.rs`): `TestManifest` implements `Iterator<Item = Result<Test>>`. It loads `manifest.ttl` files using Fluree's own Turtle parser, follows `mf:include` links recursively, and extracts per-test metadata: type, query file, data file, expected results.

**2. Handler Dispatch** (`evaluator.rs`): `TestEvaluator` maps test type URIs (e.g., `mf:PositiveSyntaxTest11`) to handler functions. For each test, it finds the matching handler and invokes it.

**3. SPARQL Handlers** (`sparql_handlers.rs` + `query_handler.rs`): The Fluree-specific logic. Syntax, query-evaluation, and update-evaluation tests all run in isolated **subprocesses** via the `run-w3c-test` binary (`subprocess.rs`). For syntax tests, the subprocess calls `parse_sparql()` + `validate()` and reports whether errors were found (5-second timeout). Note the surface split: syntax tests validate with the strict `Capabilities::default()`, while the production update seam (`fluree-db-api`'s `parse_and_lower_sparql_update`) runs the same `validate()` pass with `Capabilities::with_delete_where_extensions()` — identical rules except that Fluree's documented DELETE WHERE extensions (existential blank nodes; anonymous annotation tails) are admitted. The negative-update-syntax tests that reject those two shapes therefore guard the strict surface only; the production seam's behavior is pinned by `fluree-db-api` integration tests (`it_transact_update.rs`). For query evaluation tests, the subprocess creates an in-memory Fluree ledger, loads default-graph Turtle plus named graphs (each wrapped as a TriG `GRAPH` block through the transact builder), executes the SPARQL query, and compares results against expected `.srx`/`.srj`/`.ttl`/`.csv`/`.tsv` files using isomorphic matching (10-second timeout). For update evaluation tests (`mf:UpdateEvaluationTest`), the subprocess loads the initial graph-store state (`ut:data`/`ut:graphData`), applies the update through the public `sparql_update` transact surface, and compares the resulting default-graph and named-graph states against `mf:result`'s expected state, including a check for unexpected non-empty named graphs. If a test exceeds its timeout, the parent kills the child process — no zombie threads.

**4. Test Entry Points** (`tests/w3c_sparql.rs`): Each test function is ~5 lines — just a manifest URL and a skip list. The harness does the rest.

### Key Design Decisions

- **Subprocess isolation** for all test execution. Each syntax and eval test runs in a child process (`run-w3c-test` binary) that can be killed on timeout. This prevents zombie threads from parser infinite loops or runaway queries.
- **Syntax timeout: 5 seconds, eval timeout: 10 seconds.** If a test exceeds its limit, the subprocess is killed and the test is marked as a timeout failure.
- **Uses Fluree's own Turtle parser** for manifest files. If our parser can't handle well-formed W3C manifests, that's a bug worth knowing about.
- **Fluree's `list_index`** approach (instead of `rdf:first/rdf:rest`) simplifies manifest list handling.
- **`@base` prepended** to manifest files since they use `<>` (empty relative IRI) which requires a base.

## Test Categories

### Syntax Tests (Phase 1)

| Suite             | What It Tests                             | Manifest                    |
| ----------------- | ----------------------------------------- | --------------------------- |
| SPARQL 1.1 syntax | Parser correctness for SPARQL 1.1 grammar | `syntax-query/manifest.ttl` |
| SPARQL 1.0 syntax | Backward compatibility with SPARQL 1.0    | `manifest-syntax.ttl`       |

### Query Evaluation Tests (Phase 2)

Each test creates an in-memory Fluree ledger, loads RDF data, executes a SPARQL query, and compares results against W3C expected outputs. Run with `make test-cat CAT=<suite fn name>`.

| Suite              | What It Tests                                   | Manifest                          |
| ------------------ | ----------------------------------------------- | --------------------------------- |
| Aggregates         | COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE | `aggregates/manifest.ttl`         |
| BIND               | BIND expressions, variable assignment           | `bind/manifest.ttl`               |
| Bindings           | VALUES inline data                              | `bindings/manifest.ttl`           |
| Cast               | xsd:integer(), xsd:double(), xsd:string()       | `cast/manifest.ttl`               |
| Construct          | CONSTRUCT query form                            | `construct/manifest.ttl`          |
| Exists             | FILTER EXISTS, FILTER NOT EXISTS                | `exists/manifest.ttl`             |
| Functions          | String, numeric, date/time, hash, IRI functions | `functions/manifest.ttl`          |
| Grouping           | GROUP BY semantics, error handling              | `grouping/manifest.ttl`           |
| Negation           | MINUS, NOT EXISTS                               | `negation/manifest.ttl`           |
| Project-Expression | SELECT expressions, AS aliases                  | `project-expression/manifest.ttl` |
| Property-Path      | `/`, `\|`, `^`, `+`, `*`, `?` operators         | `property-path/manifest.ttl`      |
| Subquery           | Nested SELECT within WHERE                      | `subquery/manifest.ttl`           |

### BIND / VALUES Compliance Notes

**BIND** (10/10 — 100%):
- Fixed lexer to tokenize `+`/`-` as separate operators per the SPARQL spec (`INTEGER` is unsigned; `INTEGER_POSITIVE`/`INTEGER_NEGATIVE` are grammar-level). This fixed `?o+10` being mis-tokenized as `Var, Integer(10)` instead of `Var, Plus, Integer(10)`.
- BIND input variable liveness is handled by `precompute_suffix_vars` (cross-block) and `pending_binds.expr.variables()` (within-block) in the WHERE planner — no special handling needed in `compute_variable_deps`.
- Explicitly nested `{ }` blocks inside WHERE are lowered as anonymous subqueries (`SubqueryPattern`) to preserve SPARQL scope boundaries (bind10).

**VALUES / Bindings** (10/11 — 91%):
- Post-query VALUES (`WHERE { ... } VALUES ?x { ... }`) is now parsed and lowered. Added `values` field on `SelectQuery` AST and `post_values` field on `ParsedQuery` to prevent the planner from reordering it relative to OPTIONAL/UNION.
- `NestedLoopJoinOperator::combine_rows` fixed to handle `Unbound`/`Poisoned` left-side shared variables by falling back to right-side values. This fixes VALUES with UNDEF (values4, values5, values8).
- `ValuesOperator` updated to treat `Poisoned` (from failed OPTIONAL) as wildcard in `is_compatible` and `merge_rows`, fixing values7 (OPTIONAL + VALUES).
- Remaining failure: `graph` test requires named graph support (GRAPH keyword) — tracked separately.

## Managing the Skip List

Skip entries live in `tests/registers/mod.rs` — one `pub const` per suite,
passed as the `ignored_tests` parameter of that suite's `check_testsuite()`
call. The register is enforced in **both directions**: an unregistered
failure fails the suite, and a registered test that now *passes* also fails
the suite (stale entry — remove it in the same change that fixes the
feature). Entries look like:

```rust
check_testsuite(
    "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl",
    &[
        // Deliberately accept bare `1` as integer literal (RDF 1.1 vs 1.0)
        // Spec: https://www.w3.org/TR/sparql11-query/#rNumericLiteral
        // Reviewed: 2025-02-15 by @ajohnson, @bsmith
        "https://...#test_99",
    ],
)
```

**Rules:**

1. Start with an **empty** skip list. Expect full compliance.
2. Only add entries after investigation confirms a _deliberate_ design choice, not a bug.
3. Every skip entry must have a comment explaining why, linking to the relevant spec section.
4. Skip entries require review by 2+ team members.
5. The total skip list should be <5% of tests (Oxigraph skips ~25 out of 700+).
6. Review skip entries periodically — remove them as features are added.

## Updating the rdf-tests Submodule

The W3C test data lives in a git submodule at `testsuite-sparql/rdf-tests/`. To update to the latest W3C tests:

```bash
cd testsuite-sparql/rdf-tests
git pull origin main
cd ../..
git add testsuite-sparql/rdf-tests
git commit -m "chore: update W3C rdf-tests submodule"
```

After updating, run the full suite to check for new tests or changed expectations:

```bash
cd testsuite-sparql
cargo test
```

## Related Documentation

- [Tests guide](tests.md) — General testing practices
- [SPARQL query docs](../query/sparql.md) — User-facing SPARQL feature documentation
- [Compatibility](../reference/compatibility.md) — Standards compliance status
- [Crate map](../reference/crate-map.md) — Workspace architecture
