# Browse-parity implementer-1 gates (Clusters A + C1 + D + E2)

Branch: `perf/browse-parity` (base `perf/audit-harness` @ 733cb2c8e)
Code commits (dependency order — D before C1, since C1 uses D's typed error):
- A  634324005 — un-fork the crawl object formatter (D1/D2/D3)
- D  18e82659b — typed refusal envelope (err:r2rml/UnsupportedPattern)
- C1 959d9838d — loud-refuse a dropped VALUES clause on crawls
- E2 9ad472b07 — short-circuit a crawl over a class with no TriplesMaps
- cfg — un-orphan build()'s #[cfg(native)] from with_secret_resolver (DEC-002
  scope addition, lead-verified; separate commit)

Recorded: 2026-07-20. All gates run LOCALLY (CI does not fire on a non-main base).

## no-native featureset gate (DEC-002 permanent addition)
`cargo check -p fluree-db-api --no-default-features --features aws,iceberg,shacl`
(solo's real combination) — FAILED at the base rev (storage_path / FileStorage /
FileNameService missing: build() had been left cfg-less by the #1505 SecretRef
insertion, and with_secret_resolver was over-gated behind native+iceberg) → PASSES
after the cfg fix. Native default + iceberg still builds. --all-features and
workspace builds structurally MASK this class of bug via feature unification, so
this exact featureset check is the gate.

## cargo fmt --check (my files) — CLEAN
`rustfmt --edition 2021 --check <9 changed .rs>` → exit 0.
Files: fluree-db-api/src/{format/mod.rs, graph_source/crawl.rs, view/stream_query.rs};
fluree-db-query/src/{error.rs, graph.rs, r2rml/mod.rs, r2rml/rewrite.rs};
fluree-db-server/src/error.rs; fluree-vocab/src/errors.rs.

## clippy (--all-targets --no-deps; CI main has no -D warnings) — CLEAN for my files
- `cargo clippy -p fluree-db-query --all-targets --no-deps` → no lints in my files, no errors.
- `cargo clippy -p fluree-db-api --all-targets --no-deps --features iceberg` → no lints in my files, no errors.
  2 PRE-EXISTING lib warnings, both in files I did NOT change:
  * graph_source/r2rml.rs:249 (`question_mark` — known clippy-1.97 env drift)
  * ledger_info.rs:1626 (very complex type)

## tests — 0 failed across the board
- `cargo test -p fluree-db-query` → 1312 (+2/16/9/2/13/9) passed, 0 failed.
- `cargo test -p fluree-db-api --features iceberg --lib` → 814 passed, 0 failed, 1 ignored.
- `cargo test -p fluree-db-api --features iceberg --test grp_graphsource` → 122 passed, 0 failed.
- `cargo test -p fluree-db-api --features iceberg --test grp_query` → 378 passed, 0 failed, 2 ignored.
- `cargo test -p fluree-db-api --features iceberg --test grp_query_sparql` → 288 passed, 0 failed.
- `cargo test -p fluree-db-api --features iceberg --test grp_misc` → 245 passed, 0 failed, 5 ignored.
- `cargo test -p fluree-db-server --lib error::` → 5 passed, 0 failed.
- `cargo test -p fluree-vocab --lib errors` → 3 passed, 0 failed.

Feature note: the `iceberg` feature is REQUIRED to compile the crawl e2e test
module (it uses `fluree_db_iceberg::io::batch`); a featureless
`cargo test -p fluree-db-api` does not exercise the new crawl tests. Live-infra
integration tests skip cleanly on unset `ICEBERG_E2E` — NO live queries run.

## New tests added
- crawl (e2e): ref-shape parsed asserts, typed-json variant, boolean-shape,
  /info⇔crawl @id invariant (Cluster A); VALUES loud-refuse + flat-select-VALUES
  regression guard (C1); unmapped-class empty-with-zero-scans + mapped-class
  control (E2).
- server (error): r2rml_unsupported_pattern_is_400_with_distinct_type (D).

## Machine-readable code token (Cluster D) — solo gates on this
- fluree-vocab constant: `R2RML_UNSUPPORTED_PATTERN`
- HTTP 400 body `@type` value (the stable dispatch key): `err:r2rml/UnsupportedPattern`
- streaming ndjson `error` code: `r2rml_unsupported_pattern`
- Display keeps the substring "cannot be converted to R2RML scans" for prose-match migration.

# Browse-parity wave-2 gates (Cluster B + E1 + the browse-shape corpus family)

Branch: `perf/browse-parity` (base `perf/audit-harness`). Code commits on top of
the wave-1 tip:
- B   016186b7d — route the constant-IRI select-map onto the bound-subject crawl (D4)
- E1  be859fe97 — forward the LIMIT budget through the property-var crawl (D7)
- Bp  ff2332d7d — omit @id from an explicit constant-IRI projection (Cluster B native parity)
- corpus + harness — the browse-shape corpus family (q079-q085) + the bench JSON-LD path
- (this gate record)

Recorded: 2026-07-20. All gates run LOCALLY (CI does not fire on a non-main base).

## Cluster B (D4) — constant-IRI select-map routing
`{"select": {"<iri>": [...]}}` (the `["*"]`, `["@type"]`, and forward-predicate-list
forms) lowered to Root::Sid → native binary-index hydration → the empty `{"@id":…}`
stub for an EXISTING virtual subject. Now `detect_wildcard_crawl` recognizes a
constant-IRI root (behind `FLUREE_R2RML_SELECT_MAP_ROUTING`, default on) and
`expand_bound_subject_select_map` runs ONE bound-subject wildcard scan (pruned to
the subject's table via subject-template reversal), applying the projection at
assembly. Absent/unreversible subject → the native-parity `[{"@id":…}]` stub.
**Native parity nuance (caught by blessing q081/q082):** native OMITS `@id` from an
explicit projection that doesn't request it (`["@type"]` → `{"@type":…}`, no `@id`);
only the wildcard/id-only forms (and an explicit `"@id"`) carry it — tracked via
`CrawlProjection::Predicates.want_id`.

## E1 (D7) — property-var crawl budget forwarding
The property-scoped browse crawl DNF'd: its selective const-predicate scan and its
variable-predicate FULL-SOURCE wildcard both estimated equal
(`DEFAULT_PROPERTY_SCAN_SELECTIVITY`), so reorder left the wildcard as the
unbudgeted inner full-source scan. `estimate_pattern` now costs a pruning-key-less
variable-predicate R2RML scan as `FULL_SCAN` (behind `FLUREE_R2RML_BUDGET_PROPERTY_VAR`,
default on), so reorder places it LAST — the LIMIT-budgeted correlated OUTER driven
by the selective scan. Sound: reordering two co-subject scans preserves the
solution set; every driving subject has ≥1 triple so the budget never under-fills.

## Bench harness — JSON-LD select-map execution (new)
`fluree-bench-virtual/src/exec.rs` now detects a JSON-LD body (first non-comment
line begins with `{`) and runs it through `.jsonld()` (crawl expansion on virtual /
native hydration on native) instead of `.sparql()`. The node-document array result
is canonicalized by the existing `canon::canonicalize` bare-array branch
(key-sorted, multiset — so key/node order is parity-safe). Members carry a `#`
comment header stripped before parse. This is what lets the browse select-map
shapes be corpus members at all.

## cargo fmt --check (my files) — CLEAN
`rustfmt --edition 2021 --check` (exit 0) on:
fluree-db-api/src/graph_source/crawl.rs; fluree-db-query/src/{planner.rs,
r2rml/mod.rs}; fluree-bench-virtual/src/{exec.rs, corpus.rs}.

## clippy (--all-targets --no-deps) — CLEAN for my files
- `cargo clippy -p fluree-db-query --all-targets --no-deps` — 0 lints in my files.
- `cargo clippy -p fluree-db-api --all-targets --no-deps --features iceberg` — 0 lints in my files.
  PRE-EXISTING warnings only, all in files I did NOT change: fused_aggregate.rs:112-116
  (5 doc-list, from F1), ledger_info.rs:1626 (complex type), r2rml.rs:249 (question_mark,
  clippy-1.97 env drift).

## tests — 0 failed (modulo one pre-existing flake)
- `cargo test -p fluree-db-query` → 1315 lib (+2/16/9/2/13/9 integration) passed, 0 failed.
  (Includes the new `r2rml_full_source_wildcard_reorders_last_for_budget` planner test.)
- `cargo test -p fluree-db-api --features iceberg --lib` → 820 passed, 0 failed, 1 ignored.
  (Includes 6 new Cluster-B crawl e2e tests + the switch-seam detect tests.)
- grp_graphsource → 122, grp_query → 378 (2 ign), grp_query_sparql → 288, grp_misc → 245 (5 ign).
  **grp_misc FLAKE:** `it_minmax_fast_path_fired::multilang_min_served_by_fast_path`
  intermittently fails under full-suite parallelism (passed in isolation and on rerun;
  a native MIN fast-path test untouched by these R2RML-only changes — pre-existing
  test-order nondeterminism, not a regression).
- `cargo test -p fluree-bench-virtual --bins` → 35 passed, 0 failed (corpus now 85; the
  browse tag, count, smoke-cover, and rows_only meta-tests updated).

## no-native featureset gate (DEC-002 permanent addition) — PASSES
`cargo check -p fluree-db-api --no-default-features --features aws,iceberg,shacl` → Finished, 0 errors.

## Corpus family (q079-q085) + native bless
7 members promoted from the P3 shape matrix (JSON-LD select-maps except q085 SPARQL):
q079 class-page crawl (Cluster A, rows_only), q080 subject-detail `["*"]` + q081
`["@type"]` + q082 property-list (Cluster B, Full), q083 property-scoped listing (E1,
rows_only), q084 filtered page (rows_only), q085 inbound-edge count (works-today shape 9,
Full). Native oracles blessed OFFLINE from native-sf01 (`baseline --expected`, 7 written).
q038's native leg reproduces UNCHANGED (hash `0af26fd7…`, sanity confirmed). q080's native
node doc shows FK refs as `{"@id":…}`, `isCurrent: true`, dates as `{"@value","@type"}` —
the shape Cluster B must reproduce on virtual (verified in the live subset).
