# Browse-parity implementer-1 gates (Clusters A + C1 + D + E2)

Branch: `perf/browse-parity` (base `perf/audit-harness` @ 733cb2c8e)
Code commits (dependency order — D before C1, since C1 uses D's typed error):
- A  634324005 — un-fork the crawl object formatter (D1/D2/D3)
- D  18e82659b — typed refusal envelope (err:r2rml/UnsupportedPattern)
- C1 959d9838d — loud-refuse a dropped VALUES clause on crawls
- E2 9ad472b07 — short-circuit a crawl over a class with no TriplesMaps

Recorded: 2026-07-20. All gates run LOCALLY (CI does not fire on a non-main base).

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
