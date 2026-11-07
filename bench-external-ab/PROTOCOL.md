# PROTOCOL — external-engine A/B (binding rules)

> **Notice**: This benchmark harness invokes an independently installed DuckDB CLI binary as an external process, solely for comparative performance measurement. No DuckDB source code, binaries, extensions, or platform components are included in, linked into, distributed with, or incorporated into this repository or any Fluree product. DuckDB® is a trademark of its owner; references here are nominative — identifying the third-party product being measured — and imply no affiliation or endorsement. Versions, configuration, and the full protocol are disclosed for reproducibility.

This is the condensed, binding rule-set for the external-engine A/B program. It supersedes any looser wording elsewhere. The harness is engine-agnostic; external engines are adapters in `harness/engines/`. The first external engine measured is DuckDB. Formatting: one paragraph per line for clean diffs.

## 0. Reference point under test

Fluree's virtual (R2RML-over-Iceberg) query path is measured against the external engine per comparable query on the same physical files; the reference point is that Fluree should be at least on par. Any shortfall beyond measurement noise is recorded as a NAMED ENGINE GAP with a query shape attached — a factual measurement, not a verdict — and where Fluree lags it is reported with equal plainness. Graph-native shapes the SQL engine cannot express (`?s ?p ?o` crawls, property paths) stay Fluree-only regression members (a capability difference, not an A/B score).

## 1. Substrate (same physical files)

Fluree's virtual path reads ONLY catalog-fronted Iceberg, never raw Parquet, so the shared A/B substrate must be a catalog-fronted Iceberg table both engines open. Two substrates, split by role:

- SUBSTRATE A = the deployment-real Snowflake-managed Iceberg (Polaris REST + OAuth2). Carries the catalog/planning layer, bare COUNT (manifest stats), and small single-file dim reads. DuckDB currently CANNOT reliably scan the partitioned multi-file FACT tables here (issue #1568, Finding 1 — a Snowflake-managed-S3 connection interaction, NOT a network or vended-cred failure; Fluree reads them fine). Substrate-A configs are committed as `<placeholder>` TEMPLATES only.
- SUBSTRATE B = a local, engine-pure Iceberg fixture: MinIO (S3) + `apache/iceberg-rest-fixture` + pyiceberg-written tables (`substrate/`). Own S3 creds + `ACCESS_DELEGATION_MODE none` sidestep the vended-cred path; UNPARTITIONED single-file tables sidestep the many-small-files fan-out. Substrate B carries the DATA-scanning wave (single-table AND joins) for BOTH engines and is where the headline A/B numbers come from.

DuckDB-parquet-direct over the same source files is an engine FLOOR reference only (no Fluree counterpart) — used to supply a DuckDB answer for a shape substrate B can't run, always labeled asymmetric via the per-row `substrate` field.

## 2. Timing — memo-COLD / data-WARM, fresh process per rep (PRIMARY)

The primary reported mode is memo-COLD / data-WARM: a FRESH engine process per timed repetition for BOTH engines (OS page cache + on-disk caches warm; NO engine-internal result memo can serve a timed run). This is mandatory because Fluree's query memo has produced ~350x warm-cache optimism on a reused handle — a reused-handle "warm" run can serve a memoized hashmap lookup while DuckDB re-executes, measuring caching, not the engine. The house benchmark methodology (fluree/benchmark-db) names the same principle: "no warm result cache; each engine's result cache is disabled or cleared per query." Reused-handle "hot" numbers may be collected as a non-headline secondary curiosity; they NEVER headline and NEVER gate.

Fluree cold (`--cold` / cache-cleared per rep) pays the full cold catalog/metadata fetch each rep, symmetric with DuckDB re-attaching in every fresh process. DuckDB's comparable cold number is `wall_ms + setup_ms` (query + ATTACH/OAuth), because Fluree's wall includes its own cold catalog cost; the harness records both so the boundary is chosen downstream without a re-run.

Report N>=5 measured reps: median AND p95 (catalog-latency variance hides under median alone). rep1 (first cold-catalog touch) is reported separately from the steady-state median of reps 2..N.

## 3. Memory — disclosed footprints, not equal caps

Equal numeric memory caps are NOT parity: DuckDB `memory_limit` is an allocator-tracked hard ceiling that forces spill; Fluree's budget is a per-query planning ESTIMATE that does not bound real RSS. Setting them equal makes DuckDB spill-at-cap while Fluree over-runs the same number silently — a systematically Fluree-favorable comparison. Protocol: MEASURE and report peak RSS per run for both engines (`/usr/bin/time -l`, "maximum resident set size", bytes on macOS); set DuckDB `memory_limit` (and Fluree's budget) generously enough that NEITHER engine is starved. Fairness claim = "neither engine memory-starved, real footprints disclosed", not "caps equal." A deliberately spill-forcing memory-pressure comparison is a separate, explicitly-annotated wave (pin the cap LOW and document what each engine does when it cannot fit).

## 4. Timing boundary — full result consumption

The timed region for BOTH engines runs from query submission to FULL consumption of all result rows. DuckDB: `.mode csv` + `.output <sink>` so every row is produced AND serialized inside the timed statement (no duckbox display truncation, no lazy Arrow handle); rows counted from the sink. Fluree: complete SPARQL-results-JSON serialization (or the CLI's full-result tally). Recorded as `timing_boundary="query_exec_serialize"` in every row. This matters because Fluree's headline cost IS per-row RDF-binding materialization; timing Fluree to fully-built bindings while DuckDB streams lazily would be undefined and asymmetric. Both `wall_ms` values are ENGINE SELF-TIMERS — Fluree = the CLI's `(N rows, X ms)` tally through full JSON serialization; DuckDB = the `.timer` `real` line — so every row ALSO records the harness-measured `extra.proc_wall_ms` (whole-process wall) as an independent cross-check of the self-reported tally; where the two diverge materially, `proc_wall_ms` is the honest upper bound.

## 5. Substrate field discipline

Every emitted result row carries a `substrate` field (`parquet_local` | `iceberg_rest` | `iceberg_rest_nopushdown` | `iceberg_rest_minio` | `iceberg_rest_minio_partitioned` | `fluree_minio_rest` | ...) and an `engine_version` provenance string (DuckDB CLI version; Fluree CLI version + the binary's worktree git commit, so a fusion-lacking build can never be silently mistaken for a fixed one). No number silently mixes substrates: a DuckDB-parquet-floor answer standing in for a shape substrate B can't run is labeled `parquet_local` and called out as asymmetric in the results table.

## 6. Correctness FIRST

For each paired query, verify DuckDB and Fluree return the SAME result BEFORE any timing counts — row count always; canonical multiset value-compare for deterministic queries. A pair whose results diverge is a correctness bug to file, not a timing data point. A pair TIMES only after it MATCHES.

## 7. Equivalence rules (what makes SQL == SPARQL comparable)

The R2RML mapping is a clean 1:1 star->RDF: one table = one TriplesMap, subjects keyed on the surrogate `*_KEY`, each column -> exactly one single-valued predicate with an explicit xsd datatype, FK edges via `rr:parentTriplesMap`. Consequences, applied per pair in `harness/pairs/manifest.json`:

- (i) BAG vs SET: SPARQL SELECT is bag semantics like SQL; the single-valued mapping means row multiplicities match, so no DISTINCT is silently needed — apply DISTINCT on both sides only where the SPARQL query does.
- (ii) NULL vs UNBOUND: a SQL NULL omits the triple; `COUNT(?col)` counts bound values = SQL `COUNT(col)` (NULL-skipping); a triple pattern on a predicate = SQL `WHERE col IS NOT NULL`; OPTIONAL = LEFT JOIN; FILTER NOT EXISTS = anti-join (use SQL `NOT EXISTS`, mind `NOT IN` NULL semantics).
- (iii) DECIMAL/FLOAT: xsd:double SUM/AVG are non-deterministic across engines' summation order — compare double aggregates with a NUMERIC TOLERANCE (`sum_is_double: true` in the manifest), never exact hash. Integer/COUNT aggregates are hash/exact-compared. Raw double/decimal CELLS are compared numerically (not by lexical form) so DuckDB's double->string need not match Fluree's xsd:double lexical form.
- (iv) ORDER: unordered LIMIT compares ROW COUNT only; ordered queries compare content. Top-k pairs carry a UNIQUE tiebreaker in the sort key so the selected rows are exact across engines.
- (v) TERM SHAPE: a DuckDB row is normalized into the same canonical cell form Fluree uses (templated-IRI subject reconstructed from the surrogate key; typed literal to its lexical+datatype) so a DuckDB row and a Fluree binding compare identically.

## 8. DNF caps

Every timed run is hard-capped (`--timeout-s`, default 180 s). On overrun the whole process group is SIGKILLed (so a timed-out remote scan can't keep draining S3 in the background) and the run is recorded `ok=false` with `extra.dnf=true` and the reason. DNF rows never contribute a median; they are reported as DNF.

## 9. Machine discipline

Timed runs happen ONLY when 1-minute load average < 15 (poll before each timed set; stamp the load context on every timed set). One heavy phase at a time. Check `df` free space before each phase (>=30 GB general; >=60 GB before a scale-up data generation). Never `--max-performance`. Every long step is driven by in-turn polling of the running process — never end a turn waiting on an unhooked process.

## 10. Scope

Append-only SF01 (and larger locally-generated SF). MoR / equality-delete tables are OUT (DuckDB 1.5.2 crashed on equality deletes; Fluree's side never applies delete files — a silent-stale-rows risk). A MoR comparison is a separate, deliberately-fixtured wave. DuckDB is pinned >= 1.5.3 (older versions over-scan Iceberg; the runs here use v1.5.5). Every DuckDB invocation is prefixed `arch -arm64` on Apple Silicon (Rosetta penalty otherwise — dropping it invalidates the numbers).
