# fluree-bench-virtual (`vbench`)

A corpus benchmark runner that compares SPARQL execution between a **native
materialized ledger** and one or more **R2RML/Iceberg virtual graph sources**.
For each query × target it records per-query wall time, virtual-pathway span
counters (scan/plan/parquet/catalog/OAuth), and a result-correctness hash.

It is built to run for months across performance PRs, so the record schema
(`src/schema.rs`, `SCHEMA_VERSION`) favors stability over cleverness. Runs are
written as newline-delimited JSON (`run.jsonl`) that `vbench report` (and future
dashboards / regression gates) read back.

## Build

```sh
cargo build -p fluree-bench-virtual --release
# binary: <target>/release/vbench   (use --release for realistic wall times)
```

The crate depends on `fluree-db-api` with the `iceberg` feature, so a build pulls
in the R2RML/Iceberg read path and the AWS SDK the Snowflake REST catalog needs.

## Subcommands

```sh
vbench setup --verify [--targets native-sf01,virtual-sf01]
vbench run   --targets native-sf01[,virtual-sf01] [--subset smoke] [--queries q001,q002]
             [--out FILE] [--keep-heads] [--timeout-s S] [--cache-state hot|cold]
             [--survey] [--max-queries N] [--max-wall-budget-s S]
vbench exec-one --query q001 --target virtual-sf01 [--keep-heads] [--cold] [--timeout-s S]
vbench report   --run FILE [--json]
vbench baseline --expected [--force] [--run FILE | --targets native-sf01] --perf --run FILE
                [--baseline-dir DIR]
vbench compare  --run FILE [--baseline-dir DIR] [--gate]
vbench dashboard --run native.jsonl --run virtual.jsonl [--out FILE] [--title T]
```

Global `--corpus-dir` / `--targets-dir` override the defaults (`<crate>/corpus`,
`<crate>/targets`); global `--cache-dir` pins the binary-index / Iceberg on-disk
artifact cache to a known directory.

- **setup --verify** — opens each target and runs a trivial probe (`COUNT` of a
  small class). For a native target with `expected_total_triples`, it also
  asserts the total triple count (schema stability of the prepared home).
- **run** — for each query × target: one discarded **priming** rep, then N
  **measured** reps (default native 5, virtual 3). If the first measured rep
  exceeds 60 s, the run collapses to a single rep (adaptive). Reports the
  **median-wall** rep; the reported wall/rows/hash/counters all come from that
  one rep so they are internally consistent. Records stream to `run.jsonl` and
  are flushed after each line, so a crash keeps partial results.
  - `--queries q010,q011` — run only these ids, in corpus order (overrides
    `--subset`); the in-process resume hook after a crashed sweep.
  - `--cache-state cold` — the **cold protocol**: run each query in a fresh
    `exec-one --cold` subprocess (empty catalog TTL cache, OAuth token, footer
    LRU, leaflet cache) whose **child** clears the home-scoped disk artifact
    cache first, so it pays the full cold cost. No priming; 2 s pacing between
    children. The parent's `--timeout-s` / `--cache-dir` overrides are
    forwarded to each child.
  - `--survey` — mark the run informational: it is **never a gate** (`baseline`
    refuses it, `compare` skips it). For the live SF20 stress subset.
  - `--max-queries N` / `--max-wall-budget-s S` — caps that bound live-Snowflake
    cost: stop after N queries, or once a target's cumulative wall passes S.
- **exec-one** — a single execution (no priming) printed as one `RunRecord` JSON.
  It is also the unit the cold protocol spawns per query. `--cold` clears the
  target's home-scoped disk cache before executing (records `cache_state=cold`).
- **report** — native-vs-virtual comparison table. `--json` emits structured rows.
- **baseline** — bless the reference:
  - `--expected` writes the per-query **native** correctness oracle to
    `baselines/expected/<qid>.json` (`result_hash`, `rows`, first-20 `head_rows`,
    provenance). Queries the manifest expects to error on virtual get
    **no** expected file — there is nothing to compare a correctly-erroring
    virtual result against. With no `--run`, a fresh native run is executed.
    **Re-blessing is guarded:** an existing oracle whose hash/rows differ from
    the new run **refuses** (printing the per-query delta, writing nothing)
    unless `--force` is given — so a native regression that still exits ok
    can't be silently blessed as the new truth. Oracles the run reproduces
    exactly are left untouched (diff-free); `rows_only`-gated queries compare
    row count, since their hash legitimately varies between runs.
  - `--perf` writes the per-target perf reference to `baselines/perf/<target>.json`
    (`hot_wall_ms_median`, optional `cold_wall_ms`, pathway counters, provenance),
    merging so a hot run and a later cold run both populate an entry.
- **compare** — check a run against the blessed baselines: an **expected-hash**
  check (a virtual result must match the native oracle) plus a **perf ratio** vs
  the perf baseline, judged against `budgets.json`. `--gate` exits nonzero on any
  parity or perf violation. A perf violation is **auto-rerun once** in-process
  before it's declared red (live-noise discipline); cold records are advisory
  (never gate); survey runs are skipped. Parity honors each query's
  `hash_gate`: `full` (default) requires an exact result-hash match; `rows_only`
  (a nondeterministic-selection `LIMIT`, where any *k* rows are a valid answer)
  gates on row count instead, since two engines can return different-but-equally-
  correct rows. The dashboard's hash column applies the same rule (`✓ rows`).
- **dashboard** — render one or more runs into a self-contained HTML dashboard
  (summary tiles + a per-query native-vs-virtual table with ratios, status pills,
  hash-match, pathway-span counters). Publish the file as an Artifact to share.

### Auth for virtual targets

`virtual-sf01` authenticates to the Snowflake Polaris catalog with the
`VBENCH_PAT` env var (a read-only `ICEBERG_READER` PAT). Export it before running
a virtual target, e.g. `export VBENCH_PAT="$(cat ~/.vbench/snowflake-pat.txt)"`.
The cold subprocesses inherit it. `virtual-sf20` (the `horizon-demo` home)
carries its own stored auth.

### Baselines, budgets & gating

`baselines/` (checked in — the reference) holds `expected/<qid>.json` +
`perf/<target>.json`; `budgets.json` (crate root) sets the perf gate:
`{ default_budget_pct: { native, virtual_hot }, min_delta_ms, cold: advisory,
overrides: {} }`. A query **violates** only when its observed hot wall exceeds
its blessed baseline by more than the class percent (per-query `overrides`
win) **and** by at least `min_delta_ms` absolute — the floor keeps a percent
of a tiny baseline (10% of 8 ms = 0.8 ms) from gating on scheduler noise, at
the documented cost of not flagging a regression that stays below the floor.
A typical loop: `run` native → `baseline --expected --perf` → later `run`
(native or virtual) → `compare --gate` to catch a correctness break or a perf
regression. Cold runs, survey runs, and full-vs-smoke stay distinguishable in
the record (`cache_state`, meta `survey`, meta `subset`) so a `compare`
matches like with like (`compare` picks the hot or cold baseline wall by the
record's `cache_state`).

**Perf baselines are single-machine, single-day medians** (the committed ones
came from the audit host). Correctness oracles (`expected/`) are
hardware-independent and portable; the perf medians are not. Before gating on
perf from a different machine — or after enough environmental drift that
ratios move on an unchanged binary — re-bless locally the same day:
`vbench run --targets … --out fresh.jsonl` on the **base** commit, then
`vbench baseline --perf --run fresh.jsonl`, and only then `compare --gate`
your change against those local medians.

### Cold-cache layout (verified)

The engine's default disk artifact cache is **machine-global**
(`$TMPDIR/fluree_binary_cache`), *not* under the target home. So the cold
protocol pins a **home-scoped** cache dir, `<home>/.vbench-iceberg-cache` (a
sibling of `storage/`, never `storage/` itself), and clears it before each cold
exec. `clear_cold_cache` refuses any path whose final component isn't a vbench
cache name — a guard so the cold protocol can never delete `storage/` or ledger
data. Verified on `/Users/ajohnson/vbench/.fluree`: cold clearing forces the
`iceberg.parquet_read` / `r2rml.load_table` spans to fire (they are cache-skipped
when warm).

## Targets (`targets/*.json`)

```json
{ "id": "native-sf01", "kind": "native",
  "fluree_home": "/Users/ajohnson/vbench/.fluree",
  "alias": "enterprise-sf01",
  "expected_total_triples": 35238778 }
```

`fluree_home` is a `.fluree` home directory; the on-disk store is
`<fluree_home>/storage`. `kind` drives whether the query builder attaches
`.with_r2rml()` (virtual only). A target may carry `"status": "pending"` to mark
it non-runnable; `run` / `exec-one` / `setup` refuse a pending target.

Shipped targets:

| id | kind | home | notes |
|---|---|---|---|
| `native-sf01` | native | `~/vbench/.fluree` | 35,238,778-triple baseline; source of the blessed oracles |
| `virtual-sf01` | virtual | `~/vbench/.fluree` | scale-matched counterpart (Snowflake `ENTERPRISE_DEMO.DW_SF01`, loaded 2026-07-10, exact generator row counts); full hot + cold-subset perf baselines committed |
| `virtual-sf20` | virtual | `~/horizon-demo/.fluree` | live Snowflake SF20, survey-only — **expensive / rate-limited** |

## Corpus (`corpus/`)

`corpus/manifest.json` catalogs each query; `corpus/queries/*.rq` holds the SPARQL
with a lineage header (BI question + design reference). The shipped corpus is
the full design set: **26 BI questions → 54 queries (`q001`–`q054`)** spanning
dimension and fact tables, with a 16-query `smoke` subset that covers every
feature tag. Each entry carries: an `id`, `file`, `bi_question`, `tags` (from
the closed 20-variant enum in `src/corpus.rs` — `bgp_star`, `join`, `fk_chain`,
`filter_range`, `filter_string`, `filter_date`, `filter_iri`, `optional`,
`union`, `aggregate`, `count`, `group_by`, `having`, `order_by`, `distinct`,
`subquery`, `values`, `negation`, `property_path`, `construct`), source
`tables`, a `class`, `expected_rows` (exact or `[min,max]`),
`order_sensitive`, `timeout_s`, `subsets`, an optional per-target-kind
`expected_status`, and `hash_gate` (`full` default; `rows_only` for the nine
nondeterministic-selection LIMIT queries per the corpus determinism policy).

`Corpus::load` validates the manifest before any run: unique ids, every `.rq`
file present and non-empty, tags within the enum, and the `smoke` subset covering
every tag that appears anywhere in the corpus.

## Result hashing

Both engines render results as SPARQL-results JSON, and `src/canon.rs` reduces a
result to an **order-independent multiset hash**: rows are canonicalized
cell-by-cell (IRIs verbatim; integers reparsed; decimals shortest-round-trip;
floats quantized to 12 significant digits; language tags case-folded and kept,
so a lang divergence fails the gate), then the row-set is sorted and
SHA-256'd. Two engines that emit the same bindings in a different order hash
equal. A document that is neither a JSON-LD graph, an ASK boolean, nor a
well-formed `results.bindings` table is an **error**, not a 0-row success.
Known blind spot: decimal canonicalization goes through `f64`, so decimals
differing only past ~15 significant digits false-equate (no corpus query
projects such a value; see `src/canon.rs` for why the exact-decimal fix is
deferred).

> **Scale matters for hash comparison.** `virtual-sf20` holds ~20× the data of
> `native-sf01`, so their hashes will **not** match — that is expected. Hash
> comparison is only meaningful against a scale-matched target
> (`virtual-sf01`, once loaded).

## Pathway span counters

`src/spans.rs` pins the virtual-pathway span allowlist. Each name was verified
against a `debug_span!`/`.instrument()` callsite (cited in the module doc):

| span | what it times |
|---|---|
| `r2rml.scan_table` | whole scan setup (loadTable + planning) |
| `r2rml.load_table` | cold REST/OAuth catalog round-trip |
| `iceberg.scan_plan` | manifest read + file pruning (records `files_selected`/`files_pruned`/`estimated_row_count`) |
| `iceberg.parquet_read` | per-file Parquet decode (records `file_size`; runs in spawned tasks) |
| `iceberg.oauth_token` | OAuth token mint |

`spans_missing` flags the must-fire span (`r2rml.scan_table`) when it doesn't
fire — the signal that a "virtual" query didn't actually hit the R2RML engine,
or tracing was mis-installed. The other four are conditional and deliberately
not in the expected-always set: `scan_plan` fires only on the planner's
pruning/pushdown branch (finding F7 — treating it as must-fire false-flagged
most virtual queries); `parquet_read` is data-dependent (a metadata-only COUNT
can skip Parquet); `load_table` / `oauth_token` fire only on a cold
catalog/OAuth miss. A unit test greps each literal at its cited engine
callsite so a silent engine-side rename can't zero a counter.

The capture layer (`BenchSpanCapture`, from `fluree-bench-support`) is installed
once as a global subscriber. Reps run strictly sequentially and drain the sink
after each, so per-rep counters are isolated even though the sink is
process-global — and spans emitted from the Iceberg reader's spawned decode tasks
are still captured (verified live).

## Runtime & deadlines (caveats)

- **Multi-thread runtime, `block_on`.** The R2RML query future is not `Send`
  (Parquet state across awaits), so it is run with `Runtime::block_on` on the
  calling thread rather than `tokio::spawn`ed. The runtime is *multi-thread* so
  the Iceberg reader's per-file `tokio::spawn` decode fan-out runs in parallel; a
  current-thread runtime would serialize decode and distort the measurement.
- **Per-query deadline.** Each execution gets a fresh `QueryCancellation`. A
  watchdog task cooperatively cancels it at `timeout_s` (the R2RML operators poll
  the handle and stop). A `tokio::time::timeout` at `timeout_s + 5 s` is the hard
  backstop. **Caveat:** if the hard backstop fires, the query future is dropped
  mid-scan — the in-flight scan may keep draining briefly in the background. The
  outcome is recorded as `dnf` with the wall pinned to the deadline cap.
- **Pacing.** A 2 s sleep precedes every execution against a virtual (live
  Snowflake) target to stay within catalog rate limits. Do **not** run the full
  smoke set against `virtual-sf20` casually — prefer `exec-one` for spot checks.
- **Storage layout.** vbench resolves the store as `<fluree_home>/storage`; it
  does not parse a custom `[server].storage_path` from the home's `config.toml`.

## Provenance

Each `run.jsonl` opens with a `RunMeta` line: schema version, ULID run id, RFC-3339
timestamp, git commit (+ dirty flag), build profile, host, tokio runtime shape,
the subset filter, and a fingerprint of every target (id, kind, alias, home).
_TODO: extend target fingerprints with a store-state hash (ns@v2 head) so an
incomparable "the ledger changed under me" run is detectable automatically._

## Validation status

`cargo test -p fluree-bench-virtual -p fluree-bench-support` is green
(corpus-validation, canon, span-aggregation/rename-guard, budget, bless-guard,
and cold-arg-forwarding unit tests — all hermetic: no network, no PAT).
End-to-end verified: `setup --verify` triple-count assertion on `native-sf01`;
a full 54/54-ok native run (source of the blessed oracles); the full
virtual-sf01 hot sweep + 6-query cold subset behind the committed perf
baselines; and a live SF20 survey. Cold q001 on virtual-sf01 hash-matched the
native blessed oracle end-to-end (the cross-pipeline parity proof).
