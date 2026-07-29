# Benchmarking

Performance benchmarks for the Fluree DB workspace. Use this doc to:

- learn what benches exist and what hot paths they cover,
- run them locally,
- read criterion's output and understand regression budgets.

For *adding* a new bench (or a new bench category), see
[`docs/contributing/benches.md`](docs/contributing/benches.md). The
chassis is documented in
[`fluree-bench-support/README.md`](fluree-bench-support/README.md).

## Running benches

```bash
# Run every bench in the workspace (long; uses default Quick profile).
cargo bench

# One specific bench at default scale:
cargo bench -p fluree-db-api --bench insert_formats

# Quick validation — single iteration, no statistics, useful for "did I
# break something":
cargo bench -p fluree-db-api --bench insert_formats -- --test

# Bigger inputs:
FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench insert_formats

# Full criterion sample counts (nightly profile):
FLUREE_BENCH_PROFILE=full cargo bench -p fluree-db-api --bench insert_formats

# Tracing on (writes spans to stderr; useful for debugging slow scenarios):
FLUREE_BENCH_TRACING=1 cargo bench -p fluree-db-api --bench insert_formats
```

## Env vars

| Var | Values | Default | Effect |
|---|---|---|---|
| `FLUREE_BENCH_PROFILE` | `quick` \| `full` | `quick` | sample-count + warmup discipline |
| `FLUREE_BENCH_SCALE` | `tiny` \| `small` \| `medium` \| `large` | `small` | per-bench input size |
| `FLUREE_BENCH_HOST_CLASS` | any name | `{os}-{arch}` | comparability class for baseline capture/compare |
| `FLUREE_BENCH_HOST` | any name | `$HOSTNAME` | host name recorded as provenance |
| `FLUREE_BENCH_TRACING` | `1` (or unset) | unset | install a stderr tracing subscriber |
| `FLUREE_BENCH_RUNTIME` | `multi` (or unset) | single-threaded | tokio runtime shape |
| `RUST_LOG` | tracing-subscriber filter | `info` when `FLUREE_BENCH_TRACING=1` | tracing levels per crate |

Those defaults are what a bare `cargo bench` on a laptop uses. **Both CI jobs
pin `FLUREE_BENCH_PROFILE=quick` and `FLUREE_BENCH_SCALE=tiny`** — including the
nightly, which is quick-profile despite the name — so a nightly number is not a
`full`-profile number. `FLUREE_BENCH_RUNNER_CLASS` is still read as the
pre-rename spelling of `FLUREE_BENCH_HOST_CLASS`.

## Current benches

Hand-maintained, and reconciled against the `[[bench]]` declarations plus
`regression-budget.json` — `cargo test -p fluree-bench-support --test
workspace_reconcile` enforces those two against each other, but nothing enforces
this table, so it is the one that rots. Add a row when you add a bench file.

| Crate | Bench file | Topic |
|---|---|---|
| `fluree-db-api` | `insert_formats.rs` | JSON-LD vs Turtle insert throughput, matrix of (format × txn count × nodes/txn) |
| `fluree-db-api` | `vector_query.rs` | End-to-end vector similarity through the query engine, 1K/5K articles, 768-dim |
| `fluree-db-api` | `fulltext_query.rs` | Full-text query through novelty + index |
| `fluree-db-api` | `import_bulk.rs` | Bulk Turtle import via `fluree.create(id).import(path).execute()`; single- vs default-threaded |
| `fluree-db-api` | `transact_commit.rs` | Single-commit latency on a fresh and a populated ledger (`iter_batched` setup) |
| `fluree-db-api` | `query_cold_reload.rs` | File-backed cold reload (load only, and load + first query) |
| `fluree-db-api` | `reindex_full.rs` | `Fluree::reindex(...)` end-to-end against a single-txn populated ledger |
| `fluree-db-api` | `reindex_incremental.rs` | Orchestrator's incremental path via `Fluree::trigger_index(...)` over delta novelty |
| `fluree-db-api` | `novelty_replay.rs` | Cold reload with `without_indexing()` so populate stays in novelty; scaled by commit count |
| `fluree-db-api` | `query_hot_bsbm.rs` | Warm-cache SPARQL: BSBM-shape Q3/Q5/Q9 against a reindexed ledger |
| `fluree-db-api` | `query_hot_bsbm_bi.rs` | BSBM Business-Intelligence "bowtie" (BI-1 F2): join-ordering tie-break between two equally-selective anchors |
| `fluree-db-api` | `query_hot_property_path.rs` | Hot-cache SPARQL property paths, one scenario per execution mode of the operator (`*` closure, sequence, `?`, …) |
| `fluree-db-api` | `query_hot_whole_graph_agg.rs` | Cypher aggregate folds from `fast_whole_graph_agg` (whole-graph + class scalars, histograms) against a linear-cost pipeline baseline |
| `fluree-db-api` | `query_overlay_matrix.rs` | The same query shapes at three ledger conditions — base / overlay / novelty — so the columnar+novelty merge lane has coverage |
| `fluree-db-api` | `annotation_hydration.rs` | `inject_annotations` hydration cost: index scan vs sealed annotation arena |
| `fluree-db-api` | `annotation_planner.rs` | Planner direction for `f:reifies*` edge-annotation queries: arena-informed row counts vs HLL-only stats |
| `fluree-db-query` | `vector_math.rs` | SIMD vs scalar dot/L2/cosine micro-bench |
| `fluree-db-spatial` | `spatial_bench.rs` | S2 covering build + within/intersects/radius latency |

## Reading criterion output

Each bench produces a console line per scenario like

```
insert_formats/jsonld/100txn_10nodes
                        time:   [184.59 ms 188.42 ms 192.71 ms]
                        thrpt:  [129.32K elem/s 132.34K elem/s 135.10K elem/s]
```

The triple in `time` is `[lower_bound mean upper_bound]` of a 95%
confidence interval. `thrpt` is the throughput unit chosen by the bench
(elements/sec, bytes/sec, etc.).

Criterion also emits an HTML report at
`target/criterion/<group>/<bench>/report/index.html`. Open it to see
plots and prior-run comparisons.

## Regression budgets

`regression-budget.json` at the workspace root sets the per-bench, per-scale
percentage regression that CI's gated job will accept once the gate is in
its final shape. The default is 5% for any (crate, bench, scale) tuple
not explicitly listed.

### CI gate — phases

The gate runs across `ci.yml` (per-PR) and `bench.yml` (nightly +
on-demand):

1. **Reconcile + smoke (`bench.yml` `bench-gate`, nightly).**
   - **Reconcile.** `cargo test -p fluree-bench-support --test workspace_reconcile`
     asserts every `[[bench]]` declared in a workspace member's `Cargo.toml`
     has a matching entry in `regression-budget.json`, and vice versa. A
     missing or stale entry fails with a message naming the `crate/bench` pair.
   - **Smoke.** `cargo bench --workspace -- --test` runs each bench's
     scenarios once at `tiny` scale — catches benches that compile but panic
     at runtime (bad SPARQL, broken setup, missing API surface).
2. **Per-PR compare (`ci.yml` `bench-compare`).** Runs the cheap subset
   (`query_overlay_matrix` + `query_hot_bsbm`) at `tiny`/`quick` and compares
   against the committed baseline via the `bench-baseline` bin. **Time and peak
   memory enforce only when the baseline's `host_class` matches the runner's**
   (see [Baselines](#baselines-capture--compare) for why). Today's committed
   baseline is `host_class=local` and the runner is `ci-ubuntu-latest`, so the
   step passes `--allow-host-mismatch` and both metrics annotate rather than
   gate; **phase-share drift enforces regardless of host class.** The nightly
   `bench-gate` runs the same compare over a larger sample.

   The job costs ~30 minutes, so it is gated on a `bench-paths` job that skips it
   when a PR touches nothing perf-relevant (engine crates, bench crates,
   `bench-baselines/`, `regression-budget.json`, `Cargo.toml`/`Cargo.lock`, or the
   CI/bench workflows). The list errs inclusive: a false positive costs one bench
   run, a false negative lets a regression through.

3. **CI-class capture (`bench.yml` `bench-capture`, `workflow_dispatch`).**
   Captures the cheap subset on `ubuntu-latest` (`host_class=ci-ubuntu-latest`)
   and uploads it as an artifact. Committing it plus dropping
   `--allow-host-mismatch` from the compare step lands a real per-PR gate. The
   `capture_samples` dispatch input controls how many repeat runs are folded into
   one median + MAD — use ≥ 5 for a baseline meant to gate, since without a noise
   estimate the budget has to absorb shared-runner flap on its own.

> **Visibility gap (documented; fix is a follow-up).** The per-PR gate only
> *compiles* benches — `clippy --all --all-features --all-targets` in `ci.yml`
> builds every bench, and `bench-compare` runs only the two cheap ones. Every
> other bench is *executed* nightly-only (`bench.yml`), visible to the last
> default-branch committer rather than to the PR that introduced a break. So a
> bench that compiles cleanly but panics at runtime — bad SPARQL, or a setup
> that matches zero data — sails through every PR and only reddens the nightly.
> This is exactly how the `query_hot_whole_graph_agg` `@vocab` bug reached main
> (its class scenarios matched zero nodes; the filtered-histogram sanity assert
> panicked). Two consequences: historical numbers for that bench's
> `SCALARS_CLASS`/`HISTOGRAM_CLASS` scenarios predate the fix, measured empty
> scans, and are void — re-baseline them the first time a committed baseline
> includes `whole_graph_agg` (the current committed baseline covers only the
> cheap subset, which is unaffected); and whether to promote a tiny `-- --test`
> runtime smoke into per-PR CI is an open follow-up, costed against the same
> budget that keeps the compare subset cheap.

To intentionally accept a regression (or tighten a budget), edit
`regression-budget.json` in the same PR and explain in the PR body.

## Baselines: capture & compare

Committed performance reference points live in
[`bench-baselines/`](bench-baselines/README.md) as one JSON file per phase
reference (`guardrails-pre.json`, …), captured and compared by the
`bench-baseline` bin. (This supersedes the earlier single `bench-baselines.json`
scheme — it is now a directory plus a bin.)

**Capture** — run the benches, then snapshot criterion's estimates plus the
memory and meta sidecars into a git-stamped baseline:

```bash
cargo bench -p fluree-db-api --bench query_overlay_matrix
cargo run -p fluree-bench-support --bin bench-baseline -- \
    capture --label guardrails-pre --out bench-baselines/guardrails-pre.json
```

The baseline records provenance: `git_sha`, `captured_at`, `profile`, `scale`,
and a `host` block — os, arch, cpu model, physical/logical cores, and,
load-bearing for the gate, `host.class`. The `"pre"` reference for a phase is
captured at the branch's **merge-base** (see `bench-baselines/README.md`).

**Compare** — after a change, rerun the benches and compare:

```bash
# --allow-host-mismatch is required against the committed baseline, which is
# host_class=local; without it compare refuses (exit 2) rather than compare
# absolute numbers across machines. Drop it once you have a same-class baseline.
cargo run -p fluree-bench-support --bin bench-baseline -- \
    compare --baseline bench-baselines/guardrails-pre.json --allow-host-mismatch \
    [--only <substr>] [--share-drift-pp 5] [--advisory]
```

Each scenario present in both runs is checked against `baseline × (1 +
budget_pct/100)` from `regression-budget.json`, for wall-clock time, peak memory
(`peak_bytes`), and — for benches that record them — per-phase time and per-phase
share.

| metric | units | gated across host classes? |
| --- | --- | --- |
| `time` | ns | no |
| `peak_mem` | bytes | no |
| `phase_time` | ns | no |
| `phase_share` | percentage points | **yes** |

**Absolute metrics enforce only within a host class.** A mismatch is a *refusal*
— exit 2, no verdict — unless `--allow-host-mismatch` downgrades them to
`::warning::` annotations. `--advisory` forces the downgrade even on a match.

> **No bench emits phases yet.** The `meta` sidecar and everything below about
> phases is Tier-1 scaffolding for the `fluree rdf` conversion lane: the schema,
> the recorder, and the gate are in place, but no bench in the workspace calls
> `meta::record_scenario` today, so no phase or corpus row appears in any current
> report. The share gate goes live with the first producer. Corpus identity is in
> the same state — the refusal rules are real, they simply have nothing to check
> until a bench declares what it read.

**Share drift always enforces**, because a share is a ratio within a single run:
the machine cancels. Only share *growth* gates; shares sum to 100, so gating both
directions would fail a clean improvement twice, once for the phase that got
better and once for its complement. This is the metric that catches the
regression an aggregate number hides — a scenario 2% slower overall whose parse
share moved 60% → 75% has a parse regression masked by a write improvement.

**A phase that appears or disappears is gated on its share alone.** Comparing
only same-named phases is how a regression hides: move work into a phase the
baseline never had and the old phase's share *falls*, reading as an improvement,
while the new phase carrying the cost produces no row. So a phase new in the
current run breaches when it claims more than the drift threshold, and a phase
that has vanished from the current run breaches when it *used to* claim more than
the threshold — work that large did not evaporate, it moved somewhere unmeasured.
Such rows are marked `(NEW)` / `(VANISHED)` and carry no `phase_time` row, since
there is no ratio to report against a missing side.

**A corpus mismatch is always a refusal, with no override.** Benches that read an
input record its SHA-256, byte length, element count, input/output syntax, and
thread count; comparing a run over one input against a baseline captured over
another is the most expensive silent failure this gate can produce, because the
output is indistinguishable from a real regression.

| condition | outcome | exit |
| --- | --- | --- |
| within budget | pass | 0 |
| enforced breach | fail | 1 |
| host class differs, no `--allow-host-mismatch` | refuse | 2 |
| corpus sha256 / syntax / thread count differs | refuse | 2 |
| baseline `schema_version` newer than the binary | refuse | 2 |

### Noise floors

`ubuntu-latest` flap is not a regression, and criterion's confidence interval
does not measure it — it describes iterations *within* one run. Accumulate
several independent runs instead:

```bash
for i in 1 2 3 4 5; do
  cargo bench -p fluree-db-api --bench query_overlay_matrix
  cargo run -p fluree-bench-support --bin bench-baseline -- \
      capture --label ci-pre --out bench-baselines/ci-pre.json --accumulate
done
```

With ≥ 5 samples the baseline records a median and a MAD, the gate compares
medians instead of single-run means, and the wall-clock budget widens to
`max(budget_pct, 3 × MAD / median)`. MAD rather than standard deviation because
one outlier run is the common failure here and barely moves it.

Three bounds on that, each of which exists because dropping it inverts the gate:

- **Widening only.** A budget tighter than the measured noise can only produce
  false alarms; a quiet scenario keeps its declared budget.
- **Wall-clock only.** The floor applies to `time` and `phase_time`, never to
  `peak_mem`. A tracking allocator's peak is near-deterministic for a given
  workload, so widening the memory budget by a *timing* MAD would let a real
  allocation regression through on the strength of a noisy clock.
- **Baseline only.** The current run's own spread never widens its own budget.
  Otherwise the worse a run behaves, the more it is forgiven.

`capture --accumulate` refuses to pool samples across host classes or across
corpora. Blending two populations into one median and MAD destroys the evidence
that they were ever different, and the accumulated file inherits the fresh run's
provenance — so there would be nothing left for a later `compare` to catch.

### Why the gate has two phases

Neither metric survives a cross-machine comparison. `ubuntu-latest` shared
runners flap, so a 5–10% threshold on a single PR run comparing absolute
nanoseconds against a baseline captured on different (local Apple-silicon)
hardware false-positives every few PRs. Peak memory is steadier but not
portable either: allocator behaviour, page size, and background load all move a
tracking allocator's peak, and the ±2.2% local-vs-CI figure this gate was first
built on is one measurement between two specific machines — not a portability
result. Gating on it while merely warning on time would also put the *less*
portable of the two signals in the blocking position.

So the gate runs in two phases:

- **Phase 1 (today).** The committed `guardrails-pre.json` is `host_class=local`,
  CI runs as `ci-ubuntu-latest`, the classes don't match, and the compare step
  passes `--allow-host-mismatch` so both absolute metrics annotate without
  gating. The job still earns its keep: the annotations surface real movement on
  the PR that caused it, share drift gates for any bench that records phases, and
  the compare itself exercises the capture/compare machinery.
- **Phase 2.** Commit a CI-class baseline and drop `--allow-host-mismatch`, and
  both absolute metrics gate from the next PR on.

A note on `host_class` values. The derived default is `{os}-{arch}`, which is
deliberately coarse and deliberately not a promise: an M1 and an M4 both derive
`macos-aarch64` and their absolute numbers are not interchangeable. A class is a
claim *a human makes* that two machines' numbers may be compared, so any host
whose numbers are meant to gate should set `FLUREE_BENCH_HOST_CLASS` explicitly
(`ci-ubuntu-latest`, `bench-m8gd`, …) rather than inherit the default.

Two ways to reach phase 2:

1. **Committed CI-class baseline (implemented).** Run `bench.yml`'s
   `workflow_dispatch` `bench-capture` job with `capture_samples: 5`, download
   the artifact, commit it, and remove `--allow-host-mismatch` from `ci.yml`'s
   compare step. Its `host_class=ci-ubuntu-latest` matches CI and its noise floor
   absorbs runner flap, so both metrics enforce.
2. **Same-runner interleaved base-vs-HEAD (documented future option).** Measure
   merge-base and HEAD in the *same* job on the *same* runner and compare those,
   instead of any committed cross-machine file — the only design that survives
   shared-runner variance at a tight time budget. Deliberately not wired: it
   doubles the per-PR build cost, the exact eviction risk that had removed an
   earlier per-PR compare.

The nightly reconcile + smoke still catches the regressions that don't depend on
baseline comparison: API breakage, panics, missing budgets.

> **Void pre-fix numbers — `query_hot_whole_graph_agg`.** Before the `@vocab`
> fix in this PR, that bench's class-anchored scenarios (`scalars/class`,
> `histogram/class`, `histogram/class_filtered`) matched zero nodes and silently
> measured *empty* results — so any historical numbers for them are void and
> must be re-baselined the first time a committed baseline includes that bench.
> The committed `guardrails-pre.json` subset is `query_overlay_matrix` +
> `query_hot_bsbm` **only** — it does not include `query_hot_whole_graph_agg` —
> so there is no collision, and the recaptured baseline is unaffected.

## Architecture

The bench chassis lives in
[`fluree-bench-support`](fluree-bench-support/README.md):

- `init_tracing_for_bench()` — opt-in tracing subscriber.
- `next_ledger_alias(prefix)` — atomic unique-alias generation.
- `bench_runtime()` — tokio runtime with bench-friendly defaults.
- `BenchProfile`, `BenchScale` — env-driven knobs.
- `gen::*` — deterministic data generators (people graphs, vectors,
  paragraphs).
- `fixtures::*` — vendored / fetched fixture loaders.
- `budget::*` — regression-budget loader and `check()` helper.
- `report::*` — opt-in human-readable end-of-run summary tables.

Benches start from `fluree-bench-support/templates/BENCH_TEMPLATE.rs` and
reuse these helpers rather than reimplementing them. See
[`docs/contributing/benches.md`](docs/contributing/benches.md) for the
six-step workflow to add one.

## Tracing inside a bench

A bench that wants per-stage timings (e.g., bulk import: parse → chunk →
resolve → root-build → publish) can run with
`FLUREE_BENCH_TRACING=1` and inspect the stderr output. The eventual
`FLUREE_BENCH_TRACING=file:./out.json` mode (handled by `BenchSpanLayer`)
will dump JSON spans for offline analysis; that mode is reserved today and
falls back to stderr until it ships.

For tracing conventions inside the database itself (where to put
`debug_span!` vs `trace_span!`, how to use `.instrument()` safely across
`.await`), see [`docs/contributing/tracing-guide.md`](docs/contributing/tracing-guide.md).

## Where benches live

```
fluree-bench-support/        # chassis (helpers, generators, templates, fixtures)
fluree-bench-alloc/          # tracking allocator behind the peak_mem metric
<crate>/benches/<name>.rs    # one file per bench; criterion harness=false
regression-budget.json       # per-bench, per-scale budgets at the workspace root
bench-baselines/             # committed reference points (see its README)
.github/workflows/ci.yml     # bench-paths + bench-compare (per-PR, cheap subset)
.github/workflows/bench.yml  # bench-gate (nightly) + bench-capture (on demand)
target/criterion/            # criterion estimates — what `capture` reads
target/fluree-bench-mem/     # peak/total allocation sidecars
target/fluree-bench-meta/    # corpus identity + phase timing sidecars
```

## Troubleshooting

- **"could not find `Cargo.toml` in `…`"** — run `cargo bench` from the
  workspace root or pass `-p <crate>` to scope to a specific crate.
- **A bench compiles but `cargo bench --bench X` says "no benchmark named X"** —
  check that the crate's `Cargo.toml` has a matching `[[bench]] name = "X"`
  entry.
- **Regression budget fails with no obvious cause** — re-run with
  `FLUREE_BENCH_PROFILE=full` to widen the sample. If still flaky, the
  budget for that bench/scale needs raising; edit the JSON and explain
  in your PR.
- **Tracing output appears in CI but not locally** — set
  `FLUREE_BENCH_TRACING=1` explicitly. CI may set it; local runs do not.
