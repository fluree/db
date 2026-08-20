# fluree materialize

Build a **native twin ledger** from a virtual (R2RML-over-Iceberg) graph source: bulk-materialize every triple the mapping produces, verify the result against the source, and write it out as a local ledger or a portable `.flpack` pack.

A *twin* is a normal, fully-indexed Fluree ledger whose contents are a point-in-time snapshot of the virtual source — so it can be queried, branched, time-travelled, and shared like any other ledger, without a live catalog/S3 round-trip on every query.

**Note:** Requires the `iceberg` feature flag (enabled by default). See [Compatibility and Feature Flags](../reference/compatibility.md#fluree-db-api-features).

## Usage

```bash
fluree materialize <GRAPH_SOURCE> [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<GRAPH_SOURCE>` | The virtual graph-source id to materialize (e.g. `dw-gs:main`). Must already be mapped — see [`fluree iceberg map`](iceberg.md) / [Iceberg graph sources](../graph-sources/iceberg.md). |

## Options

| Option | Description |
|--------|-------------|
| `--into <LEDGER>` | Name for the twin ledger. Defaults to the graph-source id with a `-twin` suffix, preserving any `:branch` (`dw-gs:main` → `dw-gs-twin:main`). |
| `--output <FORM>` | `pack` (a `.flpack` file — the default), `ledger` (a local native ledger, left registered), or `s3` (direct-S3 CAS publish — not yet wired in the file-backed CLI; see DEC-003 §3). |
| `--output-path <PATH>` | Destination for `--output pack` (default: `<twin>.flpack` in the current directory). |
| `--verify <MODE>` | Depth of the parity gate run before the twin is announced: `quick` (default) or `full`. See [Verification](#verification). A failed gate **drops the twin** and exits non-zero. |
| `--max-performance` | Own-the-box: auto-size memory/parallelism to the host (~80% RAM). Only on a cleared machine — the default is deliberately conservative to stay co-resident-safe. |
| `--allow-mor-deletes` | Proceed even if a source table carries Iceberg merge-on-read delete files (sets `FLUREE_ICEBERG_ALLOW_MOR_DELETES`). The twin is then a point-in-time snapshot that **may include rows a MoR-aware reader would hide** — documented staleness. Default: fail closed. See [Iceberg merge-on-read](../graph-sources/iceberg.md#limitations). |
| `--allow-duplicate-parent-keys` | Proceed even if a foreign-key parent join key maps to **more than one parent row**. Default: fail closed. See [Duplicate parent keys](#duplicate-parent-keys). |
| `--home <PATH>` | Fluree home directory (overrides `$FLUREE_HOME` / the platform data dir). Where the twin ledger and its storage live. |
| `--tmp-dir <PATH>` | Scratch directory for `--verify full`'s on-disk spool + external-sort runs. Defaults to a subdirectory of the twin's `.fluree` storage area. See [Machine-safety posture](#machine-safety-posture). |

**Global flags** that tune the build (see [CLI README](README.md#global-options)):

- `--memory-budget-mb <MB>` — Memory budget in MB. `0` (unset) uses the co-resident default of **512 MB**, NOT host auto-sizing — pass `--max-performance` for the `0`-means-auto behavior (see [Machine-safety posture](#machine-safety-posture)). Drives chunk size, produce concurrency, and the FK parent-index budget; the build **fails loud** rather than exceeding it.
- `--parallelism <N>` — Produce-side worker threads (0 = the co-resident default of 2). Also bounds the concurrent Iceberg snapshot pins and FK pre-index scans.

## Description

`fluree materialize` streams the whole virtual graph source through the R2RML enumerator into the native bulk-import pipeline, producing a fully-indexed native ledger — the *twin*. Because the twin is an ordinary ledger, everything that works on a ledger works on the twin (SPARQL / JSON-LD / Cypher queries, time travel, branching, policy, `.flpack` export), with none of the per-query catalog/S3 latency of the live virtual source.

The flow is **build → publish → verify → (drop on failure)**. Verification needs a queryable twin, so the build publishes first; if the parity gate fails, the twin is dropped so an unverified twin is never left announced. (The drop can only ever hit a ledger this build just created — `create … import` refuses a ledger that already has commits.)

### The completion stamp (twin validity)

The build writes a **completion stamp** into the twin's *final* commit — a single `txn_meta` record carrying:

- `builderVersion` — the materialize builder that produced the twin,
- `mappingHash` — a SHA-256 of the R2RML mapping (a mapping change invalidates the twin),
- `watermark` — the per-table pinned Iceberg snapshot vector captured at build time (what a delta-sync reads), and
- `sampleSeed` — the seed for the reproducible verification sample.

The contract is: **a twin is valid iff a head-walk finds this stamp.** A build that dies mid-way leaves the head commit unstamped, so a partial twin is detectable. The stamp predicates live in the `https://ns.flur.ee/materialize#` namespace, and are required to resolve to that namespace on read — a commit carrying the same local names in any other namespace is not mistaken for a twin stamp.

To narrow cross-table snapshot skew, the builder does a **pin-all pre-pass** first: every table's current Iceberg snapshot is pinned (metadata-only) up front, so the watermark reflects one narrow window rather than the whole build duration.

### Verification

The parity gate re-checks the built twin against the virtual source at the pinned snapshots. Both modes are **memory-bounded** — neither ever holds the whole graph resident:

- **`quick`** (default) — per-class instance counts + a seeded sample of **3 subjects per class**, compared against the build's *own* enumerator. This is a **shared oracle**: it catches ingest/index corruption (a triple that made it into the enumerator but not the twin, or vice versa), but a bug in the enumerator logic itself appears identically on both sides and is *not* caught. Peak memory is O(sampled subjects).
- **`full`** — a whole-twin triple diff against the source. Both sides are spooled to disk (the source streamed through the enumerator, the twin streamed in a single linear pass over the binary index), each numeric/temporal value-canonicalized, then external-sorted and diffed. Peak memory is O(one external-sort run), never O(graph). Cost is roughly one extra full source read.

For a production cutover, run `--verify full` and, ideally, an independent native diff (e.g. materialize, `fluree export`, and diff against a separately-built ledger) — the shared-oracle caveat means quick verify alone cannot prove enumerator correctness.

### Duplicate parent keys

An R2RML `RefObjectMap` join resolves each child row's foreign key to a **parent subject**. If a parent join key maps to more than one parent row (a non-unique join column paired with a subject template keyed on other columns), the correct RDF output is a *fan-out* — one edge per matching parent. The builder does not yet emit that fan-out; it keeps a single parent per key. Baking that into a permanent twin would silently drop the other edges, so `fluree materialize` **refuses such a source by default**, with an error naming the parent table, its join column(s), and the ambiguous-key count.

Which parent is kept is **deterministic**: the lexicographically smallest parent subject IRI wins (the same rule the virtual query path now uses), so a rebuild from the same pinned snapshots is reproducible rather than a race between data files. Pass `--allow-duplicate-parent-keys` to build anyway; the twin then records the per-parent ambiguous-key counts in its completion stamp, so an overridden twin self-documents the anomaly it baked. The true fan-out is a tracked follow-up — once it lands, re-materialization heals existing twins.

### Machine-safety posture

The default posture is **co-resident-tolerant**: a modest fixed memory budget (512 MB) and low parallelism (2), never own-the-box auto-sizing. Raise them explicitly with `--memory-budget-mb` / `--parallelism`, or pass `--max-performance` on a cleared machine to auto-size to the host.

The build honors the memory budget rather than silently OOMing: the FK parent index (held resident for the whole build) is charged against a fraction of the budget, and the build **fails loud** with a clear error if it would overflow — raise `--memory-budget-mb` or reduce the FK-parent (dimension) key cardinality.

`--verify full` spools its two N-Triples renderings plus their sorted runs to disk — potentially tens of GB on a large twin. By default that scratch lands under the twin's `.fluree` storage area, **not** `std::env::temp_dir()`: on many Linux hosts `/tmp` is a tmpfs (RAM-backed), which would put the bounded-memory spill straight back into memory on exactly the large twins the design protects. Use `--tmp-dir` to point it at other fast local scratch. The scratch is cleaned up on completion (and on error); a hard crash (SIGKILL) leaves it behind, but rooting it under `.fluree` keeps that residue discoverable and cleanable.

## Examples

```bash
# Materialize a mapped Iceberg source to a .flpack pack (default), quick-verified
fluree materialize dw-gs:main

# Materialize into a named, locally-registered ledger
fluree materialize dw-gs:main --into analytics-twin:main --output ledger

# Full-verify before announcing, with an explicit budget on a shared box
fluree materialize dw-gs:main --output ledger --verify full --memory-budget-mb 2048

# Own-the-box throughput on a cleared machine
fluree materialize dw-gs:main --output ledger --max-performance

# Route full-verify scratch to a dedicated fast disk (never a tmpfs /tmp)
fluree materialize dw-gs:main --verify full --tmp-dir /mnt/scratch/fluree-verify

# Build from a source whose tables carry merge-on-read deletes (documented staleness)
fluree materialize dw-gs:main --output ledger --allow-mor-deletes
```

## Output

`--output ledger`:

```
Materializing twin 'dw-gs-twin:main' from 'dw-gs:main' (parallelism=2, memory_budget_mb=512, verify=Quick)…
Quick verify: class counts + a seeded per-class sample against the build's own enumerator — catches ingest/index corruption, NOT enumerator logic (shared oracle). Run `--verify full` plus the independent native diff before a production cutover.
Parity gate passed (7 checks).
Twin ledger 'dw-gs-twin:main' built and verified: 35238778 flakes, index t=42.
```

`--output pack`:

```
Twin packed to dw-gs-twin_main.flpack (35238778 flakes). The source twin ledger 'dw-gs-twin:main' stays registered locally; drop it with `fluree drop dw-gs-twin --force` when no longer needed.
```

## See Also

- [Iceberg / Parquet graph sources](../graph-sources/iceberg.md) — mapping tables and the twin/watermark concept
- [R2RML](../graph-sources/r2rml.md) — the mapping language
- [iceberg](iceberg.md) — `fluree iceberg map` and related commands
- [export](export.md) — `.flpack` archives
- [drop](drop.md) — remove a twin ledger
