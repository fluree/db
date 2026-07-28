# Virtual-Dataset (Iceberg / R2RML) — Per-File Decode Cost Decomposition (PR-2 phase 1)

**Date:** 2026-07-11
**Branch:** `perf/r2rml-pr2-decode-levers` (worktree `db-vbench`, off PR-1 HEAD `c68ba4f3a`)
**Companions:** `05-diagnosis.md` §0 (the file-count-bound wall), `ROADMAP.md` PR-2, `01-pathway-inventory.md` §N (strategies).
**Scope:** MEASUREMENT ONLY. No engine behavior changed. The evidence comes from four additive debug-level sub-spans temporarily nested inside the small-file Parquet read path and captured by the vbench span sink; the sub-spans and the harness allowlist entries are the only diff and are left uncommitted.

---

## The question

`05-diagnosis.md` established the master bottleneck: a fact table is ~7,670 tiny (~90 KB, ~23-row) Parquet files, decoded at ~39 files/s behind an 8-way concurrency clamp ⇒ ~197 s ⇒ DNF. It attributed a "~200 ms fixed per-file cost (footer parse + column-chunk setup + whole-file read + Arrow decode)" but did not split that 200 ms. This phase answers: **where does the ~200 ms go**, and **which PR-2 lever(s) move it**.

Candidates carried in from the diagnosis: (a) parquet footer fetch+parse, (b) Arrow reader setup, (c) tokio::spawn + channel overhead per file, (d) disk-cache admission/lookup, (e) S3 per-file round-trips even on cache hits, (f) the concurrency clamp itself.

---

## Method

Four sub-spans were added inside `read_task_small_file` (`fluree-db-iceberg/src/io/send_parquet.rs`), each wrapping one stage of a single file's read, nested under the existing outer per-file `iceberg.parquet_read` span (`r2rml.rs:1276`):

| Sub-span | Wraps | Stage |
|---|---|---|
| `iceberg.read_footer` | `read_metadata(path)` (`send_parquet.rs:293`) | footer fetch + parse |
| `iceberg.plan_columns` | `build_batch_schema*` | projected column-index resolution (CPU, metadata-only) |
| `iceberg.fetch_bytes` | `read_file_for_task(...)` (`send_parquet.rs:579`) | column-chunk / whole-file byte fetch (disk cache ∨ S3) |
| `iceberg.decode` | `decode_batches_arrow(...)` | Arrow decode from in-memory bytes |

All four are allowlisted in `fluree-bench-virtual/src/spans.rs` so the sink records `{n, total_us, max_us}` per name; **mean per-file wall = `total_us / n`**, which is independent of rep count and of concurrency (each concurrent file gets its own span instances). The residual `parquet_read − Σ(children)` is the tokio::spawn + `buffer_unordered` scheduling cost (candidate c). Probes ran against live `virtual-sf01` (Snowflake-managed Iceberg via R2RML, SF=0.1), sequential and 2 s-paced, on a 16-core host, release `vbench`.

Two cache regimes bracket the range:
- **cold** — `exec-one --cold` clears the home-scoped disk artifact cache and runs a fresh process (empty footer cache, empty catalog/OAuth cache): every read hits S3.
- **warm** — `run --cache-state hot`, median of 3 measured reps after a discarded priming rep: catalog + OAuth + disk cache are warm.

Primary probe: **q011** (orders-in-quarter; a date FILTER prunes the FACT_ORDER scan to **91 files**, completes ~3–7 s). Scale confirmation: **q046** (ORDER BY + LIMIT full 7,670-file FACT_ORDER scan).

---

## Result — the per-file cost is one S3 footer read, and warming does not touch it

Per-file mean wall (µs), q011, 91 files:

| Component | Cold | Warm | What it is | Moved by warming? |
|---|---:|---:|---|---|
| `iceberg.read_footer` | **196,208** | **190,627** | 2 sequential S3 range GETs: last 8 bytes (footer length), then the footer body (`send_parquet.rs:311`, `:331`) | **No** |
| `iceberg.fetch_bytes` | 83,519 | 231 | whole-file byte fetch — cold: 1 S3 GET; warm: local disk read (`read_whole_local`, `send_parquet.rs:600`) | **Yes** (disk cache) |
| `iceberg.decode` | 134 | 104 | Arrow decode from in-memory bytes | n/a (already trivial) |
| `iceberg.plan_columns` | 2 | 2 | projected column-index resolution | n/a (trivial) |
| spawn/channel (residual) | 166 | 42 | `tokio::spawn` + `buffer_unordered` scheduling | n/a (trivial) |
| **per-file total** (`iceberg.parquet_read`) | **280,029** | **191,006** | | |

Read the table top-down and the diagnosis's four-part "~200 ms" collapses to one line. Decode is **0.13 ms** — three orders of magnitude below the budget; the CPU decode is not the cost. Spawn/channel is **0.17 ms** — candidate (c) is dead. Column planning is noise. `fetch_bytes` is real cold (83 ms, one S3 GET) but **warming drives it to 0.23 ms** — the disk cache serves the 90 KB file from local SSD, exactly as designed.

**Everything that warming can fix, it fixes. What remains is `read_footer` at ~190 ms, cold and warm alike.** That single stage is **99.5 % of the warm per-file wall** and it is fixed: two sequential S3 round-trips per file, on every scan, that neither cache mitigates.

The setup floor moves independently and is not per-file: cold `r2rml.scan_table` = 3.35 s (cold `load_table` 2.50 s incl. OAuth 0.52 s + `scan_plan` 0.43 s); warm = 0.73 s (no `load_table`/OAuth — catalog warm; `scan_plan` 0.51 s). That is the H7 cold-catalog floor (PR-8's target), separate from the decode wall measured here.

### Why the footer read never warms — root cause

Two independent reasons, both in code:

1. **The footer cache is far too small for a fact table.** `ParquetFooterCache` is built at `(metadata_capacity/2).max(32)` = **64 entries** (`fluree-db-api/src/graph_source/cache.rs:160-162`, default `metadata_capacity`=128). q011 touches 91 files and a fact table has 7,670 — the working set is 1.4×–120× the cache, so it thrashes (every footer is a miss on every rep). This is why warm `read_footer` (190 ms) ≈ cold (196 ms): the cache is effectively never hit at scan scale.

2. **The footer read bypasses the disk cache entirely.** `read_metadata` range-reads the footer from **source S3** (`self.storage.read_range`, `send_parquet.rs:312`/`:332`) — it never consults `DiskArtifactCache`. So even when the whole 90 KB file is already sitting in the disk cache (warm `fetch_bytes` = 0.23 ms proves it is), the footer is still fetched fresh from S3 in two round-trips. The bytes are literally on local disk and we go back to S3 for them.

So the per-file wall is a fixed **~3 S3 round-trips cold / 2 warm** (footer: 2; data: 1 cold, 0 warm), at ~85–98 ms/round-trip on this link. That is the ~200 ms — it is fixed **S3 round-trip count**, not CPU, not spawn, not decode. This is the "10×-without-layout-changes" case, sharpened: it is addressable in-engine (collapse the round-trips) independent of file compaction.

---

## Concurrency-clamp verification (candidate f)

`iceberg_scan_concurrency(num_files)` (`r2rml.rs:37-49`): honor `FLUREE_ICEBERG_SCAN_CONCURRENCY` (a positive int, **uncapped**, min'd with `num_files`); else `available_parallelism.min(num_files).clamp(1, 8)`. On this 16-core host, q011 (91 files) ⇒ `min(16, 91).clamp(1,8)` = **8**. The default `run` matched the c=8 sweep point exactly (3,729 ms), confirming the clamp binds at 8 on any host with ≥8 cores; the env override is the only way past it.

Sweep (warm q011, override on):

| concurrency | wall (ms) | per-file wall (µs) | footer (µs) | vs default |
|---:|---:|---:|---:|---:|
| 1 | 19,477 | 197,299 | 194,615 | 0.19× |
| 4 | 5,665 | 201,996 | 199,866 | 0.66× |
| **8** (default) | **3,729** | 200,020 | 196,213 | 1.00× |
| 16 | 2,840 | 244,417 | 237,914 | 1.31× |
| 32 | 1,787 | 229,839 | 221,584 | **2.09×** |

Two facts. (1) **Per-file wall is flat across concurrency** (~200 µs, creeping to ~244 µs at c≥16 from mild S3 connection contention) — concurrency parallelizes the fixed round-trips, it does not change them, corroborating that the cost is I/O latency not a shared CPU resource. (2) **Wall scales ~linearly with 1/concurrency** until the ~1 s setup floor and the per-file contention flatten it: 8→32 buys **2.1×** on q011. The knee is ~16–32; past that the fixed setup floor dominates a small scan.

---

## Scale confirmation — q046 (full 7,670-file scan)

q046 (DNF-bounded to a 35 s window, 1,633 files decoded in that window, sampled across the whole table): `iceberg.parquet_read` = **194,866 µs/file**, `iceberg.read_footer` = **192,702 µs/file**, `fetch_bytes` = 1,786 µs, `decode` = 125 µs. Identical to q011's 91 files — the per-file footer cost is **scale-invariant**, not an artifact of q011's date-pruned partition. This closes the loop with §0: 7,670 × 195 ms / 8 = **187 s** ≈ the observed ~197 s DNF; 8 / 195 ms = **41 files/s** ≈ the diagnosis's ~39 files/s.

---

## Recommendation — PR-2 levers, ranked

**Lever A (primary, structural) — collapse the footer read into the whole-file read for small files.** For a file below the sparse threshold (already fetched whole via the disk cache / `storage.read`), fetch the whole file **once** and parse the footer from those in-memory bytes, instead of `read_metadata` issuing two separate S3 range reads up front. This collapses 3 S3 round-trips/file → 1 cold, and — critically — when the whole file is disk-cached (warm), the footer is parsed from the local file, so `read_footer` joins `fetch_bytes` at ~0.2 ms. Projected per-file: **cold 280 → ~90 ms (~3×); warm 191 → ~1 ms (~190×)**. For the 7,670-file scan: cold decode phase 187 s → ~60 s at c=8; **warm repeated scans of a fact table go from 187 s to ~1–2 s** (disk-served footers). Every one of the 30 fact-touching DNFs benefits in proportion to file count.
- Surface: `read_task_small_file` / `read_metadata` in `send_parquet.rs`. Behavior-neutral on results (same footer, same bytes). Risk: low–moderate (restructures the small-file read order; unit-cover footer-parse-from-whole-file).

**Lever B (primary, multiplier) — raise the decode-concurrency default above 8.** Measured 8→32 = 2.1× on q011; projected on the 7,670-file scan the decode phase (c=8: 187 s) → **~48 s at c=32 (~4×)**. The files are 90 KB micro-partitions and S3 sustains high parallel GET; the only cost seen is a mild per-file creep (200→244 µs) at c≥16. Recommend raising the default (e.g. `min(cpus·2, 32)` or a latency-aware default) while keeping the existing uncapped env override; the knee is ~16–32.
- Surface: `iceberg_scan_concurrency` (`r2rml.rs:37-49`), a one-function default change. Risk: low (memory is O(concurrency) × 90 KB = trivial; S3 fan-out stays bounded).

**Combined A+B (projected):** 7,670-file cold scan ~187 s → **~15–20 s** (1 GET × ~85 ms × 7,670 / 32); warm ~1 s. That converts the entire fact-scan DNF class into completing queries. Data-side compaction (7,670 → tens of files, `05-diagnosis.md` §0/H1(c)) remains the structural endgame and multiplies with both, but is out of engine scope and not required of customers.

**Lever C (secondary, follow-up) — size or bypass the footer cache for multi-query workloads.** The 64-entry cap makes the footer cache useless at fact-table scale. Lever A subsumes the warm/repeated case (the disk cache already holds the bytes), so this is lower priority; if kept, size the footer cache to the table or drop it in favor of the disk-served footer from Lever A. Note only.

**Out of PR-2 scope (PR-8):** the warm setup floor (`scan_table` ~0.73 s, `scan_plan`/manifest ~0.5 s) and the cold catalog +2.6 s — the H7 floor, targeted separately.

---

## Reproduction

```
# cold decomposition (clears disk cache, fresh process)
VBENCH_PAT="$(cat ~/.vbench/snowflake-pat.txt)" \
  vbench exec-one --query q011 --target virtual-sf01 --cold      # per-file JSON on stdout

# warm decomposition (median of 3 reps, caches primed)
VBENCH_PAT=... vbench run --targets virtual-sf01 --queries q011 --cache-state hot --virtual-reps 3

# concurrency sweep
for C in 1 4 8 16 32; do FLUREE_ICEBERG_SCAN_CONCURRENCY=$C \
  vbench run --targets virtual-sf01 --queries q011 --cache-state hot --virtual-reps 1; done

# scale confirmation (full FACT_ORDER scan, DNF-bounded)
VBENCH_PAT=... vbench run --targets virtual-sf01 --queries q046 --cache-state hot --virtual-reps 1 --timeout-s 35
```

Per-span mean = `.counters.spans["<name>"].total_us / .n` from the run/exec-one record. Raw records: `scratchpad/pf_cold_q011.json`, `pf_warm_q011.jsonl`, `pf_sweep_c*.jsonl`, `pf_scale_q046.jsonl`.

## Uncommitted measurement diff (to revert or gate behind a flag before shipping)

- `fluree-db-iceberg/src/io/send_parquet.rs` — 4 debug sub-spans in `read_task_small_file` + `use tracing::Instrument as _`.
- `fluree-bench-virtual/src/spans.rs` — 4 names added to `SPAN_ALLOWLIST`.

Both are additive and behavior-neutral. Keep them if PR-2 wants an on-going per-stage counter (small, stable), or drop them once the levers land — the levers themselves need no new spans.
