# Track C — Strategy slate (DRAFT for adversarial review)

**Status:** draft by audit-doc → lambda-audit adversarial pass (deployed grounding) → team-lead
second pass → survivors to AJ. **No remedy is committed here** — each lever is a candidate with its
predicted effect, cost, and, critically, **what it does NOT fix**. Grounded in
`A-engine-reality.md` (§1–4 + Appendix A.1–A.6) and `lambda-reality-B-deployed-forensics.md`
(deployed forensics + measured throughput).

## The frame: five query families and one measured ceiling

Everything is scored against the families the two tracks actually found, not a generic "virtual
query":

| Family | What it is | Observed | Dominant cost |
|---|---|---|---|
| **F-RUNAWAY** | `get_data_model` / info-stats | 900 s, pins query **+** router container 15 min | un-deadlined native full-stats scan of every table |
| **F-FACT** | any chat query materializing a FACT table (36–200 M rows) | the nine 55–61 s cancels | **row materialization** (CPU) |
| **F-SINGLE** | a single-large-file table (`DIM_CUSTOMER` 1.74 M rows / 1 file) | 31 s measured, `C=1` | single-threaded materialization |
| **F-CATALOG** | many-table first-touch (q055 shape; the star-schema breadth) | ~1.2 s/table cold | cross-region catalog RTT × N tables |
| **F-MANY** | many-tiny-file fact (the DW_SF01 epic shape — **not** this deployed dataset) | epic's tail | per-file request floor `F/C×L` |

**The measured ceiling that governs the ranking (Track B):** R2RML materialization runs at
**≈56,000 rows/s per core**, and the box has **≈6 vCPUs**, so the hard throughput ceiling is
**≈336,000 rows/s** — and `files_pruned=0` on every table means **every row is materialized**. This
single fact reorders the intuitive slate:
- The dominant term is **CPU-bound row materialization**, not fetch and not file count.
- A FACT table already spreads across ≥6 files, so `C = min(6, files) = 6` **already saturates all
  vCPUs**. Therefore **raising `FLUREE_ICEBERG_SCAN_CONCURRENCY` does almost nothing** for the
  deployed workload — you cannot exceed 6 concurrent decodes on 6 cores. The three ways past the
  ceiling are: **(a) materialize fewer rows** (prune/pushdown, or don't materialize at all for
  stats), **(b) more cores** (bigger Lambda), **(c) use the cores you have** (intra-file
  parallelism for the single-file case that wastes 5 of 6).

---

## Layered slate

Layers by intervention surface. Within each lever: **mechanism · family covered · predicted effect ·
cost/risk/owner · dependencies · does NOT fix**.

### Layer 0 — Stop the bleeding (correctness/safety, not speed)

**L0.1 — Manifest-backed virtual info-stats (routing fix).** *Recommended first — highest severity,
self-contained.*
- **Mechanism:** route a graph-source-federated dataset's `/info` stats to the **existing**
  metadata-only path (`build_graph_source_info` → snapshot-summary row counts, mapping-derived
  classes/properties, NDV `null`) instead of the native `assemble_full_stats` that materializes the
  federated tables. The metadata path already exists and is zero-data-read
  (`ledger_info.rs:1524-1526,1607-1648`); the gap is that `LedgerInfoBuilder::execute` reaches it
  only on the `is_not_found()` fallback (`:2172-2188`), so a virtual dataset that is *also* a
  committed ledger takes the scanning native path (`:487,:527`).
- **Family:** F-RUNAWAY (kills it outright).
- **Predicted effect:** 900 s → **sub-second** for `get_data_model`; removes the container-pin
  capacity risk. Independent of any deadline fix.
- **Cost/risk/owner:** **engine PR** (moderate). Risk: correctly detecting "virtual-backed committed
  ledger" and preserving native output shape; consumers already tolerate `null` NDV/flakes
  (`Option` fields, "null when unknown, virtual no-scan"). Low blast radius (info path only).
- **Depends on:** nothing.
- **Does NOT fix:** F-FACT chat scans (a real query still materializes); this only fixes the *stats*
  path.

**L0.2 — Deadline everywhere + engine scan-loop cancellation polling.** *Belt-and-suspenders for the
runaway; makes every cancel actually bite.*
- **Mechanism:** (solo) attach `x-query-timeout-secs` / `opts.timeout` to **all** engine invokes
  including `execute_query("info", …)` on the `/data-model` path (currently sends neither —
  `backend/mod.rs`), so the deadline guard bounds it. (engine) have the Iceberg scan/materialize
  loop **poll `check_cancelled()` mid-sweep** — today it is checked only between operator pulls /
  per-POM (`operator.rs:909/1090/1332/2377`), never inside the parquet fan-out or the
  `decode_large_file` blocking decode (§3d), so a cancel doesn't bite until the current scan yields;
  and detached `tokio::spawn` reads outlive the query (§3e).
- **Family:** F-RUNAWAY (bounds it even if L0.1 slips), F-FACT (frees the container promptly on
  cancel instead of burning to the next checkpoint).
- **Predicted effect:** 900 s → ≤ deadline; container freed at the deadline instead of at the next
  operator checkpoint; stops burn-after-abandon.
- **Cost/risk/owner:** **solo PR** (trivial — add the header) **+ engine PR** (moderate — thread a
  cancellation check into the scan/decode loop without a per-row cost). Risk: partial-work cleanup
  of detached reads.
- **Depends on:** nothing; complements L0.1.
- **Does NOT fix:** speed — nothing gets faster, the query still fails, just bounded and clean.

### Layer 1 — Engine (plan-level, survive cold, no cache needed)

**L1.1 — Predicate + row-filter pushdown / file pruning (`files_pruned=0` is the smoking gun).**
*The only lever that attacks the dominant term across the main chat family.*
- **Mechanism:** push selective SPARQL FILTERs down to Iceberg row-group pruning + Parquet row
  filters so fewer rows are materialized. Today `files_pruned=0` on every deployed table — either
  the BI queries carry no pushable predicate, or the mapping/plan isn't translating it. Extends
  PR-7 (numeric stats) / PR-5 (top-k) beyond their current trigger shapes.
- **Family:** F-FACT (the main chat-timeout family), partial F-SINGLE.
- **Predicted effect:** proportional to selectivity — a query filtered to 1 % of a 200 M FACT drops
  ~590 s → ~6 s. **Zero** when the query has no selective predicate (a BI "count all orders by
  region" scans everything by nature).
- **Cost/risk/owner:** **engine PR** (large, soundness-heavy — the hardest item). Risk: correctness
  of pushdown/decline; must decline safely (the epic's whole discipline).
- **Depends on:** knowing the real BI query shapes (do they carry pushable filters? — evidence gap;
  Track B saw none pushed).
- **Does NOT fix:** unfiltered aggregate/scan queries; F-RUNAWAY (stats needs no rows at all — use
  L0.1); F-CATALOG.

**L1.2 — Intra-file (row-group) parallelism.** *The only lever for the single-file regime.*
- **Mechanism:** decode a single file's row groups across multiple `spawn_blocking` tasks instead of
  one (`decode_large_file` is one blocking thread, `send_parquet.rs:691`; row groups are independent
  and already enumerated, `:68`), so a single-file table uses all vCPUs.
- **Family:** F-SINGLE (and few-file FACTs where files < cores).
- **Predicted effect:** ~×min(cores, row_groups) for single/few-file tables — `DIM_CUSTOMER` 31 s →
  ~5–6 s; a 1-file FACT similarly. **No effect** on a table already spread over ≥6 files (it already
  uses 6 cores).
- **Cost/risk/owner:** **engine PR** (moderate). Risk: memory (N row groups resident at once);
  ordering of emitted batches.
- **Depends on:** nothing; survives cold.
- **Does NOT fix:** the aggregate CPU ceiling — it redistributes the same 336k rows/s ceiling to use
  idle cores; a table already at `C=6` gets nothing. F-RUNAWAY, F-CATALOG.

**L1.3 — Manifest-count fast paths for aggregate queries (extend PR-6).** *Adjacent to L0.1 for
real queries.*
- **Mechanism:** answer `COUNT`/simple aggregates from Iceberg `record_count` sums where sound
  (`table_row_count` / `sound_manifest_row_count` already exist, `r2rml.rs:843,2212`); extend the
  fused-aggregate to more predicate-less / single-table shapes (the F22 un-fused-COUNT gap).
- **Family:** F-FACT for the aggregate subset.
- **Predicted effect:** a `COUNT(*)`-class query over a 200 M FACT → sub-second (no materialization).
  Bounded to aggregate shapes that don't need row values.
- **Cost/risk/owner:** **engine PR** (moderate). Risk: soundness of the manifest sum under deletes
  (guard already exists).
- **Depends on:** nothing.
- **Does NOT fix:** queries that materialize rows (projections, joins on values).

### Layer 2 — Config / CFN (cheapest, but check the ceiling)

**L2.1 — `FLUREE_ICEBERG_SCAN_CONCURRENCY` in CFN.** *Cheap, but near-moot for THIS workload —
include only with the caveat.*
- **Mechanism:** set the env so `C` isn't clamped to `min(available_parallelism, files)`.
- **Family:** F-MANY (many-tiny-file, I/O-wait-bound), marginally F-FACT.
- **Predicted effect:** **~0 for the deployed DW_SVL FACTs** — they already have 64–129 files so
  `C = min(6, files) = 6 =` all vCPUs; you cannot run 8 concurrent decodes on 6 cores for a
  CPU-bound materialization. Real benefit only when (a) the box has more vCPUs, or (b) the workload
  is fetch-wait-bound (many tiny files overlapping S3 latency — the DW_SF01 shape, not deployed).
- **Cost/risk/owner:** **CFN env** (trivial). Risk: memory (more in-flight decodes).
- **Depends on:** L4.1 (more vCPUs) to matter for F-FACT.
- **Does NOT fix:** the CPU ceiling on 6 cores; F-SINGLE (1 file → still 1).

**L2.2 — Raise Lambda memory ⇒ more vCPUs.** *The real "more concurrency" lever.*
- **Mechanism:** already at 10,240 MB (≈6 vCPU, the max) — so this is **capped**; noted for
  completeness. The only way to exceed 6 vCPUs is a different compute (Fargate/ECS/EC2), which is
  L4.
- **Family:** F-FACT, F-SINGLE (with L1.2).
- **Predicted effect:** materialization ceiling scales ~linearly with cores — but **Lambda is
  maxed**, so 0 additional headroom here.
- **Cost/risk/owner:** **CFN** — but no headroom left; escalates to L4 (non-Lambda compute).
- **Does NOT fix:** anything within Lambda (already max).

### Layer 3 — Solo request pattern

**L3.1 — Async query pattern (escape the 55 s cap).**
- **Mechanism:** for known-heavy operations (data-model, big BI scans), return a job id + poll/stream
  instead of a synchronous 55 s-bounded call; let the Lambda run to a higher bound and deliver when
  done.
- **Family:** F-FACT, F-RUNAWAY (bounds the sync router pin).
- **Predicted effect:** removes the 55 s UX cliff for slow-but-valid queries; the query still takes
  its wall, but the user gets a result instead of a 504.
- **Cost/risk/owner:** **solo PR** (large — new async/job surface + UI). Risk: product complexity;
  still needs a hard ceiling (pair with L0.2).
- **Depends on:** L0.2 (a real deadline/ceiling so async jobs don't run away).
- **Does NOT fix:** the wall itself; a 590 s FACT is still 590 s, just not a timeout.

### Layer 4 — Infrastructure / architecture

**L4.1 — Same-region placement (Lambda ↔ parquet ↔ catalog).**
- **Mechanism:** co-locate the query Lambda with the Iceberg parquet (currently us-east-1 → us-east-2)
  and the catalog, or replicate parquet into us-east-1.
- **Family:** F-CATALOG (removes ~1.2 s/table metadata RTT), F-FACT/F-SINGLE (removes cross-region
  per-byte fetch latency on the cold read).
- **Predicted effect:** removes a measured ~1.2 s/table cold floor (a multi-second win on a 19-table
  star) + cross-region fetch latency. **Secondary** — it does not touch the materialization wall, so
  a FACT query stays >55 s.
- **Cost/risk/owner:** **infra/CFN + data placement** (moderate; may be a customer-BYO-bucket
  constraint — the parquet is in a BYO `-use2` bucket, so this may be **not solo's to move**).
- **Depends on:** where the customer's Iceberg data lives (may be immovable).
- **Does NOT fix:** materialization (the dominant term).

**L4.2 — Catalog persistence across cold containers (shared/EFS/pre-warm).**
- **Mechanism:** move the per-container `/tmp` catalog cache (cold on all 16 containers) to a shared
  store (EFS mount or an S3-backed layer), or pre-warm it, so the metadata/pointer/scanfiles survive
  cold containers.
- **Family:** F-CATALOG.
- **Predicted effect:** removes the cold catalog chain (~1.2 s/table × N) on cold containers.
  Secondary vs materialization.
- **Cost/risk/owner:** **architecture** (large — EFS mount to Lambda, or a new shared cache). Risk:
  EFS latency/throughput, concurrency, staleness (the 300 s TTL question, F21).
- **Depends on:** nothing hard; interacts with the disk cache design.
- **Does NOT fix:** materialization; the parquet *data* cache still cold unless also shared (bigger).

**L4.3 — Shared/persistent parquet data cache.**
- **Mechanism:** back the parquet artifact cache with a shared store so a warmed file survives cold
  containers (today `/tmp` is per-container; the 8 GiB budget is already engine-wired, §reconciliation
  — the issue is persistence, not existence).
- **Family:** F-FACT/F-SINGLE **only on repeated reads of the same file**.
- **Predicted effect:** helps only when the *same* file is re-read (repeat query on warm shared
  cache); the first cold read still pays full cost, and cancelled-at-55 s scans never warm it.
- **Cost/risk/owner:** **architecture** (large). Risk: shared-cache latency can exceed a same-region
  S3 GET — may not pay off.
- **Does NOT fix:** the first-ask wall (materialization + cold read); the dominant term.

### Layer 5 — Data layout (Snowflake / customer side)

**L5.1 — Compaction to `N × 128 MB` where `N ≥ vCPUs`.**
- **Mechanism:** compact the source Iceberg tables so each has ≥ (cores) files of ~128 MB — enough
  files to fill all cores, not so few that a table drops to `C=1`.
- **Family:** F-MANY (per-file request-floor collapse), F-SINGLE (splits a 1-file table so `C>1`
  without L1.2).
- **Predicted effect:** F-MANY per-file overhead → gone; F-SINGLE gains parallelism. **Moves zero
  rows**, so the materialization ceiling is unchanged — a 200 M FACT is still 200 M rows to
  materialize.
- **Cost/risk/owner:** **Snowflake-side / customer maintenance job** (external owner). Risk:
  write-side cost + freshness (newly written small files exist until compaction runs); **compacting
  to too FEW files re-creates the F-SINGLE C=1 pathology** (the two-regime target is the guardrail).
- **Depends on:** customer control of the source layout (BYO dataset — may not be solo's to run).
- **Does NOT fix:** the dominant materialization term; F-RUNAWAY; F-CATALOG.

### Layer ∅ — The honest do-nothing per item

For each family, "accept it as-is": F-RUNAWAY do-nothing = keep pinning containers (unacceptable —
capacity risk, so L0.1/L0.2 are near-mandatory). F-FACT do-nothing = chat returns a clean 504 on
heavy queries (current state; correctness OK, UX bad). F-CATALOG do-nothing = a multi-second cold
floor (tolerable). The do-nothing baseline is the yardstick every lever's `(unlock × breadth)/cost`
is measured against.

---

## Ranking — `(predicted unlock × breadth) / cost`

| Rank | Lever | Family | Unlock | Breadth | Cost | Owner | Note |
|---|---|---|---|---|---|---|---|
| **1** | **L0.1** manifest-backed info-stats routing | F-RUNAWAY | kills 900 s runaway | narrow (stats) but highest severity | **low-mod** | engine | metadata path already exists — routing fix |
| **2** | **L0.2** deadline-everywhere + scan-loop cancel | F-RUNAWAY, F-FACT | bounds + frees containers | broad (all invokes) | **low** (solo) + mod (engine) | solo+engine | cheapest safety; make cancels bite |
| **3** | **L1.1** predicate/row pushdown | F-FACT | attacks the dominant term | broad **iff** queries filter | **high** | engine | ceiling-buster; 0 if no selective predicate |
| **4** | **L1.2** intra-file row-group parallelism | F-SINGLE | ~×6 single-file | moderate | **mod** | engine | plan-level, survives cold; nothing for ≥6-file tables |
| **5** | **L1.3** manifest-count aggregate fast paths | F-FACT (agg) | sub-s for COUNT-class | moderate | **mod** | engine | extends PR-6/F22 |
| **6** | **L3.1** async-query pattern | F-FACT | removes 55 s cliff | broad (UX) | **high** | solo | doesn't speed anything; pair w/ L0.2 |
| **7** | **L4.1** same-region placement | F-CATALOG, cold floor | ~1.2 s/table | moderate | **mod** | infra | secondary; parquet may be immovable BYO |
| **8** | **L5.1** compaction (N×128 MB) | F-MANY, F-SINGLE | per-file floor | moderate | **mod** | Snowflake | moves 0 rows; wrong target = C=1 pathology |
| **9** | **L4.2/4.3** shared catalog/data cache | F-CATALOG | cold-container persistence | narrow | **high** | architecture | secondary vs materialization |
| **10** | **L2.1** `SCAN_CONCURRENCY` env | F-MANY | ~0 for deployed | narrow | **trivial** | CFN | **near-moot**: FACTs already at C=6=vCPUs |

**Reading the ranking:** the top of the slate is **not** the intuitive "turn on a cache / raise
concurrency" — those are near-moot (L2.1) or secondary (L4). The high-value work is **stop the
runaway (L0.1/L0.2, cheap, near-mandatory)** then **reduce rows materialized (L1.1 pushdown, L1.3
manifest aggregates) and use idle cores (L1.2)** — because the measured wall is CPU-bound row
materialization on a fixed 6-core budget, and the only ways past that are to materialize fewer rows
or use the cores you have.

---

## What I most want the adversarial pass to attack

1. **The 56k rows/s ceiling and the "concurrency is moot" claim (L2.1).** One measured point
   (`DIM_CUSTOMER`, 4 columns). Is materialization really CPU-bound, or is some of that 31 s S3
   fetch that concurrency *would* overlap? A second measured table (esp. a multi-file FACT's actual
   rows/s at `C=6`) would confirm or break the ranking. If FACTs are partly fetch-bound, L2.1 and
   L4.1 rise.
2. **L1.1's breadth depends on unknown query shapes.** Do the real BI chat queries carry pushable
   predicates? Track B saw `files_pruned=0` everywhere — is that "no predicate" or "predicate not
   translated"? The answer moves L1.1 between "ceiling-buster" and "0."
3. **L0.1 output-parity risk.** Does any consumer of `/data-model` / `/info` depend on exact
   `flakes`/NDV that the metadata path returns as `null`? If so, routing to the metadata path is a
   contract change, not a pure win.
4. **L4.1 feasibility.** Is the customer's BYO parquet bucket (`-use2`) movable/replicable at all, or
   is cross-region a fixed constraint that makes L4.1 impossible and raises L1's weight?
5. **Does compaction (L5.1) belong at all** if the customer owns the source layout and the dominant
   term (materialization) is layout-independent?
