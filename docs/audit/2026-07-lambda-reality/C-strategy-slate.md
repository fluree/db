# Track C — Strategy slate (v3, AJ-facing)

**Provenance.** Drafted by audit-doc (engine reality), adversarially reviewed by lambda-audit
(deployed grounding, `C-adversarial.md` O1–O7), second pass by team-lead (P1–P6). Grounded in
`A-engine-reality.md` (§1–4 + Appendix A.1–A.7) and `lambda-reality-B-deployed-forensics.md`
(deployed forensics, measured throughput, the 39 production query shapes). **No remedy is committed
— each lever states its predicted effect, cost, and, explicitly, what it does NOT fix.**

---

# Executive summary (one page)

**The problem.** Chat queries against the deployed BYO Iceberg dataset (`full-enterprise-byo-1`,
Snowflake `DW_SVL`) time out: nine `sparql_query` calls hit the 55 s chat budget and were cancelled;
one `get_data_model` ran 900 s and pinned two Lambda containers for 15 minutes. The perf wave IS
deployed (pin `210cf4833`) and correct — the deployed dataset is simply a **different shape than the
wave optimized.**

**Three evidence-forced reframes (the epistemic story — why to trust the third, not the first).**
This phase changed its mind about the dominant cost twice before landing:
1. **Per-file request floor** — the epic's `DW_SF01` shape (7,670 tiny files, 39 rows/file). Levers:
   compaction, concurrency, file pruning.
2. **Cross-region latency** — the deployed data is cross-region (us-east-1 Lambda → us-east-2
   parquet). A ~1.2 s/table cold constant.
3. **Row materialization (the measured truth).** Three completed single-file scans give a **flat
   ≈56,000 rows/s per core** of R2RML materialization across a 26× row range (DIM_ACCOUNT 56,560;
   DIM_GEOGRAPHY 53,210; DIM_CUSTOMER 56,360) — the signature of a **CPU-bound** per-row cost, not
   fetch. With `files_pruned=0` on every table, **every row is materialized.** FACT tables
   (36–200 M rows, 64–129 files) → **107–590 s** at C=6. This one number explains BOTH the 55 s
   cancels AND the 900 s runaway.

> **The ceiling that governs everything:** `wall ≈ rows_materialized / (56k × cores_used)`.
> 56k/core is **measured** (3 points); the **336k aggregate at C=6 is INFERRED** (no multi-file FACT
> ever completed — all >55 s — so linear ×6 scaling is an assumption; if the 6 decodes contend, FACT
> queries are *even slower*). Either way: the only ways under the bar are **materialize fewer rows**,
> **materialize faster per row**, or **more cores** (Lambda is already at its 10 GB / ~6-vCPU max).

**The workload (39 production shapes, families A–E).** The agent issued ~39 whole-table rollups in
ONE turn:

| Family | ~Count | Shape | Prunable? | Dominant cost |
|---|---|---|---|---|
| **A. DIM rollup, no filter** | ~15 | `COUNT/SUM/AVG … GROUP BY <dim attr>`, single DIM | **No, by design** | materialize the dim |
| **B. DIM rollup + boolean flag** | ~5 | + `ex:isCurrent true` | Marginal (low-selectivity, not pushed) | materialize the dim |
| **C. FACT rollup, no filter (the KILLERS)** | ~5 | `… GROUP BY <dim attr>` over 36–200 M-row FACTs (fact→dim FK join) | **No, by design + the volume wall** | materialize the FACT |
| **D. Plain projection + `LIMIT 20`** | ~11 | several with **no ORDER BY**, DIM+FACT | **Prunable but DECLINED (a bug)** | full scan despite LIMIT |
| **E. `get_data_model` info-stats** | 2 | whole star schema | **Shouldn't scan at all** | un-deadlined full-stats scan |

Families A/B/C (~25) are **unprunable by design** — a `GROUP BY` with no `WHERE` filter must read
every row; `files_pruned=0` is *correct*, not a defect. So predicate pushdown (the intuitive lever)
has **minority** breadth. Family D (~11) is a real, cheap-to-fix bug. Family E is the runaway.
(The 52 s cancel dissected in Track B is a family-C query: DIM_PRODUCT is the ~3 s small join side —
167K rows / 56k ≈ 3 s — and the 52 s of silence is the FACT_ORDER-class partner materializing, a
fact→dim(Product) join on `PRODUCT_KEY`, exactly the family-C `GROUP BY dim attribute` pattern.)

**The ranked slate, by effort class.** Ranked `(unlock × breadth) / cost`:

- **DAYS (ship first — cheap, high-certainty):**
  1. **T1.3 — DatasetOperator LIMIT/top-k forwarding** (family D, ~11 shapes). A *confirmed* bug
     (A.7): the dataset-path operator doesn't forward the row budget, so `LIMIT 20` full-scans.
     Full-scan → sub-second. The forwarding pattern we've built 3×.
  2. **T1.2 — Manifest-backed info-stats routing** (family E, the runaway). The metadata-only path
     already exists; route the virtual-dataset stats to it. 900 s → sub-second. Consumers are
     null-safe (O4).
  3. **T3.1 — Deadline everywhere + cancellation that bites** (safety, all families). Solo attaches
     the timeout to *all* invokes incl. info; engine polls cancellation mid-sweep. Bounds the burn;
     unconditional prerequisite for async.
- **WEEKS (the real ceiling-buster for the majority):**
  4. **T1.1 — Aggregate/GROUP-BY pushdown** (families A/B/C, ~25 shapes — the majority AND the
     killers). Compute the rollup columnar at the scan (aggregate by the FK-key column, join the
     small grouped result to the dim after) instead of materializing every FACT row to RDF. The one
     lever that matches the dominant deployed family. Large, soundness-heavy — the "new strategy."
  5. **T1.5 — Async query pattern** (families A/B/C UX floor). The only near-term user-visible win
     for the grouped-aggregate family until T1.1 lands: return a result eventually instead of a 504.
  6. **T2.3 — Intra-file row-group parallelism** (family-D/A/B single-file tables). Breaks the
     `C=1` pin so a single-file table uses all 6 cores (~×6).
  7. **T2.1 / T2.2 — Projection narrowing + R2RML row-path profiling** (all materializing families).
     Fewer terms per row; and 56k rows/s for 4 columns is slow — a profile may find a broad 2×.
- **ARCHITECTURE / EXTERNAL (heavy or not-Fluree-owned):**
  8. **P.1 — Product/prompt: MCP surfaces manifest stats** so the agent needn't fire ~39 exploratory
     rollups (converges with T1.2 — "make `get_data_model` fast + honest"). Possibly the best
     unlock-per-cost, but reduces *demand*, not capacity.
  9. **T3.2 — Customer-region compute** (family-CATALOG cold floor). NOT parquet replication (the
     BYO bucket is customer-owned, `s3_region` is customer config — O2); the only real same-region
     play is running compute in the customer's region. Heavy.
  10. **T3.3 — Shared/persistent catalog cache**, **T3.4 — SCAN_CONCURRENCY env (near-moot)**,
      **T3.5 — compaction (near-no-target on this dataset)** — Tier-3 constants; see below.

**The honest headline:** the deployed timeout is the single-large-file / whole-FACT-rollup shape the
epic never optimized. The cheap wins (D bug, E routing, deadline) are days of work; the lever that
actually moves the majority (aggregate pushdown) is weeks and is the genuinely new capability;
everything cache/region/compaction-shaped is a secondary constant, and two of those levers lean on
infra the BYO customer — not Fluree — controls.

---

# Detailed levers

Per lever: **mechanism · family coverage (A–E) · predicted effect (anchored) · owner-surface ·
dependencies · does NOT fix · first concrete step.**

## Tier 1 — Touch fewer rows (attacks the dominant term)

### T1.3 — DatasetOperator LIMIT/top-k budget forwarding *(confirmed bug; rank #1)*
- **Mechanism:** `DatasetOperator` (`dataset_operator.rs:177`, `impl Operator :339`) inherits the
  no-op `set_row_budget`/`set_topk` (`operator.rs:101/111`); `GraphOperator` overrides them
  (`graph.rs:639/647`). A dataset-path virtual query is `Project → Limit → DatasetOperator →
  [member: GraphOperator → R2RML scan]`; the LIMIT budget (`limit.rs:85`) dies at the
  `DatasetOperator` and never caps the scan's 512 K-row materialize window (A.7).
- **Family:** **D** (~11 shapes).
- **Predicted effect:** a `LIMIT 20` becomes a ~20-row scan → **full scan (≥55 s) → sub-second**.
- **Owner-surface:** **engine PR** (low — one operator gains two methods + a forwarding test).
- **Dependencies:** confirm the deployed query uses the dataset path (below).
- **Does NOT fix:** families A/B/C (no LIMIT to push — LIMIT is post-aggregation there); E.
- **First concrete step:** `EXPLAIN` a `SELECT … LIMIT 20` on `full-enterprise-byo-1:main` and read
  the operator tree — confirm a `DatasetOperator` sits between `Limit` and the graph-source scan
  (vs the single-view `GraphOperator` path, which already forwards). Then mirror `graph.rs:639/647`
  onto `DatasetOperator`, threading the budget/top-k to each member's inner op.

### T1.2 — Manifest-backed virtual info-stats routing *(kills the runaway; rank #2)*
- **Mechanism:** route a graph-source-federated dataset's `/info` stats to the **existing**
  metadata-only path (`build_graph_source_info` → snapshot-summary row counts, mapping-derived
  classes/properties, NDV `null`; `ledger_info.rs:1524-1526,1607-1648`) instead of native
  `assemble_full_stats` (`:487,:527`), which materializes the federated tables. The gap is routing:
  `LedgerInfoBuilder::execute` reaches the metadata path only on the `is_not_found()` fallback
  (`:2172-2188`), so a virtual dataset that is *also* a committed ledger takes the scanning path.
- **Family:** **E** (kills it), and P.1's engine half.
- **Predicted effect:** 900 s → **sub-second**; removes the two-container pin.
- **Owner-surface:** **engine PR** (low/moderate — path exists; routing + skip-scan).
- **Dependencies:** none.
- **Does NOT fix:** real chat scans (A–D). **Output parity (O4):** ship **counts-from-manifest**;
  return NDV / datatype-distribution / `flakes` best-effort (`null` when Iceberg column stats lack
  them). Every consumer is null-safe — `data_model.rs` reads `count`/`total_instances` (present),
  the MCP formatter guards the NDV hint behind `if prop.ndvValues && …`. The lost NDV selectivity
  hint is an **accepted, reversible degradation**, not a contract break.
- **First concrete step:** at the info dispatch, detect "graph-source-federated committed ledger" and
  branch to the metadata path; assert the null-safe consumers in a test.

### T1.1 — Aggregate / GROUP-BY pushdown *(the majority ceiling-buster; rank #4, WEEKS)*
- **Mechanism:** families A/B/C are `COUNT/SUM/AVG … GROUP BY <dim attribute>` with no row filter —
  unprunable, so the only lever is to **not materialize every row to RDF.** Two sub-families, sized
  separately:
  - **(i) whole-table `COUNT(*)`** → Iceberg manifest `record_count` sum (already sound-guarded:
    `table_row_count` / `sound_manifest_row_count`, `r2rml.rs:843,2212`; the F22 lineage). **Cheap.**
  - **(ii) grouped `COUNT/SUM/AVG … GROUP BY <dim attr>`** (families A/C — the killers) → the
    tractable design (P2): the GROUP-BY key is a **dim attribute reached via a fact→dim FK join**, so
    **aggregate by the FK key COLUMN at the scan** (columnar over the Parquet chunks — count/sum per
    FK value, **no per-row RDF term construction**), producing a *small* grouped result keyed by FK,
    **then join that small result to the dim after** to attach the attribute labels. This covers the
    family without opening generic join-aware pushdown. **Real column-level engine work.**
- **Family:** **A, B, C** (~25 shapes — the majority and the 55 s/900 s killers).
- **Predicted effect:** COUNT-class → **sub-second**. Grouped SUM/AVG → bounded by column-chunk
  decode of the *aggregated + FK* columns only, not all columns × all rows to terms — removes the
  336k-rows/s materialization term for the exact dominant family. Not sub-second for
  extreme-cardinality GROUP BY, but the FACT killers drop from ~107–590 s to seconds.
- **Owner-surface:** **engine PR — large** (the COUNT slice is moderate; the columnar grouped
  aggregate + post-join is the real work). Risk: soundness (deletes, nulls, decimal), and the
  fall-back boundary when the shape isn't a clean fact→dim FK aggregate.
- **Dependencies:** extends PR-6/F22 machinery.
- **Does NOT fix:** family D (projection, not aggregate); E (use T1.2); queries needing row values.
- **First concrete step:** confirm from the 39 shapes how many of the ~25 are pure `COUNT` (cheap,
  ship first) vs `SUM/AVG` (the columnar work), then prototype the FK-key columnar aggregate on one
  family-C shape (FACT_ORDER `GROUP BY orderChannel`) and gate it against the materialized answer.

### T1.5 — Async query pattern *(UX floor for the killers; rank #5, WEEKS)* — RAISED per O1.3
- **Mechanism:** for known-heavy shapes (family C, `get_data_model`), return a job id + poll/stream
  instead of a synchronous 55 s-bounded call; run to a higher bound and deliver into the turn model.
- **Family:** **A/B/C** (the grouped-aggregate family that has *no* engine speed lever until T1.1).
- **Predicted effect:** removes the 55 s UX cliff — a 590 s FACT rollup returns a result instead of a
  504. It does **not** speed the query.
- **Owner-surface:** **solo PR — moderate/high.** Not greenfield (O6): solo has job-row scaffolding
  (`STATUS_TABLE`, `task-watcher`, materialize SQS-FIFO, `/turns/{id}/complete`) — this is *assemble*,
  not invent — but there is **no async query path today** (`invoke_sync`), so it is new surface.
- **Dependencies:** **T3.1 (hard — P5):** async WITHOUT a server deadline + cancellation = the query
  runs invisibly to 900 s (the current runaway, just backgrounded); async WITH it = a pure UX win.
- **Does NOT fix:** the wall itself; families D/E have cheaper direct fixes.
- **First concrete step:** spec a query-as-job envelope over the existing job-row + turn-complete
  primitives, deadline-bounded; prototype for `get_data_model` first.

### T1.4 — Predicate / row-filter pushdown *(minority; rescoped — was overweighted)*
- **Mechanism:** push selective `WHERE` filters to Iceberg row-group pruning + Parquet row filters.
  **Rescoped (O1):** the evidence shows families A/C carry **no** filter (`files_pruned=0` is
  correct), so this covers only family D's filtered variants and family B's boolean flags — and B's
  flags are low-selectivity (most rows current/active) so pruning barely helps even if pushed.
- **Family:** **D (filtered subset), B (marginal)** — the minority.
- **Predicted effect:** proportional to selectivity; **~0 for A/C** (the majority).
- **Owner-surface:** **engine PR — high** (soundness). **Investigate before scoping.**
- **Does NOT fix:** the unfiltered aggregate majority (→ T1.1).
- **First concrete step:** none urgent — subsumed by T1.1 for the majority; revisit only if a real
  filtered family emerges.

## Tier 2 — Materialize less / faster per touched row

### T2.3 — Intra-file (row-group) parallelism *(the single-file lever; rank #6)*
- **Mechanism:** `decode_large_file` runs the whole file on ONE `spawn_blocking` thread
  (`send_parquet.rs:691`); row groups are independent + enumerated (`:68`). Decode across cores so a
  single-file table uses all 6 vCPUs.
- **Family:** **A/B/D single-file tables** (DIMs, single-file FACTs).
- **Predicted effect:** ~×min(cores, row_groups) — DIM_CUSTOMER 31 s → ~5–6 s. **Zero** for a table
  already on ≥6 files (the FACTs — already C=6). Note (O5): this is the engine-side answer to the
  single-file problem that compaction would otherwise address, without asking the customer to run a
  maintenance job.
- **Owner-surface:** **engine PR — moderate.** Plan-level, survives cold. Risk: N row groups
  resident (memory), emitted-batch ordering.
- **Does NOT fix:** the ≥6-file FACT killers (→ T1.1); E.
- **First concrete step:** split `decode_large_file`'s row-group range across `spawn_blocking` tasks
  bounded by `min(cores, row_groups)`; verify on DIM_CUSTOMER.

### T2.1 — Projection narrowing
- **Mechanism:** materialize RDF terms only for referenced columns, not the full star (the two-scans
  finding shows narrow projections are read; ensure the row path doesn't build terms for unreferenced
  POMs).
- **Family:** A/B/C (partial), D.
- **Predicted effect:** ~(demanded cols / total POMs) — a 20-predicate star projecting 3 → ~5–6×.
- **Owner-surface:** **engine PR — moderate.**
- **Does NOT fix:** a whole-star projection; the per-row floor (→ T2.2).
- **First concrete step:** trace whether the R2RML operator materializes unreferenced POMs for a
  narrow SELECT; if so, gate materialization on referenced vars.

### T2.2 — R2RML row-path throughput (profile)
- **Mechanism:** 56k rows/s/core for 4 columns is slow — profile where it goes (term construction,
  per-binding allocs, IRI encoding, POM iteration); late/lazy materialization is a candidate.
- **Family:** ALL materializing families (the shared per-row constant).
- **Predicted effect:** unsized — a 2× on the row path is a 2× on **everything**. Potentially the
  broadest Tier-2 win, or irreducible; a profile decides.
- **Owner-surface:** **engine — profile, then PR.**
- **Does NOT fix:** the row COUNT (→ Tier 1).
- **First concrete step:** flamegraph a single-file DIM scan (DIM_CUSTOMER) in a local harness; report
  the per-row breakdown before scoping a fix.

## Tier 3 — Constants + hygiene (secondary; two lean on customer infra)

### T3.1 — Deadline everywhere + cancellation that bites *(unconditional safety; in DAYS class)*
- **Mechanism:** (solo) attach `x-query-timeout-secs`/`opts.timeout` to **all** engine invokes incl.
  `execute_query("info", …)` (currently neither — the runaway, `backend/mod.rs`). (engine) poll
  `check_cancelled()` mid-sweep in the scan/decode loop — today checked only between operator pulls
  (`operator.rs:909/1090/1332/2377`), never inside the parquet fan-out or `decode_large_file`; and
  detached `tokio::spawn` reads outlive the query (§3d/3e).
- **Family:** E (bounds it even without T1.2), all (frees containers on cancel).
- **Predicted effect:** no speedup — bounds burn, frees containers, **prerequisite for T1.5 async.**
- **Owner-surface:** **solo PR — trivial (header) + engine PR — moderate (mid-loop cancel).**
- **Does NOT fix:** speed.
- **First concrete step:** solo — add the header on the `/data-model` invoke; engine — add a
  cancellation check to the parquet fan-out loop.

### T3.2 — Customer-region compute *(demoted + rescoped per O2)*
- **Mechanism:** the deployed cross-region cost has TWO parts: **(a) a Fluree TEST artifact** — the
  parquet is `s3://fl-svl-iceberg-smoke-use2`, a **Fluree-owned smoke bucket** read via ambient
  creds cross-region; that ~1.2 s/table is self-inflicted and fixable by moving the *smoke* bucket to
  us-east-1 (test hygiene, **not a product lever** — P3). **(b) a real customer** — `s3_region` is
  **customer-supplied per-catalog config** (`virtual_graphs.rs:728`); the customer's parquet lives in
  the customer's region and **Fluree cannot relocate it.** The only real same-region play is
  **running query compute in the customer's data region** (multi-region deployment).
- **Family:** F-CATALOG cold floor (~1.2 s/table); cross-region fetch trim.
- **Predicted effect:** removes the ~1.2 s/table metadata RTT — **secondary** (materialization
  dominates once FACTs are touched, so it does not bring a FACT query under 55 s).
- **Owner-surface:** **(a) infra test-hygiene — trivial; (b) architecture — heavy** (multi-region).
- **Does NOT fix:** materialization.
- **First concrete step:** move the smoke bucket to us-east-1 for clean re-measurement (removes the
  test artifact); shelve multi-region compute as a heavy architecture item.

### T3.3 — Shared / persistent catalog cache (EFS/pre-warm) — architecture, secondary.
Removes the per-container cold catalog (~1.2 s/table × N) across the 16 cold containers; secondary vs
materialization; EFS latency + 300 s-TTL staleness (F21) are the risks. First step: prototype an EFS
mount for the `-catalog` dir and measure vs a same-region catalog GET.

### T3.4 — `FLUREE_ICEBERG_SCAN_CONCURRENCY` env *(near-moot — keep for completeness)*
`~0` for the deployed workload: DW_SVL FACTs already have 64–129 files so `C = min(6, files) = 6 =`
all vCPUs; you cannot run 8 CPU-bound decodes on 6 cores (the 56k ceiling, O3). Real benefit only
with more vCPUs (Lambda maxed) or a fetch-wait-bound workload. First step: none — do not set it
expecting a deployed win.

### T3.5 — Compaction *(demoted per O5 — almost no target here)*
Moves zero rows (doesn't touch the dominant term) AND has **no table to act on** on this dataset: the
FACTs are already **64–129 files** (over-fragmented, the opposite of F-MANY), and you would not
compact a 67k-row DIM. Its only local benefit — splitting single-file `DIM_CUSTOMER` — is what T2.3
does engine-side without a customer maintenance job. It is a lever for the *other* (DW_SF01
many-tiny-file) dataset shape, not this one. First step: none for this dataset.

## Product / prompt tier

### P.1 — MCP surfaces manifest stats / a fast data-model *(converges with T1.2; possibly best unlock/cost)*
- **Mechanism:** the agent issued **~39 whole-table rollups in one turn** to understand the data. The
  MCP surface already has the home (O7): `get_data_model` calls `/data-model` (`mcp-tools.ts:1307`);
  **T1.2 makes that call fast + honest.** Surfacing dataset row counts / schema / column stats
  (zero-read Iceberg metadata) up front lets the model skip exploratory `COUNT/GROUP BY` scans.
- **Family:** A/B/C (removes the *demand*), E (is the `get_data_model` fix itself).
- **Predicted effect:** could remove a large fraction of the 39 rollups — the wall you never pay.
- **Owner-surface:** **solo/product — low/moderate** (tool-surface + prompt), reusing T1.2's engine
  work.
- **Does NOT fix:** a genuine user question that needs a real rollup (→ T1.1); capacity (it reduces
  demand).
- **First concrete step:** ship T1.2, then have `get_data_model` return manifest-backed counts +
  schema, and prompt the agent to consult it before issuing rollups.

## Layer ∅ — Honest do-nothing per family
**E** do-nothing = keep pinning two containers 15 min (unacceptable — T1.2/T3.1 near-mandatory).
**D** do-nothing = a `LIMIT 20` scans the whole table (a bug left in; cheap to fix — hard to
justify). **A/B/C** do-nothing = chat 504s on rollups over facts (correct, UX bad — the async floor
T1.5 or the real fix T1.1). **CATALOG** do-nothing = a multi-second cold floor (tolerable).

---

# Ranking — `(predicted unlock × breadth) / cost`

| Rank | Lever | Effort | Family | Unlock | Cost | Owner |
|---|---|---|---|---|---|---|
| 1 | **T1.3** DatasetOperator forwarding | days | D (~11) | full-scan → sub-s | low | engine |
| 2 | **T1.2** manifest info-stats routing | days | E | 900 s → sub-s | low-mod | engine |
| 3 | **T3.1** deadline + cancel-that-bites | days | E + all (safety) | bounds burn | low+mod | solo+engine |
| 4 | **T1.1** aggregate/GROUP-BY pushdown | weeks | A/B/C (~25) | killers → seconds | **high** | engine (new) |
| 5 | **P.1** MCP fast data-model / stats | days-wk | A/B/C + E | removes demand | low-mod | product (rides T1.2) |
| 6 | **T1.5** async query | weeks | A/B/C (UX floor) | 504 → result | high | solo (needs T3.1) |
| 7 | **T2.3** intra-file parallelism | weeks | single-file A/B/D | ~×6 | mod | engine |
| 8 | **T2.1 / T2.2** projection / row-path | weeks | all materializing | cols-ratio / profile | mod | engine |
| 9 | **T3.2b** customer-region compute | architecture | CATALOG | ~1.2 s/table | heavy | architecture |
| 10 | **T3.3** shared catalog cache | architecture | CATALOG | cold floor | high | architecture |
| — | T1.4 predicate pushdown | — | D/B minority | selectivity | high | engine (investigate) |
| — | T3.4 SCAN_CONCURRENCY env | — | — | ~0 deployed | trivial | CFN (don't) |
| — | T3.5 compaction | — | — | ~0 target here | mod | Snowflake (don't) |

**Bottom line.** Ship the three **DAYS** items now (D-bug forwarding, E routing, deadline) — they
close the runaway and the LIMIT bug cheaply and correctly. The one lever that moves the deployed
**majority** (aggregate pushdown, families A/B/C) is a **WEEKS**, genuinely-new engine capability —
it is the strategic item AJ predicted. Everything cache/region/compaction-shaped is a secondary
constant, and the same-region/compaction levers lean on infra the **BYO customer** controls, not
Fluree. The product lever (surface stats so the agent stops issuing 39 rollups) may beat them all on
unlock-per-cost and rides the same engine work as T1.2.

---

# Open items for the record (evidence gaps, not conclusions)
- **T1.3 path confirmation** (the one thing that sizes rank #1): `EXPLAIN` a dataset `LIMIT 20` to
  confirm the `DatasetOperator` (vs single-view `GraphOperator`) path. Strong indirect evidence (D's
  full scans) says it does.
- **The 336k aggregate is inferred** (O3): no multi-file FACT completed; if the 6 decodes contend,
  FACT queries are slower than predicted — which only strengthens Tier 1.
- **T1.1 sub-family split:** how many of the ~25 A/B/C shapes are pure COUNT (cheap) vs SUM/AVG (the
  columnar work) — sizes the WEEKS effort.
