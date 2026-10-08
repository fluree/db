# Virtual-Dataset (Iceberg / R2RML) — Stacked Re-Baseline Scoreboard, Tail Diagnosis & Prize Memo (WP9)

**Date:** 2026-07-13
**Branch:** `perf/r2rml-pr4b-batched-optional` (worktree `db-vbench`, HEAD `36564e8e6`)
**Stack under test:** #1450 ← #1475 (bench) ← #1476 (PR-0 correctness) ← #1478 (PR-1 COUNT) ← #1482 (**PR-2 footer round-trips + concurrency**, `7e83ef06a`) ← #1484 (PR-3 star-prune, `3f1034355`) ← #1485 (PR-4 parent-memo, `b4fb62a47`) ← #1486 (chore) ← #1487 (PR-4b batched-OPTIONAL, `36564e8e6`)
**Supersedes:** `08-post-stack-rebaseline.md` — the predecessor's pre-restart scaffold for this same run, whose data sections were left `_(pending full run)_` because the machine restart wiped its `scratchpad/pf5_rebase_*.jsonl` artifacts. This doc completes it from the durable `results/runs/` data. (08 can be deleted or kept as history — lead's call.)
**Companions:** `05-diagnosis.md` (H1–H8 verdicts, deep-dives), `06-per-file-cost.md` (the footer-read root cause PR-2 fixed), `07-pr4b-batched-optional.md` (the OPTIONAL admission analysis), `ROADMAP.md` (the PR slate + PR-5..PR-8).
**Inputs:** re-baseline `results/runs/virtual-sf01-stacked-rebase-20260713T214121Z.jsonl` (54 queries, `cache-state hot`, 3 reps where a query completes, git `36564e8e6`); original baseline = the four `virtual-sf01-full*.jsonl` files (2026-07-10/11, pre-PR-0 head). Both are `virtual-sf01` (Snowflake-managed Iceberg via R2RML, `DW_SF01`, SF=0.1). Native oracles unchanged.

---

## 0. Headline (verified against the record)

| Outcome | Original baseline | Stacked re-baseline |
|---|---:|---:|
| `ok` (completes) | 24 | **49** |
| `dnf` (≥120–180 s) | 30 | **2** (q016, q031) |
| `error` (loud refuse) | 0 | **1** (q013) |
| `expected_error` | 0 | **2** (q034, q051) |

**28 of the original 30 DNFs flipped to `ok`.** The remaining two — q016 and q031 — are diagnosed below. Three queries that *used* to report `ok` now report a non-`ok` status **on purpose**: q013/q034/q051 returned silent-empty wrong answers before (0 rows as success) and now refuse loudly (PR-0). Counting those three as "fixed" is correct — a loud error is the right outcome for an untranslatable pattern.

Beyond the flips, **every pre-existing `ok` query also got materially faster**: the 24 queries that already completed dropped from a **median 2,189 ms → 582 ms (3.8×)**, driven by PR-2's footer-round-trip collapse (per-file footer read 190 ms → 15 µs; see §3) + concurrency raise, and PR-3's star-prune (q001 11.6×). Notable `ok→ok` wins: q001 11.6×, q054 8.9×, q005 8.1×, q006 5.3×, q011 5.2×, q020 5.0×, q035 4.4×.

---

## 1. Full before/after scoreboard (all 54 queries)

`st` = status; `ms` = median wall (DNF shown as its timeout). `flip` names the PR whose mechanism is responsible (attribution from the stack + `05`/`06`/`07`). `—` = no status change (perf-only or unchanged).

| q | before st/ms | after st/ms | rows b→a | flipped/changed by | mechanism |
|---|---|---|---|---|---|
| q001 | ok / 2189 | ok / **189** | 500→500 | PR-3 (11.6×) | star-prune: 7 scans → 2 |
| q002 | ok / 1136 | ok / 639 | 7→**8** | PR-0/0b | `rr:class` `rdf:type` row restored |
| q003 | ok / 310 | ok / 116 | 9→9 | PR-2 | footer collapse |
| q004 | ok / 415 | ok / 403 | 2789 | — | already fast |
| q005 | ok / 3392 | ok / **417** | 20 | PR-2 (8.1×) | footer collapse (dim star) |
| q006 | ok / 2480 | ok / **464** | 3593 | PR-2 (5.3×) | footer collapse |
| q007 | ok / 304 | ok / 129 | 10 | PR-2 | footer collapse |
| **q008** | **dnf** / 180000 | **ok** / 52137 | 0→9 | PR-2 (+PR-4) | scan wall lifted; **residual operator-bound** (§3) |
| **q009** | **dnf** / 180000 | **ok** / 49559 | 0→9 | PR-2 (+PR-4) | as q008 + HAVING |
| **q010** | **dnf** / 180000 | **ok** / 1925 | 0→84 | PR-2 | revenue-by-quarter rollup |
| q011 | ok / 3265 | ok / **628** | 2136 | PR-2 (5.2×) | date pushdown intact (91/7670) |
| **q012** | **dnf** / 180000 | **ok** / 128009 | 0→10 | PR-2 | completes but **scan-bound** (§3) |
| q013 | ok / 2 | **error** / 9 | 0→0 | PR-0/0a | subquery → **loud refuse** (was silent-empty) |
| **q014** | **dnf** / 180000 | **ok** / 411 | 0→4 | PR-2 | channel-mix rollup |
| **q015** | **dnf** / 180000 | **ok** / 2579 | 0→5000 | PR-2 | shipment SLA fact-to-fact |
| **q016** | **dnf** / 180000 | **dnf** / 180000 | 0→0 | *(unfixed)* | object-correlated OPTIONAL star (§2) |
| **q017** | **dnf** / 180000 | **ok** / 3354 | 0→0 | PR-2 | orders-never-shipped (MINUS/negation) |
| **q018** | **dnf** / 180000 | **ok** / 753 | 0→4 | PR-2 | large-payments-by-tender |
| **q019** | **dnf** / 180000 | **ok** / 1146 | 0→0 | PR-2 | GL large debits (decimal filter; still full-scan, no prune) |
| q020 | ok / 3047 | ok / **613** | 1414 | PR-2 (5.0×) | date pushdown intact |
| **q021** | **dnf** / 180000 | **ok** / 920 | 0→0 | PR-2 | GL account-code range |
| q022 | ok / 675 | ok / 493 | 3 | — | fused-agg (1.5× band) |
| q023 | ok / 610 | ok / 436 | 3 | — | fused-agg |
| q024 | ok / 510 | ok / 582 | 44142 | — | noise |
| **q025** | **dnf** / 180000 | **ok** / 5862 | 0→5 | PR-2 | category CSAT HAVING |
| **q026** | **dnf** / 180000 | **ok** / 927 | 0→4 | PR-2 | resolution-by-priority |
| **q027** | **dnf** / 180000 | **ok** / 1630 | 0→18 | PR-2 | web-events by type/device |
| **q028** | **dnf** / 180000 | **ok** / 4538 | 0→5000 | PR-2 | purchase-events + products |
| **q029** | **dnf** / 180000 | **ok** / 141908 | 0→100 | PR-2 | completes but **scan-bound / re-scan** (§3) |
| q030 | ok / 1221 | ok / 611 | 3912 | PR-2 (2.0×) | web-events-in-month (date prune) |
| **q031** | **dnf** / 180000 | **dnf** / 180000 | 0→0 | *(unfixed)* | correlated parent re-scan (§2) |
| **q032** | **dnf** / 180000 | **ok** / 68144 | 0→500 | PR-2 | completes but **operator-bound** (§3) |
| q033 | ok / 5320 | ok / **1812** | 4500 | PR-2 (2.9×) | employee/manager join |
| q034 | ok / 0 | **expected_error** / 1 | 0→0 | PR-0/0a | transitive path → **loud refuse** (was silent-empty) |
| q035 | ok / 3243 | ok / **738** | 14 | PR-2 (4.4×) | sequence path (decomposes to triples) |
| **q036** | **dnf** / 180000 | **ok** / 577 | 0→1 | **PR-1** | bare `COUNT(*)` → manifest shortcut (0 files read) |
| **q037** | **dnf** / 180000 | **ok** / 611 | 0→1 | **PR-1** | bare `COUNT(*)` (WebEvent 1M) → manifest |
| q038 | ok / 54601 | ok / 48472 | 1 | — | filtered COUNT — **not** shortcut-eligible; still scans |
| **q039** | **dnf** / 180000 | **ok** / 555 | 0→1 | **PR-1** | bare `COUNT(*)` (GL) → manifest |
| **q040** | **dnf** / 180000 | **ok** / 1036 | 0→1100 | PR-2 | store-revenue VALUES join |
| **q041** | **dnf** / 180000 | **ok** / 12889 | 0→1100 | PR-2 | store-revenue + filter |
| q042 | ok / 3425 | ok / 1497 | **21→24** | PR-0/0b | `rr:class` rows restored (+2.3× PR-2) |
| q043 | ok / 2125 | ok / 1872 | 0→0 | — | langtag-object expected-error control |
| **q044** | **dnf** / 120000 | **ok** / 680 | 0→0 | PR-2 | customtype-object control |
| q045 | ok / 266 | ok / 102 | 10 | PR-2 (2.6×) | pure LIMIT pushdown (budget works) |
| **q046** | **dnf** / 180000 | **ok** / 445 | 0→10 | PR-2 | ORDER BY+LIMIT: **warm full scan now cheap** (§Memo) |
| **q047** | **dnf** / 180000 | **ok** / 374 | 0→4 | PR-2 | DISTINCT channels + LIMIT |
| **q048** | **dnf** / 180000 | **ok** / 2816 | 0→5000 | PR-2 | CONSTRUCT order→region |
| q049 | ok / 4788 | ok / **1567** | 5000 | PR-2 (3.1×) | CONSTRUCT customer→region |
| **q050** | **dnf** / 120000 | **ok** / 9327 | 0→30000 | **PR-4b** (+PR-3) | OPTIONAL: 407 scans → **92** |
| q051 | ok / 3 | **expected_error** / 0 | 0→0 | PR-0/0a | subquery → **loud refuse** (was silent-empty) |
| **q052** | **dnf** / 180000 | **ok** / 1703 | 0→37 | PR-2 | web-events VALUES + products |
| **q053** | **dnf** / 180000 | **ok** / 13835 | 0→5000 | PR-2 | customers-without-orders NOT EXISTS |
| q054 | ok / 2669 | ok / **299** | 9 | PR-2 (8.9×) | employees-at-store |

**Attribution roll-up.** PR-2 (footer collapse) is the workhorse: **24 of the 28 DNF→ok flips** and the across-the-board `ok→ok` speedup. PR-1 flips the 3 bare-`COUNT(*)` DNFs to sub-second with **zero files read** (manifest shortcut). PR-4b (+PR-3) flips q050. PR-0 converts 3 silent-empty wrong answers to loud refusals (q013/q034/q051) and restores 2 dropped `rdf:type` rows (q002/q042). PR-3 additionally lands the q001 11.6× and feeds q050.

---

## 2. The two residual DNFs

### q031 `inventory_below_reorder` — correlated parent-lookup re-scan on the inner-join path (mechanism UNKNOWN → now KNOWN)

**Shape.** `?inv a InventorySnapshot ; onHandQty ?oh ; reorderPoint ?rp ; product ?p . ?p edw:name ?pn . FILTER(?oh < ?rp) LIMIT 5000`. Intent tag: "var-vs-var FILTER (never pushable) + fact→dim join."

**What the record shows (`36564e8e6`, DNF@180 s):** `r2rml.scan_table` **n = 1306**, `scan_table` total **133.3 s** (dominant), `load_table` n = 7, `iceberg.parquet_read` **n = 8973**, `parquet_read` total 10.2 s, `read_footer` 145 ms total, decode 4.6 s. So **the time is in scan *setup*, not decode** — 133.3 s / 1306 = ~102 ms per scan-setup.

**Scan decomposition (arithmetic, conclusive).** A full `FACT_INVENTORY_SNAPSHOT` scan is 7,670 files (confirmed by q032, whose fact scan is 7,670 of its 7,677 reads). q031's 8,973 reads = **7,668 (one near-complete fact scan, cut at the DNF) + 1,305 single-file re-scans**, and 1 + 1,305 = **1,306 scan setups** — matching `scan_table.n` exactly. So q031 issues **one FactInventorySnapshot scan and ~1,305 correlated single-file re-scans of the DimProduct parent lookup**, each re-scan paying ~102 ms of `loadTable`-context + `scan_plan`. That is where the 180 s goes.

**Mechanism.** This is the **H3 correlated-rebuild** pattern — the same family as q050's 377-scan explosion — but on the **inner-join** path: `?inv edw:product ?p . ?p edw:name ?pn` builds a DimProduct parent lookup (`operator.rs:2036` "Built parent lookup table for RefObjectMap join") and **rebuilds it per driving batch** with no cross-batch memoization. Two co-factors make q031 specifically explode where q032 (same fact→dim join, GROUP BY, only 8 scans) does not:
1. **`LIMIT 5000` drives an incremental batched pull** of the fact rows (vs q032's single full-aggregation pass), and each batch triggers a fresh parent-lookup build.
2. **The var-vs-var `FILTER(?oh < ?rp)` is correctly consumed at the operator level** (q031 *runs* — it DNFs, it does not error, so the filter is converted, not a silent-empty/`error` case), **but it is non-prunable** (a cross-column comparison can never become a file-stats predicate), so nothing reduces the fact row set before the per-batch join — the operator must pull the whole table in batches.

**Why the stack didn't fix it.** PR-4 (parent-memo) is scoped to the within-operator batches of a single `R2rmlScanOperator`; PR-4b admits R2RML leaves only to the **OPTIONAL** batched hash-join. q031's correlation is an **inner join with an interposed non-pushable FILTER + LIMIT**, so neither seam catches it.

**Fix class.** H3 memoization extended to the inner-join / LIMIT-batched-pull seam — either (a) hoist the DimProduct parent lookup out of the per-batch loop (memoize by `LookupCacheKey`, as PR-4 does within its operator, but at the join above the fact scan), or (b) admit this inner-join R2RML correlation to a batched hash-join (PR-4b's sibling for inner joins). Same remedy family as PR-4/PR-4b, different plan seam. Note there is also an F8 co-factor: `load_table.n = 7` for a 2-table query indicates the shared `edw:name` predicate fans `?p edw:name ?pn` out to the name-bearing dims at resolution time; PR-3's class-constrained prune does not engage because there is no co-located `?p a Class` — a widening of PR-3 (or resolving the ref-target class from the RefObjectMap) would drop the fan-out.

### q016 `orders_optional_shipment` — object-correlated OPTIONAL star, outside PR-4b's narrow admission

**Shape.** `?o a Order ; orderId ?oid OPTIONAL { ?sh edw:order ?o ; edw:shipStatus ?st } LIMIT 5000`. Fact-to-fact left join.

**What the record shows (DNF@180 s):** `scan_table` n = **445**, `parquet_read` **n = 3,413,150** (!), fetch 943 s total, footer 55.8 s total. A catastrophic re-scan of `FACT_SHIPMENT` per Order batch — 3.4M file reads.

**Mechanism.** The OPTIONAL inner is a **same-subject star on `?sh`** (`?sh edw:order ?o ; ?sh edw:shipStatus ?st`) whose correlation variable `?o` appears **only as an object** (`?sh edw:order ?o`). PR-4b (as shipped) took the *narrow* admission — subject-driven scalar-POM + single-valued RefObjectMap, i.e. exactly q050. q016's inner is neither: it is a **multi-predicate star** with an **object-only correlation**. Per `07-pr4b-batched-optional.md` (shape table + open questions 1–2), the object-only correlation is "SOUND but UNoptimized" (`corr_var_only_triple_object` is Triple-only, doesn't recognize an R2RML object corr var) and the star inner is on the deferred "widen later" list. So q016 stays on the per-row `OptionalBuilder::build` rebuild path → the 445-scan / 3.4M-read fact-to-fact explosion.

**Fix class.** Widen PR-4b's admission exactly as `07` open questions 1 & 2 describe: (1) admit same-subject R2RML star inners to `inner_pattern_is_hash_join_safe`; (2) extend `corr_var_only_triple_object` to recognize an R2RML object-only correlation so `?sh edw:order ?o` seeds `?o`-bound and hash-partitions instead of per-row rebuilding. The `07` soundness prerequisite P1 (`R2rmlPattern::referenced_vars` completeness) gates both.

---

## 3. Slow-tail mechanism classification (q012, q029, q032, q008, q009)

All five *complete* now (PR-2), but their walls are 49–142 s. The classification uses the run-record span counters. The key enabler to read first: **PR-2 collapsed the per-file footer read from ~190 ms to ~15 µs — footer is now trivial on every query** (`read_footer` µs/file: q008 21, q012 15, q029 15, q032 15). So the residual walls are *not* footer-bound; they split three ways:

| q | shape | scan_n | pqrd_n | pqrd µs/file | wall | **class** | dominant cost |
|---|---|---:|---:|---:|---:|---|---|
| **q008** | Order⋈Cust⋈Geo `GROUP BY region SUM` | 8 | 7677 | **580** | 52 s | **operator-bound** | scan is warm-cheap (~0.5 s wall-equiv); ~51 s is the fact⋈dim hash-join + GROUP BY materializing 180 K FactOrder bindings — the **H6 fused-agg-over-join** case, now visible because PR-2 lifted the H1 decode wall that used to hide it |
| **q009** | as q008 `+ HAVING` | 8 | 7677 | 642 | 50 s | **operator-bound** | identical to q008 |
| **q032** | InvSnapshot⋈Store `GROUP BY store SUM` | 8 | 7677 | 932 | 68 s | **operator-bound** (+load floor) | same H6 class; **plus ~9 s of 7× `loadTable`** cold-catalog floor (`load_table` total 8.8 s) |
| **q012** | OrderLine⋈Product `GROUP BY … ORDER BY DESC LIMIT 10` | 4 | 7673 | **43,729** | 128 s | **scan-bound** | the per-file `parquet_read` is **43.7 ms** but footer 15 µs + fetch 617 µs + decode ≈ trivial — i.e. ~43 ms/file lives **inside `parquet_read` but outside the measured sub-spans**. Most likely uncaptured cold **sparse column-chunk S3 reads** on the wide OrderLine table (projected 3 of N columns → multiple range GETs the whole-file `fetch_bytes` span doesn't wrap) and/or operator back-pressure from the top-k consumer. ORDER BY+LIMIT absorbs the budget (H2) so there is no early termination |
| **q029** | UNION(WebEvent="purchase", ="add_to_cart") `LIMIT 100` | **253** | **1,940,510** | 230 | 142 s | **scan-bound / re-scan-amplified** | 1.94 M file reads for a 100-row LIMIT: the UNION absorbs the LIMIT budget (H2) and the two branch scans of `FACT_WEB_EVENT` are **re-driven ~253 times** (files_selected 15,340 ≈ 2×7,670; reads ≈ 253× that). The `eventType` string-equality doesn't prune (WebEvent isn't partitioned by it) |

**Two remedy buckets fall out:**
- **Operator-bound rollups (q008, q009, q032):** the residual is the join+GROUP-BY materialization → **PR-6 (fused-aggregate over a single RefObjectMap join)** is the direct fix; q032 additionally wants PR-8's cold-catalog reduction for its 9 s load floor.
- **Budget-absorb scans (q012 top-k, q029 UNION):** the residual is a full/repeated fact scan the LIMIT budget can't shrink → **PR-5 (scan-side top-k with file pruning)** for q012 and **budget propagation through UNION** for q029. Note both are *warm-cheap when the fact files are cached* (cf. q046, a full 7,670-file ORDER BY scan that lands in **445 ms** warm) — so PR-2's warm path already mitigates them; PR-5/UNION-budget are the *cold* and *selective-prune* wins.

**Caching-variance caveat (important for reading these numbers).** The 54-query `hot` run cannot keep every fact table's files resident — the disk cache thrashes at fact-table scale. So whether a given fact query reads warm (~0.5 ms/file, q008) or cold-ish (~44 ms/file, q012) in a single run is partly **run-order / eviction dependent**, not intrinsic to the query. The *classification* is robust (q008's residual is the operator regardless of scan warmth; q012's is the scan path), but the absolute tail walls carry ±cache-state noise — the memo sizes prizes on mechanism, not on a single run's ms.

---

## 4. q013 corpus bookkeeping — FIXED

q013 (`products_above_avg_units`, a subquery — same root as q051) errored at 9 ms under PR-0's loud-refuse but the manifest lacked the `expected_status` that q034/q051 carry, so the corpus scored it as an unexpected `error`. Fixed to mirror q034/q051:
- `corpus/manifest.json` q013 entry: added `"expected_status": { "native": "ok", "virtual": "error" }`.
- `corpus/queries/q013_products_above_avg_units.rq` header: added the PR-0/0a note ("a subquery inside a GRAPH-over-R2RML scope now ERRORS loudly on virtual targets … expected_status = {native: ok, virtual: error}"), mirroring q051.

(Manifest re-validated as JSON, 54 queries intact.)

---

## 5. Cold-subset re-baseline (current cold numbers)

`exec-one --cold` per query (fresh process + cleared disk artifact cache = every read hits S3, empty OAuth/catalog cache), one at a time, 2 s paced, git `36564e8e6`. Cold `wall` beside the warm re-baseline wall from §1.

| q | shape | **cold wall** | warm wall | cold `loadTable` | cold scan setup | files read | reads-zero-data? |
|---|---|---:|---:|---|---:|---:|---|
| q001 | Store dim star | **2,249 ms** | 189 ms | 1× 1.6 s | 2.2 s | 2 | no |
| q008 | revenue⋈dim `GROUP BY` | **100,306 ms** | 52,137 ms | **6× 8.1 s** | 11.1 s | 7,677 | no |
| q019 | GL large debits (decimal filter) | **41,454 ms** | 1,146 ms | 1× 1.9 s | 2.7 s | 7,670 | no |
| q027 | web-events by type/device | **42,679 ms** | 1,630 ms | 1× 2.0 s | 3.0 s | 7,670 | no |
| q036 | bare `COUNT(*)` orders | **2,847 ms** | 577 ms | 1× 2.1 s | 0 | **0** | **yes (manifest)** |
| q046 | orders top-k `ORDER BY LIMIT` | **37,159 ms** | 445 ms | 1× 2.0 s | 2.7 s | 7,670 | no |

**Cold findings that size the memo:**

1. **The cold `loadTable` floor is ~1.6–2.1 s *per table*** (OAuth + catalog), and PR-2 did **not** touch it (PR-2 fixed the warm *footer* read, not the catalog). This is PR-8's exact target. For q036 (bare COUNT, **zero data files read** — PR-1's manifest shortcut holds cold) the *entire* 2.8 s cold wall **is** this floor: PR-8 alone would take cold COUNT to sub-second.

2. **PR-1 and PR-3 hold up cold.** Cold q001 is **2.2 s vs the pre-stack 20.8 s** (`04 §F8`, 6 cold `loadTable`s + fan-out) — a **9.4× cold win** from PR-3 collapsing 6 loadTables to 1, plus PR-2. Cold bare-COUNT stays sub-3 s because it reads no files.

3. **The cold-only PR-5 / PR-7 case, now sized.** A cold full 7,670-file fact scan is **~37–43 s** (q046 top-k 37 s, q019 decimal-filter 41 s, q027 WebEvent 43 s) versus **sub-second to ~1.6 s warm** for the same queries. This is precisely where PR-5 (file-pruning-by-running-k) and PR-7 (decimal/double stats prune) would read **≪ 7,670 files** — the entire win is on the cold column, ~35–43 s → seconds. It is also **gated by PR-8**: if PR-8's persistent-catalog + warm-pool work makes cold scans read from a warmed disk cache, the scan-side pruning is a smaller marginal win.

4. **The operator-bound rollup is cold-heavy.** Cold q008 is **100 s** (6× `loadTable` = 8.1 s floor + the join/GROUP-BY materialization now also paying cold decode). PR-6 (fused agg, removes the materialization) and PR-8 (removes the 8 s floor) **compound** here — this is the strongest argument for sequencing PR-6 and PR-8 adjacent.

---

## 6. THE MEMO — remaining prizes, sizing, and recommended tail order (for AJ's call)

The stack converted the virtual path from **"30 of 54 DNF"** to **"49 of 54 ok, and the always-ok set 3.8× faster."** The correctness floor is clean (silent-empty → loud refuse; `rdf:type` restored). What remains is a **short, well-characterized tail**, and — critically — the single biggest lever left is **not** any one query but the **cold floor** that sits under all of them. Sizing the prizes:

### The remaining prizes, sized

| Prize | Queries it moves | Size of the win | Risk / surface | Notes |
|---|---|---|---|---|
| **A. PR-6 fused-agg-over-join** | q008, q009, q032, q010, q025 (rollup class) | Large: turns 50–68 s operator-bound rollups into the fused-column path (the q022 single-table fused agg is already 1.5× native). This is the **most common BI shape** (revenue/qty by dimension). | MED–HIGH (agg correctness over a join). Kill-switch `FLUREE_FUSED_R2RML_AGG` exists. | The §3 diagnosis makes this the **highest-value engine item** — 3+ of the 5 slow-tail queries are exactly this class, and it was *invisible until PR-2*, exactly as `05` predicted. |
| **B. q031 inner-join parent-memo** | q031 (dnf→ok) + any correlated inner FK join under LIMIT | Medium: one DNF→ok, and closes the H3 hole on the inner-join seam (PR-4/PR-4b closed the operator + OPTIONAL seams). | LOW–MED — a cache-lifetime extension of PR-4's `LookupCacheKey`, not a semantics change. | Cheapest structural win. Pairs naturally with PR-4 (same cache). |
| **C. q016 PR-4b widening** | q016 (dnf→ok) + object-correlated / star OPTIONAL inners | Medium: one DNF→ok; completes PR-4b's deferred "widen later." | MED–HIGH — join-semantics + the `07` P1 (`referenced_vars` completeness) soundness gate. | Design already written (`07` open Qs 1–2). Do the P1 audit first regardless — it is a latent hazard for *any* correlation optimization. |
| **D. PR-5 scan-side top-k + UNION budget** | q012, q029, q046-cold, q005-class | Medium, **cold-only** (see below): warm, these already complete (q046 445 ms; q012/q029 are cache-eviction cold in the 54-run). Real win is on *cold* + *selective* ORDER BY/UNION where file-pruning-by-k reads ≪ 7,670 files. | MED–HIGH (largest new design surface; pruning-bound soundness). | **PR-5's heap-only leg is worthless warm** (it still decodes every file); its only value is the running-k-th-bound **file/row-group prune, which needs numeric column stats ⇒ PR-5 depends on PR-7.** Treat PR-5+PR-7 as one unit. Gated by the cold floor (PR-8). |
| **E. PR-8 cold/floor program** | **every query's cold column** | **Largest fleet-wide lever.** Cold `loadTable` (OAuth + catalog, ~2 s each) and cold sparse S3 reads dominate first-touch latency; PR-2 already fixed the *warm* footer, so the floor is now the **dominant cold cost**. Persistent catalog state + 429 backoff makes every cold query durably faster and de-risks PR-2/PR-3 fan-out against Snowflake Horizon 429s. | LOW–MED (cache-persistence correctness; honor creds expiry). | The cold-subset numbers in §5 size this precisely. |
| **F. PR-F9 formatter parity** | q002, q042 (hash-red) | Small: closes the last honest parity reds (native CURIE-compaction vs virtual full-IRI). **AJ has decided:** virtual aligns to native CURIE-compaction. | LOW (a result-formatter change). | Purely a correctness/parity tidy; no perf. Small, do it whenever. |
| **G. PR-7 decimal/double pushdown** | q019 (and the H4 A/B) | Small–Medium, **cold-only** overlap with D: brings decimal money-column filters to the date-column's 98.8%-prune parity; warm it's already sub-second (q019 1.1 s). | LOW–MED. | Bundles with PR-5 as "scan-side selectivity," cold-valued. |

### Recommended tail order (rationale, for AJ to accept or reorder)

1. **PR-F9 formatter parity (F)** — *first, because it is nearly free and it un-reds the parity gate.* AJ has already made the call (virtual → native CURIE compaction); it is a small formatter change with no perf risk, and it removes the last two honest hash-red queries so every subsequent PR runs against an all-green parity baseline. Clears the board.

2. **PR-6 fused-agg-over-join (A)** — *the highest-value engine work.* The §3 diagnosis is unambiguous: with the decode wall gone, the rollup class (q008/q009/q032/q010/q025) is now operator-bound, and this is the dominant real-world BI shape. It was correctly deferred behind PR-2 in the ROADMAP; PR-2 has landed, so its impact is now measurable and its gate (the corpus rollup queries) is ready.

3. **PR-8 cold/floor program (E)** — *the biggest fleet-wide multiplier, and a prerequisite for safely pushing scan concurrency further.* PR-2 fixed the warm path; the cold floor (loadTable OAuth/catalog + cold sparse reads, sized in §5) is now the dominant first-touch cost and the thing customers feel on every fresh query. Its 429-backoff half also **de-risks** PR-3's crawl and any future concurrency raise against Snowflake Horizon. High value, low-medium risk, and it makes the cold-only prizes (D, G) actually pay off.

4. **q031 inner-join parent-memo (B)** — *cheap structural win, closes the last H3 seam.* One DNF→ok, low risk (extends PR-4's existing cache to a new seam), and it removes a genuinely pathological shape (1,305 redundant parent re-scans). Small enough to ride alongside PR-6 or PR-8.

5. **q016 PR-4b widening (C)** — *the second DNF→ok, but do the P1 audit first.* The design exists (`07`); the gate is the `referenced_vars` completeness audit, which should be done as a standalone precursor anyway (it is a latent soundness hazard for B, C, and any correlation optimization). Sequence after B so the two H3-seam fixes land together.

6. **PR-7 + PR-5 as one cold-scoped unit (G+D)** — *last, and only after PR-8's numbers decide it.* These must be sequenced together and PR-7-first: **PR-5's heap-only leg is worthless warm (still decodes every file); its win is the running-k-th-bound file/row-group prune, which needs the numeric column stats PR-7 adds — so PR-5 depends on PR-7.** The prior finding holds and is now sized: warm these already complete (q046 445 ms; q019 1.1 s), so the win is **cold-only** — the cold full 7,670-file scans measured at **37–43 s** (§5) that pruning would cut to reading ≪ 7,670 files. That value is **gated by PR-8**: if PR-8's persistent-catalog/warm-pool work makes cold scans read warmed bytes, the marginal pruning win shrinks. **Defer the PR-7+PR-5 go/no-go until PR-8's cold numbers are in** — they tell us whether ~37–43 s cold scans still exist to prune. (This sharpens the prior "warm q046 is already 445–793 ms, so cold-only value" note: cold-only, PR-7-gated, *and* PR-8-gated.)

**One-line recommendation to AJ:** ship **F (parity tidy) → PR-6 (rollup, the big BI win) → PR-8 (cold floor, the fleet multiplier) → B+C (the two H3 DNFs, P1 audit first) → then decide PR-7+PR-5 (one cold-scoped unit, PR-7-first) on PR-8's cold numbers.** Rationale: value-first within risk, correctness/parity cleared up front, the two remaining DNFs are cheap-ish and bounded, and the highest-design-surface item (PR-5, which depends on PR-7) is deferred behind the measurement (PR-8) that determines whether ~37–43 s cold scans still exist to prune.

---

## 7. Reproduction & artifacts

- Re-baseline: `results/runs/virtual-sf01-stacked-rebase-20260713T214121Z.jsonl` (git `36564e8e6`, `run --cache-state hot`). Filter records with `jq 'select(.query_id)'` (line 1 is a `kind:"meta"` header — `04 §F10`).
- Original baseline: the four `results/runs/virtual-sf01-full*.jsonl` (q001-018 / resume q019-036 / resume2 q037-040 / resume3 q041-054).
- q031 live probe: `~/vbench/pf5b_q031_trace.*` (90 s bounded; note vbench installs only the span-capture layer — no fmt/stderr layer — so `RUST_LOG` yields no event lines; the scan decomposition above is from the durable span counters + the fact-file-count arithmetic, which is conclusive).
- Cold subset: `~/vbench/pf5b_cold_subset.jsonl` (§5), `exec-one --cold`, one query at a time, 2 s paced.
- Corpus bookkeeping edits (uncommitted): `corpus/manifest.json` (q013 `expected_status`), `corpus/queries/q013_products_above_avg_units.rq` (header note).
