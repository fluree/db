# PR-8-TAIL (F18 cold-floor) — q031 loadTable-pin + fact-residency — DESIGN SCOPE

**Branch:** `fix/f18-q031-memo-limit` (off `fix/f9-virtual-curie`, #1499 head e1ac1317f)
**Status:** DESIGN SCOPE (doc only) — **STOP for lead review**. No engine code until approved. **⚠️ L1 PREMISE REFUTED BY MEASUREMENT 2026-07-14 — see the MEASUREMENT ADDENDUM at the bottom before reading §a-§e.** The "loadTable/creds pin LEAK" hypothesis is wrong: q031 loads 7 DISTINCT tables once each (pin held perfectly across 72 s > the 60 s TTL). The 21.2 s is a **resolution FAN-OUT** (`?p edw:name ?pn` resolves against all 6 name-bearing dims), not a re-load storm. The real q031 lever is a **RefObjectMap-target resolution prune** (F8/PR-3 family), NOT a pin. §a-§e below are retained as the (now-superseded) reasoning trail.
**North-star item 1**, RE-SCOPED per the lead's ruling (2026-07-14, Option 1): F18 is a **cold-floor** item framed as **the PR-8 tail**. The memo non-engagement is split out as **F19** (own entry, standalone, low priority). This doc is the design scope; `17-pr-f18-q031-memo-limit.md` is the investigation record that forced the re-frame (its REVISED CONCLUSION → this).

Target (honest, pending open-item 2 below): q031 **72 s cache-thrashed → single-digit s** (≤~3 s if the one-time full fact read fits the bar; see the arithmetic).

---

## (a) The 72 s, decomposed against levers

Clean re-baseline span totals for q031 (`pf_rebaseline_1499.jsonl`, the winning hot rep, still 72.07 s — this is NOT a cold-start-only artifact, it is the cache-thrashed steady state):

| span | n | total_us | lever |
|---|---:|---:|---|
| `r2rml.load_table` | 7 | 21,221,292 | **L1** loadTable/creds pin |
| `iceberg.oauth_token` | 5 | 3,170,172 | **L1** (coupled re-auth) |
| `iceberg.parquet_read` | 9112 | 9,776,455 | **L2** data residency |
| `iceberg.fetch_bytes` | 9112 | 4,221,087 | **L2** |
| `iceberg.decode` | 9112 | 5,105,006 | **L2** / materialize |
| `iceberg.prefetch` | 242 | 3,325,709 | **L2** |
| `iceberg.read_footer` | 9112 | 160,751 | L2 (small) |
| `r2rml.scan_table` | 1448 | 7,519,607 | **L3** (F19 / shape — out of this PR) |

**Non-additive caveat.** These overlap and nest: `parquet_read` wraps `fetch_bytes`/`read_footer`/`decode`; the 9112 reads run concurrently across reader workers, so their `total_us` exceeds their wall contribution. `load_table` (21.2 s) is the one span that is both large AND serial-ish (each catalog GET blocks the scan that needs it), which is why it dominates the wall despite a smaller `total_us` than `parquet_read`.

**L1 — loadTable/creds (≈ 21.2 s + 3.2 s oauth ≈ 24 s).** q031 issues **7 `loadTable` GETs inside one 72 s query**. That is the anomaly: q031 touches essentially two physical tables (the `FACT_INVENTORY_SNAPSHOT` star + `DIM_PRODUCT` via `edw:product`/`edw:name`). A per-query pin already exists (`r2rml.rs:1130-1196`: `self.session.cached_load_table(&lt_key)` → "loadTable pin hit (query-scoped)"; `pinned_metadata_location` keeps the first-resolved snapshot across creds refreshes) and the code comment asserts "an in-flight query pins its own snapshot regardless" (`cache.rs:37`). The 60 s value (`DEFAULT_REST_LOADTABLE_TTL_SECS`, `cache.rs:42`) is only the **cross-query** layer. So the 7 GETs mean the **per-query pin is leaking on the correlated re-scan path** — the same structural root as F19 (per-driving-batch state re-creation drops the `IcebergCatalogSession` pin just as it drops the `ExecutionContext` memo). Removing the ~5 redundant GETs is the single biggest, cleanest lever.

**L2 — fact/dim data residency (parquet reads).** The 9112 `parquet_read`/`fetch_bytes`/`decode` operations are the cold/evicted re-reads of the fact + dim files. PR-8 slice 2 (`disk_catalog_cache.rs`) already persists the SECRET-FREE metadata + manifest layers across restarts (content-addressed by `metadata_location`), so it removes the metadata/manifest S3 round-trips — but **not** the parquet *data* reads and **not** the vended-creds `loadTable` GET. Under a full-corpus sweep the moka byte cache + parquet-footer cache evict the fact working set, so these re-fetch. L2 is "keep the fact/dim files resident across the query (and ideally the sweep)."

**L3 — scan/materialize (out of this PR).** `scan_table` n=1448 (the 1+1447 fact+dim-re-scan shape) is ~7.5 s of setup, and ~18 s more is generic-eval materialization of a near-full `FACT_INVENTORY_SNAPSHOT` (the `FILTER(?oh<?rp)` is a two-column comparison — un-prunable and non-selective at the file level, so the `LIMIT 5000` cannot cut the fact scan). The 1447 dim re-scans are **near-free in wall** (72 s budget ≈ 76 s no-budget) but they DO generate cache pressure that feeds L2. F19 removes them; the materialization is intrinsic to the shape. **Neither is this PR** — L3 is flagged so the arithmetic below does not over-credit L1+L2.

**Why warm was 188 ms, and whether that cache survives corpus pressure.** Fully warm (the PR-8b reps=3 phase), q031 is 188 ms: the query completes well under the 60 s TTL so **no pin expires and no re-load fires** (L1 = 0), the moka byte cache + OS page cache + parquet-footer cache hold every fact/dim file so the 1448 re-scans and 9112 reads all hit cache (L2 = 0), and what remains is the pure CPU join + `FILTER` + `LIMIT` on resident decoded batches. **That warm state does NOT survive corpus pressure, on three independent axes:** (1) the 60 s `loadTable` TTL expires on ANY query whose own runtime exceeds 60 s (q031 at 72 s expires mid-flight → the re-load storm); (2) the moka byte/footer caches evict the fact working set under a 54-query sweep (the `09` caching-variance caveat); (3) the disk catalog cache survives *restart* but only for metadata/manifest — it never holds the parquet data or the creds GET. So "188 ms" is unreachable in any realistic multi-query or long-single-query regime — it is precisely the ~350× warm-sentinel trap. The cold-floor levers are what make the *thrashed* number approach the warm one.

---

## (b) Arithmetic to the bar (≤~3 s), required vs. nice

Frame: the achievable **cache-thrashed floor** is "pay each physical resource exactly once." q031 currently pays `loadTable` 7×, the dim scan 1448×, and the fact/dim parquet many× under eviction. Pay-once = one `loadTable` per distinct table (held for the query) + one fact scan + one dim scan + materialize — which is the **q028 class**: q028 is the only sibling correlated dim-join in the corpus and runs **3.9 s** at `scan_table.n=13`. So q028 is the empirical floor this shape can reach.

| step | lever | required? | q031 wall |
|---|---|---|---:|
| baseline (thrashed) | — | — | 72 s |
| hold the loadTable/creds pin for the query lifetime (7 GETs → ~2) | **L1** | **REQUIRED** (biggest chunk; also kills the positive-feedback loop where the slow query outlives the TTL and re-loads, making itself slower) | → ~48 s |
| keep fact/dim files resident so the 1448 re-scans + reads hit cache instead of re-fetching | **L2** | **REQUIRED** to reach single-digit | → single-digit s |
| remove the 1447 dim re-scans entirely | L3 / **F19** | nice (wall-neutral now; reduces L2 pressure) | marginal |
| one-time full fact read (un-prunable FILTER ⇒ no LIMIT cut) | intrinsic | floor | residual |

**Required vs. nice:** L1 alone gets 72 → ~48 s but does NOT reach the bar. L1 **and** L2 are both required to approach the q028-class floor. F19 and the materialization (L3) are NOT required for the wall (F19 is wall-neutral today; the materialization is intrinsic) — so this PR is **L1 + L2 only**.

**The honesty caveat that feeds AJ's bar question.** The `FILTER(?oh<?rp)` is un-prunable, so the `LIMIT 5000` cannot cut the fact scan — the fact table is read in full every time. If `FACT_INVENTORY_SNAPSHOT` is large enough that ONE full cold read exceeds 3 s, then q031's cache-thrashed floor is **bounded by the fact-table size, not by the pins**, and the honest end-state is "single-digit s, dominated by one full FACT read," not "≤3 s." That is exactly the q001-class one-time-cold cost. **Open-item 2 (measure the fact row/byte count) decides which claim we can make** — and it is the concrete input to the lead's bar-semantics question with AJ (is the bar "≤3 s second-touch / warm-catalog," or "≤3 s from a genuinely cold first-ever touch"? q031 can meet the former; the latter is fact-size-bound).

---

## (c) Staleness / correctness of extending the pin

Extending the pin is **correctness-neutral by construction** — it removes redundant re-loads of the SAME snapshot within ONE query; it never widens what a query observes.

- **as_of_t / single-snapshot.** A virtual query already reads one consistent Iceberg snapshot; the per-query pin is snapshot-pinned (`pinned_metadata_location` keeps the first-resolved `metadata_location` across creds refreshes, `r2rml.rs:1185-1190`). Making the pin durable for the query's full lifetime **strengthens** the existing "in-flight query pins its own snapshot regardless" contract (`cache.rs:37`) — it does not weaken it. The invariant to preserve: a query must never observe two snapshots of the same table; the fix makes that guarantee durable, not looser.
- **Vended-cred expiry.** Snowflake Polaris vended creds are bucket/prefix-scoped and expire independently (~1 h). The pin must hold the **snapshot metadata** (location + manifest-derived file list) but keep **creds refreshable**: the code already does exactly this (`r2rml.rs:1180-1190` — a reload is "a creds refresh that must keep the pinned snapshot"). So L1 pins the metadata, not the credential. The design cost is that a creds refresh must NOT re-pay the full metadata GET — separating those two is open-item 1(b).
- **Invalidation on snapshot change (content-addressing model).** PR-8 slice 2's disk cache is keyed by `metadata_location`, an S3 content-addressed path: "a table commit yields a NEW location = a NEW key = a clean miss, no TTL or invalidation logic needed" (`disk_catalog_cache.rs:4-10`). Model the query-lifetime pin identically — key by `(table, metadata_location)`. Within a query the location is fixed (pinned) → no invalidation. Across queries the existing 60 s cross-query TTL already serves the newest snapshot to NEW queries. So the pin extension adds no new staleness surface: it only elides re-loads of the already-pinned location.

---

## (d) Which other tail/corpus queries benefit (quantified)

From `pf_rebaseline_1499.jsonl` (hot reps), `r2rml.load_table` fires (`lt_n>0`) on exactly four queries — the four longest tail entries, because each outlives the 60 s TTL and re-loads mid-query:

| q | lt_n | load_table s | wall s | share | dominant lever (this query) |
|---|---:|---:|---:|---:|---|
| **q031** | 7 | 21.2 | 72.1 | **29%** | **L1 (this PR)** |
| q016 | 2 | 4.3 | 43.1 | 10% | F14 batched-OPTIONAL (L1 secondary) |
| q038 | 2 | 3.6 | 48.6 | 7% | un-fused COUNT / generic-eval (L1 secondary) |
| q029 | 1 | 1.9 | 125.1 | 1.5% | F17 UNION re-drive (L1 negligible) |

**L1 is q031-specific in WALL impact but universally correct** — any query that runs > 60 s re-loads on any shape, so the pin is the right fix everywhere; it just only *dominates* q031. Critically, the **42/50 already-≤3 s** queries have `lt_n=0` hot (their `loadTable` is fully cached) — they never re-load, so L1 cannot regress them (the pin only ADDS a hold; a query that already loads once behaves as if pinned). L1 is strictly a tail lever with zero fast-set risk.

**L2 (residency)** benefits the read-bound tail — `parquet_read` concurrent totals: q029 (511 s), q016 (248 s), q017 (26.7 s), q028 (24 s), q015 (18.7 s), q041 (9.6 s), q040 (7.8 s), q053 (6.3 s). But L2's per-query benefit is **entangled** with the re-drive fixes (F14/F17 reduce the read COUNT; L2 reduces the per-read COST), and is bounded by working-set-vs-cache-size. Honest statement: **L1 is the q031 lever; L2 is a corpus-wide cold-floor improvement whose per-query benefit must be measured, not promised** (open-item 3 — don't double-count against F14/F17).

---

## (e) Kill switch + gate design

**Kill switch.** Add `FLUREE_ICEBERG_LOADTABLE_QUERY_PIN` (default on); off → today's behavior (per-query pin as-is, TTL-gated re-loads). Prefer a dedicated pin switch over overloading `FLUREE_ICEBERG_LOADTABLE_TTL_SECS` so the cross-query TTL stays independently tunable. L2 residency rides existing switches (`FLUREE_ICEBERG_CATALOG_DISK_CACHE` for the catalog layer; the byte/footer cache caps).

**Gate — the cache-thrashed protocol now HELPS (inverts the ~350× trap).** A fully-warm gate shows 188 ms and proves NOTHING (the pins are irrelevant under 60 s with everything resident). The cold-floor gate MUST run q031 **cache-thrashed** — full-corpus preceding order, or the PR-8 slice-2 "cold-data / warm-catalog" state (clear the data artifact dir, keep catalog persistence) — so the TTL expiry and the eviction are actually exercised. This is the first gate where the thrashed protocol is the *point*, not a caveat.

- **Deterministic sentinels (counter, not wall).** q031 `load_table.n` MUST drop **7 → ≤2** (one per distinct table, held for the query) and `oauth_token.n` drop; these `n` counts are cache-independent and are the crisp proof. Wall (72 s → single-digit) is the headline but is cache-variance-sensitive, so gate on `n` and report wall.
- **Co-benefit sentinels.** q016/q038 `load_table.n` should drop **2 → 1** — include them to show L1 generalizes, but assert their `load_table.n` (not their wall — their wall is F14/F17-bound, not L1).
- **Full-corpus cache-thrashed baseline at the PR head** (new protocol: priming + 3-rep, per-query manifest `timeout_s`): no fast query regresses in wall or hash; the 42/50 ≤3 s set stays ≤3 s.
- **Hermetic test.** Drive N correlated re-scans (or simulate a > TTL query) and assert `loadTable` is issued **once per `(table, snapshot)` for the query's lifetime**. Mirrors the F19 hermetic discipline but for the session pin, not the memo. Since F19 and L1 share the per-driving-batch-rebuild root, a combined harness may exercise both — but they SHIP separately (no scope-mix, per the lead).

---

## Open items for implementation (trace-first — mirrors doc 17's discipline; NO engine code pending approval)

1. **Why n=7.** Confirm the exact site that drops the per-query pin on the correlated re-scan path (the `r2rml.rs:1134` `session.cached_load_table` miss). Candidates: (a) operator/`IcebergCatalogSession` rebuild per driving batch (shared root with F19 → the fix is a durable session/pin across rebuilds), or (b) the creds-refresh path (`r2rml.rs:1180`) issuing a real metadata GET (counted in the span) when only creds needed refreshing → the fix separates the creds re-vend from the metadata pin. The lane differs by which it is.
2. **Measure `FACT_INVENTORY_SNAPSHOT` row/byte count** → decide whether one full cold fact read fits ≤3 s (the un-prunable FILTER means no LIMIT cut). If not, the honest end-state is "single-digit s, fact-read-bound" — the direct input to AJ's bar-semantics question.
3. **Separate L2's residency benefit from F14/F17 read-count reductions** so the corpus-wide claim isn't double-counted.

## DoD (proposed — lead to fix)

1. q031 cache-thrashed ≤ single-digit s (≤3 s pending open-item 2), with `load_table.n` **7 → ≤2** as the deterministic proof (not a warm-cache wall artifact — measured in full-corpus / cold-data-warm-catalog order).
2. Full-corpus cache-thrashed baseline at head: no wall/hash regression; the 42/50 ≤3 s set stays ≤3 s.
3. Hermetic loadTable-pin test (once per `(table, snapshot)` per query lifetime).
4. Native untouched (r2rml / graph-source only; native never instantiates the R2RML scan path). Kill switch off = byte-identical + reverts to current re-load behavior.

**STOP — design scope for review.** Open questions for the lead: (i) L1 + L2 scope as one PR, or L1 first (q031's dominant lever, cleanest counter proof) with L2 as a follow-up given its entanglement with F14/F17? (ii) the bar-semantics dependency on open-item 2 (fact-read floor) — does that go to AJ now with the fact-size measurement, or after? No engine code pending your call.

---

## MEASUREMENT ADDENDUM (2026-07-14) — open-items 1 & 2 resolved; L1 premise REFUTED

Ran the two open-item measurements before writing the L1 code addendum. Both landed, and together they **overturn the L1 "loadTable pin leak" hypothesis** and correct the q031 fix.

### Open-item 2 — table sizes (authoritative, from the content-addressed disk catalog `metadata.json` snapshot summaries; zero live-credential persist)

| table | rows | data-files | bytes | rows/file | KB/file |
|---|---:|---:|---:|---:|---:|
| **FACT_INVENTORY_SNAPSHOT** | 300,000 | **7,670** | 51.1 MB | 39.1 | 6.5 |
| DIM_PRODUCT | 37,500 | **1** | 1.0 MB | 37,500 | 997 |
| (ref: FACT_ORDER) | 180,000 | 7,670 | 54.6 MB | 23.5 | 7.0 |
| (ref: FACT_SHIPMENT) | 180,000 | 7,670 | 45.1 MB | 23.5 | 5.7 |

**Verdict: q031's fact-read floor is FILE-COUNT-BOUND, not byte-bound.** 51 MB fits any cache trivially — L2-as-"raise the 512 MiB cap" is a non-issue (the whole fact table is 51 MB). The cost is the **7,670 tiny files** (6.5 KB, 39 rows each) — the canonical decode-wall layout the ROADMAP master-finding calls out (~39 files/s ⇒ ~197 s cold-S3). The un-prunable `FILTER(?oh<?rp)` means the `LIMIT 5000` cannot cut this scan → one full read touches all 7,670 files. So the residual floor after every other fix is **one 7,670-file FACT_INVENTORY_SNAPSHOT decode**, which is **PR-2a territory (per-file decode overhead), not cache-residency**.

### Open-item 1 — why `load_table.n = 7` (the leak test) → NOT A LEAK

Ran q031 against a FRESH catalog cache and enumerated the distinct tables loaded. Result: **7 DISTINCT tables, each loaded EXACTLY ONCE** — `FACT_INVENTORY_SNAPSHOT` + `DIM_ACCOUNT, DIM_CUSTOMER, DIM_EMPLOYEE, DIM_PRODUCT, DIM_STORE, DIM_SUPPLIER`. (wall 114.8 s cold, `load_table.n=7`.)

- **The per-query pin is WORKING AS DESIGNED.** Each table loaded once; the pin held across the full 72–115 s query despite the 60 s cross-query TTL (`cache.rs:37`'s "in-flight query pins its own snapshot regardless" contract holds). **There is no re-load storm.** L1 as scoped (make the pin durable) fixes nothing — the pin is already durable.
- **The 7 loads are a RESOLUTION FAN-OUT.** The 6 dims loaded are EXACTLY the 6 that map `edw:name` (`DIM_SUPPLIER`=SUPPLIER_NAME, `DIM_ACCOUNT`=ACCOUNT_NAME, `DIM_EMPLOYEE`=FULL_NAME, `DIM_STORE`=STORE_NAME, `DIM_CUSTOMER`=FULL_NAME, `DIM_PRODUCT`=PRODUCT_NAME). q031's second triple `?p edw:name ?pn` is a **variable-subject single-predicate pattern on a shared base predicate**, so TriplesMap resolution fans out to every `edw:name`-bearing map — even though `?p` is bound by `edw:product` (a RefObjectMap whose parent is provably `DIM_PRODUCT`). **The RefObjectMap's target class is not propagated to constrain the downstream `?p edw:name` resolution.**

### Corrected q031 fix — a RESOLUTION-PRUNE (F8/PR-3 / ref-target family), NOT a loadTable-pin

- **Primary lever (deterministic): propagate the RefObjectMap parent (`DIM_PRODUCT`, from `?p ← edw:product`) to prune the resolution of downstream patterns on `?p`.** `?p edw:name ?pn` then resolves to `DIM_PRODUCT` only → `load_table.n` **7 → 2** (fact + DIM_PRODUCT), killing 5 dead dim loads (~15 s incl. the 390 K-row DIM_CUSTOMER) AND — pending confirmation — most of the 1447 `scan_table` re-scans (if those are the 6-dim fan-out re-scanned per driving batch, not DIM_PRODUCT alone; this also subsumes much of the F18/F19 "1447 DIM_PRODUCT re-scans" framing, which assumed a single dim). This is the **F8/PR-3 shared-base-predicate over-scan**, on the JOINED dim attribute rather than the primary star, and it rhymes with `[[fk-templated-ref-fusion]]`'s `trust_fk_refs` (the FK target is known; don't scan all candidate parents).
- **Residual floor (after the prune): one 7,670-file `FACT_INVENTORY_SNAPSHOT` decode** — the file-count decode-wall (PR-2a), un-cuttable by the LIMIT. **So q031's ≤3 s achievability depends on PR-2a (the ROADMAP's master lever), NOT on cache residency.**
- **L1 (pin) and L2 (residency) are both largely MOOT for q031:** the pin isn't leaking, and 51 MB doesn't need residency tuning. What remains of "cold-floor" for q031 is (fix A) the resolution-prune + (floor) PR-2a's per-file decode. The disk catalog cache (PR-8 slice 2) already removes the metadata/manifest S3 reads; the vended-creds `loadTable` GET is one-per-table and correctly pinned.

### Consequence for the slate

q031's cold-floor PR should be **reframed from "loadTable-pin + residency" to "RefObjectMap-target resolution prune (F8/PR-3 family) + lean on PR-2a for the fact decode-wall."** This is a different, smaller, DETERMINISTIC fix (a resolution-set constraint, gated by a `scan_table`/`load_table`-count sentinel: 7→2), with the residual wall explicitly attributed to PR-2a. **The L1/L2 design in §a-§e is superseded.** Recommend: file the ref-target fan-out as its own finding (F20?), re-scope the q031 PR to it, and STOP for the lead's re-ruling — no L1 pin code. The open question for AJ's bar is now cleaner: after the prune, q031 = one 7,670-file fact decode; whether that clears ≤3 s cache-thrashed is a PR-2a question (per-file overhead × 7,670), reported honestly.
