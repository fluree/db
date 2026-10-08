# PR-4d (F14) — batched-OPTIONAL seed coalescing: one inner scan for the whole driving side — DESIGN SKETCH

**Branch:** to stack on `fix/f18-q031-memo-limit` (off `fix/f9-virtual-curie` #1499) — or its own branch off #1499; lead's call.
**Status:** SKETCH — **STOP for lead review**. No engine code until approved.
**North-star slate item 2** (AJ-signed). Substrate: `07-pr4b-batched-optional.md` (soundness contract), `13-pr4c-optional-star.md` (q016 admission + the F14 measurement), F14 (`04-findings-register.md`).
**Live target:** q016 43 s → seconds cache-thrashed (see the honest cold-floor coupling in §d). q050 is already 1.24 s / 63 scans on the clean re-baseline (not a tail query) — it is the **already-fast sibling** that validates scan-once and must not regress.

## The seam (mechanism, code-anchored)

PR-4b/4c admitted q016's OPTIONAL inner (a same-subject `FACT_SHIPMENT` star correlated on the object `?o`) to the batched hash-left-join `build_batch` (`optional.rs:1191`). That path seeds the inner ONCE with the distinct correlation tuples **of one required batch**, scans it, and hash-partitions the output back by correlation key. Measured on q016 (PR-4c): `scan_table 182 = 2 FACT_ORDER (outer, collapsed) + 180 FACT_SHIPMENT (inner)`. The 180 inner scans are the F14 residual — **the inner is re-scanned per driving window, not once.**

Two nested facts produce the 180:

1. **`build_batch` granularity is per REQUIRED (outer) batch.** The operator's `next_batch` calls `build_batch(required_batch, current_required_row)` (`optional.rs:2029`) and on success advances `current_required_row = required_batch.len()` (`:2039`) — i.e. one `build_batch` consumes exactly one outer batch pulled from `self.required.next_batch` (`:2008`). So the number of `build_batch` calls = the number of batches the **outer** (Orders) side emits.
2. **The seeded inner re-scans its base table per child batch.** Inside `build_batch`, the inner is `build_where_operators_seeded(seed, inner_patterns, …)` (`:1306`) where the inner pattern is `Pattern::R2rml` (`FACT_SHIPMENT`). A correlated `R2rmlScanOperator` "re-enters `build_progress` per child batch" (`r2rml/operator.rs:366`, `:953`), and each `build_progress` does a fresh table scan (`collect_scan_capped`, `:1016`). So each `build_batch` → ≥1 fresh `FACT_SHIPMENT` scan.

So q016's outer `FACT_ORDER` scan (under the `LIMIT 5000` budget window) emits **many small windowed batches**, and each one drives a **separate** `FACT_SHIPMENT` inner scan → ~180. The batched OPTIONAL collapsed the per-ROW rebuild (PR-4b's win: 517→182) but left a **per-outer-window** inner re-scan. **F14/PR-4d = collapse that too: consume the whole driving side's correlation set in ONE inner scan + in-memory hash-join** (the "IN-set / probe" successor to `07` open-Q2).

> **Implementation item 1 (trace-first, mirrors docs 17/18):** confirm the exact outer-batch cardinality and whether the 180 is driven purely by the outer emitting small budget-windowed batches (most likely) vs. the inner's own materialize-window re-chopping a single 1000-seed. The fix differs slightly: if it's outer-batch granularity (expected), the coalescer buffers across outer batches (§a); if the inner re-windows a single large seed, the fix also needs the inner to accept the whole seed as one `build_progress` (touches `r2rml/operator.rs` — a bigger blast radius). The trace decides. No engine code before it.

## (a) The fix — coalesce the driving side into one seed

Today `build_batch` is **per outer batch**. PR-4d makes the batched OPTIONAL **buffer the whole (bounded) driving side first**, then seed + scan the inner **once**:

1. Drain `self.required` fully (or up to a **seed cap**, below), buffering the required rows in order and accumulating the **distinct** correlation tuples across ALL outer batches (the existing `seen_seed` dedup, `:1285`, just spans the whole driving side instead of one batch).
2. Build the inner ONCE seeded by that full distinct set (`MaterializedSeedOperator`, `:1305`) → **one** `FACT_SHIPMENT` scan.
3. Hash-partition the inner output by the full correlation key (unchanged, `:1340-…`) and emit the buffered required rows against their buckets (LEFT-JOIN: unmatched required rows survive with unbound optional vars).

**Seed cap (the unbounded-OPTIONAL guard).** Buffering the whole driving side is bounded and cheap **when there is a LIMIT** (q016: exactly 5000 orders) or a small driving side. For an OPTIONAL over a huge unbounded driving side, buffer-all is unbounded memory. So coalesce up to a cap — natural choice: the query's LIMIT if present, else `materialize_window_rows()` (512K, `r2rml/operator.rs:208`). Above the cap, fall back to the **current** per-window behavior on each cap-sized chunk (still strictly ≥ today: a 512K-row window is one inner scan for up to 512K driving rows vs. today's ~1000/window). So PR-4d is "one inner scan per **cap-sized** driving window" — for LIMIT-bearing or modest OPTIONALs (the whole tail here) that is exactly one scan.

## (b) Soundness — unchanged from PR-4b/4c

The partition-by-correlation-key logic is **identical**; only the seed's population widens from one outer batch to the whole driving side. The `07` contract still holds: (1) correlation-closure — the correlation set is `required-cols ∩ ⋃ referenced_vars(inner)` (`:1211`), independent of how many outer batches feed it; (2) pure restriction — an R2rml leaf carries no internal LIMIT/subquery; (3) same construction — same `build_where_operators_seeded`, same output-schema guards (`:1317-1331`) that fall back rather than mis-partition. Widening the seed cannot change any per-required-row result — a required row's bucket is keyed by its own correlation tuple regardless of which outer batch it arrived in.

**Order preservation.** The buffer holds required rows **in driving order**, so emitting them against their buckets preserves outer order (matters only if an ORDER BY sits above; q016 is `rows_only`/unordered LIMIT so it's free — but the buffer makes it correct for the ordered case too, unlike a naive bucket-drain).

**LIMIT correctness.** With `LIMIT 5000` the driving side is capped at 5000 rows upstream, so the buffer is exactly the 5000 required rows; the OPTIONAL never multiplies the driving count (left-join), so the LIMIT semantics are unchanged.

## (c) Kill switch + gate

- **Kill switch:** a sub-switch `FLUREE_R2RML_OPTIONAL_SEED_COALESCE` (default on) inside the existing `FLUREE_R2RML_BATCHED_OPTIONAL` / `FLUREE_OPTIONAL_HASH_JOIN` family; **off ⇒ per-outer-batch** (today's shipped PR-4b/4c path, byte-identical). Lets the coalescing be toggled independently of the star admission (PR-4c) and the base hash-join (PR-4b).
- **Deterministic sentinel:** q016 `scan_table` **182 → single-digit** (2 outer + ~1 inner) — the crisp cache-independent proof of one inner scan (exactly the PR-4c gate criterion, now actually met). q050 `scan_table` **63 → single-digit** and no wall regression.
- **Three-way differential (hermetic):** coalesced ≡ per-outer-batch ≡ per-row, IDENTICAL solution multisets, on ONE mock covering the `07`/PR-4c risks: OPTIONAL-miss (unmatched required row survives, unbound optional vars), multi-row-per-correlation cartesian, dangling/null object member, AND a **multi-outer-batch** seed (≥2 outer batches whose distinct correlation sets overlap — the new coalescing-specific case, asserting dedup across batches doesn't drop or double a bucket).
- **Live gate:** q016 rows-parity vs oracle (rows_only per manifest), cache-thrashed full-corpus order; no other query's wall/hash moves; the 42/50 ≤3 s set stays put.

## (d) Honest expected win — and the cold-floor coupling (important, cross-slate)

PR-4d removes the **180× inner re-scan multiplier**, leaving q016 with **one** `FACT_SHIPMENT` inner scan. But per the PR-4c doc's honest read, that one scan is a **fact-scale cold scan** (`FACT_SHIPMENT` ~7,670 files; the `edw:order = ?o` seed FILTER prunes only if the FK column carries file stats — if not, a near-full scan). So:

- **Warm / warm-disk:** fast (one inner scan + partition, all resident) — seconds or less.
- **Cache-thrashed / first-ask (AJ's ruled bar):** q016 bottoms out on **one cold `FACT_SHIPMENT` scan** — the SAME residual as q031's one cold fact read (doc 18). If that one cold fact scan exceeds the bar, q016 is **fact-read-bound**, not re-scan-bound, and PR-4d alone doesn't reach ≤3 s.

**This is the key cross-slate insight:** the tail's plan-shape fixes (PR-4d here, F17 UNION-budget for q029, F19 memo for the correlated family) each remove a **re-drive multiplier**, but every one of them bottoms out on the SAME **cold-floor** — one cold `loadTable` + one cold fact scan — which is doc 18's PR-8-tail (L1 loadTable pin + L2 fact residency). So under AJ's cache-thrashed bar, **PR-4d is necessary but not sufficient for q016 without the cold-floor L2 (fact residency)** — exactly as q031 needs L1+L2. The plan-shape PRs and the cold-floor PR are complementary, and the honest end-state for each tail query is "one cold fact touch," which only the cold-floor work removes. Recommend sequencing PR-4d (removes the multiplier; deterministic scan-count proof) **and** flagging its cache-thrashed residual as an explicit input to the cold-floor L2 measurement (don't double-count; doc 18 open-item 3).

## Blast radius

- **q016** — the clear case (this PR). **q050** — already fast (1.24 s), same family; must not regress, should drop to single-digit scans (validates the coalescer on a shipped shape).
- Any OPTIONAL with a correlated R2RML inner admitted by PR-4b/4c takes this path — all benefit uniformly (fewer inner scans), none change results (soundness §b).
- **NOT q029.** The register's F17 note ("likely subsumed by F14/PR-4d") is **mechanistically wrong to assume** — q029's ~253 re-drives are **UNION-branch** re-scans (`UnionOperator`, budget absorption, F17), a *different* operator than the batched OPTIONAL. PR-4d does not touch UNION. Correct the F17 "subsumed by PR-4d" wording to "shares the re-drive **symptom**, different operator — F17 UNION-budget is a separate fix." (Register follow-up, not this PR.)
- Native untouched: `R2rmlScanOperator` / the batched OPTIONAL R2RML admission never instantiate on a native query.

## DoD (proposed — lead to fix)

1. q016 `scan_table` 182 → single-digit (deterministic proof of one inner scan), rows-parity vs oracle, cache-thrashed order.
2. q050 no regression (wall + hash), scan-count drops.
3. Three-way differential (coalesced ≡ per-outer-batch ≡ per-row) incl. the multi-outer-batch dedup case; switch-matrix byte-identical with the sub-switch off.
4. Full-corpus cache-thrashed baseline at head: no other query's wall/hash regresses; 42/50 stay ≤3 s.
5. Native 54/54 + W3C + unit sweeps green; zero native-path change.

**STOP — design review before implementation.** Open questions for the lead: (i) buffer the whole driving side vs. cap at the LIMIT/512K window — confirm the cap policy; (ii) stack on `fix/f18-q031-memo-limit` or its own branch off #1499; (iii) given the §d cold-floor coupling, is PR-4d's success criterion the **scan-count collapse** (deterministic, meetable now) with the cache-thrashed wall explicitly deferred to the cold-floor L2 — i.e. PR-4d gated on 182→single-digit + parity, NOT on q016's wall hitting ≤3 s alone?
