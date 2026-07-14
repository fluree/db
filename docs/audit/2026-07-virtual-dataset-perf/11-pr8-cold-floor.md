# PR-8 — Cold / Floor Program (persistent catalog state + 429 backoff) — DESIGN

**Date:** 2026-07-14
**Branch:** `perf/r2rml-pr8-cold-floor` (off `perf/r2rml-pr6-rollup-fusedagg` HEAD `05d2f81aa`)
**Status:** DESIGN **APPROVED with rulings** (lead, 2026-07-14) — now in the **measure-first** phase: sub-spans landed, running the cold decomposition, then implementing to the numbers. The three open questions are resolved below (see §2 header + §5). **The security ruling reshapes the persistence design: NO tokens and NO vended credentials on disk — in-memory only, unchanged.**
**Companions:** `09-stacked-rebaseline.md` §5 (the cold-subset numbers this grounds in), `06-per-file-cost.md` (the H7 setup floor, separate from the per-file footer PR-2 fixed), `05-diagnosis.md` §H7, `ROADMAP.md` PR-8.
**Code read at this HEAD:** `fluree-db-api/src/graph_source/{catalog_session.rs, cache.rs, r2rml.rs}`, `fluree-db-iceberg/src/catalog/rest.rs`, `fluree-db-query/src/r2rml/{operator.rs, fused_aggregate.rs}` (serialization audit, §5).

## Lead rulings (2026-07-14) — the design contract

1. **(ii, decided first because it shapes everything) NO credentials persisted to disk.** No OAuth bearer tokens, no vended S3 credentials. The `.fluree` home already stores the PAT and that is a known sore spot (the `fluree info` leak finding); a second on-disk credential surface with an independent lifetime is not worth a small win (OAuth is ~0.5 s **once** per process; a within-creds-TTL `loadTable` skip only helps repeated cold restarts inside a ~15–60 min window — an edge case). **Persist only:** `TableMetadata` + `scan_files` (content-addressed by `metadata_location`, immutable, zero secrets — the clean win) and at most the non-secret **`metadata_location` pointer** under a drift-TTL. **A cold process still pays one `loadTable` GET per table** (it needs fresh vended creds regardless) — the design is shaped around that floor being **irreducible without creds-persistence**.
2. **(i) Dedicated `DiskCatalogCache` directory**, not `DiskArtifactCache`'s — different semantics (small immutable content-addressed entries vs an LRU byte-budget artifact store), and the cold DoD protocol must be able to clear the artifact cache while keeping (or independently clearing) catalog persistence — separate dirs make the protocol expressible.
3. **(iii) Parallelize catalog resolution across TriplesMaps** if it is serial today — added to PR-8 scope. The generate-path precedent (`0ade90c59`, multi-table preview `buffered(8)`, 47 s→10 s) suggests this may be the **biggest single-query cold win available with no persistence at all.** §5 audits the call sites and reports the wall-vs-sum evidence. **[Audited: it IS serial in both the generic and the fused multi-table paths — see §5.]**
4. **DoD honesty:** with no creds on disk, q036's cold floor keeps OAuth (~0.5 s once) + one `loadTable` GET (~1.3–3 s). **"Sub-second" may be foreclosed** for a cold first-touch; re-set the target from the decomposition data (e.g. "cold = OAuth-once + one *parallelized* `loadTable` per table + ~0 metadata/manifest"), do not chase a number the security ruling forecloses.

---

## 0. The cold floor, in one paragraph

`09 §5` measured a fresh cold process paying **~1.6–2.1 s of `loadTable` per table** (q001 dim star 2.2 s cold; q036 bare `COUNT(*)` **2.8 s cold and its ENTIRE wall is this floor** — it reads zero data files thanks to PR-1). The engine already has a full stack of catalog caches — a process-wide REST client (OAuth token + connection pool), a cross-query `loadTable` cache, parsed `TableMetadata`, manifest-derived `scan_files`, and Parquet footers — **but every one of them is in-memory (`moka` / an in-process `HashMap`), so a cold `exec-one --cold` (a *fresh process* with the disk cache cleared) starts with all of them empty and re-pays the whole catalog handshake per table.** Under the **security ruling (no creds/token on disk)**, PR-8 persists only the **immutable, secret-free layers** — parsed `TableMetadata` + manifest-derived `scan_files` (content-addressed by `metadata_location`) — plus a non-secret `metadata_location` pointer, so a cold process skips the **metadata-JSON read and the manifest read**, while still issuing **one `loadTable` GET per table for fresh vended credentials** (the irreducible floor) and one OAuth exchange per process. A further lever then attacks that residual GET floor without persistence: **parallelizing the per-table `loadTable` resolutions**, which are serial today (§5.2), collapsing a k-table query's k serial GETs toward one GET's latency — all without touching the per-query snapshot pin that guarantees a query reads one consistent Iceberg snapshot. The 429 backoff (long-deferred) and a catalog-request concurrency cap ride along so PR-2's raised scan concurrency and the crawl fan-out stop storming Snowflake Horizon.

---

## 1. Where the floor actually is (grounded in the caches, to be closed by sub-spans)

**The cache stack today** (`cache.rs` `R2rmlCache`, all process-scoped in-memory + the per-query `IcebergCatalogSession`):

| Layer | Home | Key | Persists across… | What it costs on a MISS |
|---|---|---|---|---|
| REST client + OAuth `CachedToken` + HTTPS pool | `rest_clients` moka (TTL 900 s) | config fingerprint | queries **in one process** | OAuth token exchange (`self.auth`, `rest.rs:104`) — ~0.5 s (`06`) |
| `loadTable` response (metadata_location + vended creds) | `rest_load_tables` moka (TTL 60 s) **+** `IcebergCatalogSession` (per-query pin) | `(source, ns.table)` | 60 s window / one query | REST `GET /tables/<t>` (`rest.rs:221` → `request_with_retry` → `.send()`) — **~1.3–3 s/table** (the dominant term) |
| parsed `TableMetadata` | `table_metadata` moka | **`metadata_location`** | queries in one process | metadata JSON fetch + parse |
| manifest-derived file list (`CachedScanFiles`) | `scan_files` moka | **`metadata_location`** | queries in one process | manifest-list + manifests read (`scan_plan`) — ~0.4 s/table (`06`) |
| Parquet footers | `parquet_footers` (64-entry) + `DiskArtifactCache` | file path | disk cache lifetime | per-file footer (PR-2 collapsed this WARM; cold = S3 read/file) |

**Why cold still pays ~2 s/table despite all of the above (the answer to the lead's (a)).** Every layer is **in-memory and process-scoped**. The cold protocol is `exec-one --cold` = **a fresh subprocess with the home-scoped disk artifact cache cleared**. A fresh process has empty `moka` caches, so table 1 of a cold query re-pays: (1) the OAuth exchange (once), then per table (2) the `loadTable` GET, (3) the metadata-parse, (4) the manifest read; then per file (5) the footer + data S3 GETs. The tier-1 caches are a **warm-path, same-process** optimization (a burst of queries in a long-lived server reuses them); they do nothing for a cold restart. **The cold floor is the empty-caches-on-a-fresh-process cost.**

**The decomposition to CLOSE with sub-spans (§5 measurement).** From `06` we have OAuth ~0.5 s (once) + `loadTable` ~1.6–2.1 s/table + `scan_plan` ~0.4 s/table, but the **~1.6–2.1 s `loadTable` was not itself split** from the metadata read and manifest read that follow it in `load_table_context`. §5.1's landed sub-spans split `loadTable` GET vs metadata-JSON read vs manifest read per component; the live decomposition (§5.3) then shows which slice PR-8 can actually remove given the security ruling.

---

## 2. (a) Persistent catalog / credential state across process restarts

The lever: **persist the secret-free layers that are safe to persist, to a disk store a fresh process reads before hitting the catalog.** Under ruling (ii) this is **two** layers — the immutable metadata/file-list (2.1) and a non-secret snapshot pointer (2.2); the OAuth token (2.3) stays in-memory only:

### 2.1 The clean immutable win — persist `TableMetadata` + `scan_files` keyed by `metadata_location`

`metadata_location` is a **content-addressed S3 path** (`cache.rs:83`: "the S3 path is a content hash, so different snapshots have different keys"). So `table_metadata` and `scan_files` keyed by it are **immutable** — a given key's value can never go stale, and a new snapshot is simply a new key. **Persist both to disk indefinitely** (a sidecar under the existing `DiskArtifactCache`, keyed by `metadata_location`). A cold process that already knows the `metadata_location` (see 2.2) reads the parsed metadata + the manifest-derived file list from local disk, skipping components (3) and (4) entirely with **zero staleness risk and no expiry logic.** This is the highest-ROI, lowest-risk piece.

### 2.2 The `loadTable` response — persist ONLY the non-secret `metadata_location` pointer, TTL-gated (NO creds)

**Ruling (ii): the vended credentials do NOT persist.** What persists from the `loadTable` response is the **`metadata_location` string only** — a non-secret S3 path — keyed by `(source, ns.table)` under the **snapshot-drift TTL** (`FLUREE_ICEBERG_LOADTABLE_TTL_SECS`, default 60 s: bounds how stale a snapshot a *new* cold query may open). This is a pointer, not a `CachedLoadTable` (no `credentials` field written to disk).

What it buys, and what it does **not**:
- **Buys (via 2.1):** a cold query that finds a within-TTL persisted `metadata_location` uses it to key the persisted `TableMetadata` + `scan_files` — so components **(3) metadata-JSON read and (4) manifest read** are served from local disk with zero S3 round-trips.
- **Does NOT buy:** skipping the `loadTable` GET itself. **A cold process still issues one `loadTable` GET per table to obtain fresh vended S3 credentials** — the creds are never on disk, and the data files (or, for `COUNT`, the manifest) cannot be read from S3 without them. So **component (2), the `loadTable` REST GET, is the irreducible per-table cold floor** under the security ruling. The pointer only lets us *reuse* the resolved snapshot immediately (skipping the metadata/manifest reads); it cannot remove the GET.
- **Nuance the measurement must settle (q036):** a bare `COUNT(*)` reads **no data files** — it needs S3 creds only to read the *manifest*. If the manifest-derived record counts are themselves persisted (2.1's `scan_files`, keyed by `metadata_location`, immutable), a cold q036 with a within-TTL pointer could answer from disk **needing neither the manifest read nor even the `loadTable` GET** (no S3 access at all). Whether the current COUNT path (`send_read_snapshot_data_files`) can be served from persisted `scan_files` is a §5 measurement + a design decision, not an assumption — the sub-spans decide.

### 2.3 The OAuth token — **in-memory only, unchanged (ruling ii)**

**No persistence.** The `CachedToken` stays in the process-wide `rest_clients` moka (TTL 900 s), exactly as today. A cold process pays the token exchange **once** (~0.5 s, `iceberg.oauth_token`) and reuses it for every table in that process. Persisting it would put a live bearer token on disk for a one-time ~0.5 s saving that only recurs across process restarts inside the token TTL window — the security cost/benefit the lead ruled against. The rotated-secret self-heal and 401-refresh (`rest.rs:116-121`) are therefore unchanged and need no on-disk carry.

### 2.4 The correctness invariant (the lead's constraint, restated as the design contract)

> **The per-query snapshot pin STAYS. What persists is the immutable metadata/file-list layer and a non-secret snapshot pointer — never credentials, and never the query's snapshot selection.**

Concretely: `IcebergCatalogSession` (the per-query `load_tables` pin, `catalog_session.rs:86`) is unchanged. On the first resolution of a table in a cold query, the resolver still issues the `loadTable` GET for **fresh vended creds** (2.2), but consults the **persistent** layer for the `metadata_location`: if a within-TTL persisted pointer matches the freshly-loaded location (or is adopted as the pin), the `TableMetadata` + `scan_files` come from disk (2.1) instead of new S3 reads. Whatever `metadata_location` the query resolves is **pinned into the per-query session** and every later scan in that query reads that one snapshot (exactly as today, `catalog_session.rs:110-142`). The persistent layer only removes S3 metadata/manifest round-trips; it never lets two scans in one query see two snapshots, and the pointer TTL bounds cross-*query* drift just as `rest_load_tables` does today.

**Storage.** A **dedicated `DiskCatalogCache`** directory (ruling i) — its own home-scoped dir, *not* `DiskArtifactCache`'s, because the semantics differ (small immutable content-addressed entries vs an LRU byte-budget artifact store) and the cold DoD protocol must clear the artifact cache while keeping or independently clearing catalog persistence. Contents: `metadata_location → {TableMetadata, scan_files}` (immutable, no expiry, zero secrets) and `(source,table) → metadata_location` (non-secret pointer, TTL-gated). **No `credentials`, no `CachedToken` on disk.** **Kill switch** reuses the `FLUREE_ICEBERG_LOADTABLE_CACHE` switch family (off ⇒ no disk read/write, today's behavior); the cold benchmark's `--cold` clears the artifact cache and, per the DoD protocol, clears or retains the `DiskCatalogCache` independently so the gate can measure both the true cold floor and the persisted-catalog floor.

---

## 3. (b) 429 backoff + retry + a catalog-request concurrency cap

**Greenfield — there is no 429/retry/backoff anywhere in `fluree-db-iceberg` today** (grep-confirmed); `request_with_retry` (`rest.rs:88`) handles **401 only** (refresh + one retry).

- **Backoff.** Extend `request_with_retry` to treat `429 TOO_MANY_REQUESTS` (and `503`) as retryable: exponential backoff with jitter, honoring a `Retry-After` header when present, bounded attempts, then surface the error. Same `Box::pin` recursion the 401 path uses.
- **Concurrency cap.** A process-wide **catalog-request semaphore** so a fan-out cannot storm Horizon. This is where fan-out actually hits hard: the **crawl unfused/browse-wildcard path** (`crawl.rs` — the F8 fan-out that resolves many maps → many parallel `loadTable`/list calls; memory: "the wildcard-crawl fan-out trips the Snowflake Horizon 429"), and any **scan-concurrency raise** (PR-2 lever B) or **PR-3 crawl** that multiplies concurrent catalog calls. The cap bounds catalog QPS independently of the data-scan concurrency (which is S3, not Horizon).
- **Interaction with 2.x.** The persistent catalog cache *reduces* catalog QPS directly (a cold process makes far fewer `loadTable` calls), so (a) and (b) compound — (a) lowers the load, (b) makes the residual load safe.
- **Switches:** a backoff-tuning env (max retries / base delay) and a catalog-concurrency cap env, both defaulting on; off ⇒ today's single-shot behavior.

---

## 4. (c) Cold sparse-read residual — scoped OUT of PR-8, into PR-7/PR-5

`q019` (GL decimal filter) and `q027` (WebEvent by type) are **41–43 s cold** (`09 §5`) — a full ~7,670-file fact scan where, post-PR-2, the footer is collapsed warm but **cold every file is a fresh S3 GET** (footer + data), so `7,670 × ~85 ms / concurrency ≈ 37–43 s`. This is **data-fetch, not catalog floor**, and it splits cleanly away from PR-8:
- The **catalog** portion of a cold full scan (OAuth + `loadTable` + manifest read) is exactly the §2 floor — PR-8 removes it.
- The **data-fetch** portion is fundamental cold (the bytes must come from S3 once). The only levers are **(i) a persistent disk *artifact* cache** — which would warm a *repeated* cold-process scan of the same table, but the cold DoD protocol **clears it by definition**, so it's a production win, not a cold-gate number (note it, don't build it into the gate); and **(ii) reading fewer files** — decimal/double + top-k file pruning, which is **PR-7/PR-5 territory**. Keeping scopes clean: PR-8 does not chase the cold data-fetch; it closes the catalog floor and hands the sparse-read residual to PR-7's cold-only pruning (whose value the ROADMAP already gates on *these* cold numbers).

---

## 5. (d) Measurement plan — decompose the floor per component; audit serialization; then re-set the DoD

### 5.1 Sub-spans — LANDED (measurement-only, §6 slice 1)

The floor is split by tracing sub-spans. Most already existed; **the two gaps are now closed** (commit in this branch, allowlisted in `fluree-bench-virtual/src/spans.rs` + its callsite-verification test):

| Component | Span | Status |
|---|---|---|
| (1) OAuth token exchange (once/process) | `iceberg.oauth_token` (`auth/oauth2.rs`) | existed |
| (2) `loadTable` REST GET (per table) | `r2rml.load_table` (`r2rml.rs`) | existed |
| (3) metadata-JSON S3 GET + parse (per table) | **`r2rml.read_metadata`** (`r2rml.rs`, keyed by `metadata_location`) | **NEW** |
| (4a) manifest read — SCAN path | `iceberg.scan_plan` (`scan/send_planner.rs`) | existed |
| (4b) manifest read — COUNT path (q036) | **`r2rml.count_manifest_read`** (`r2rml.rs`, nested in `r2rml.count_manifest`) | **NEW** |
| (5) per-file footer/fetch/decode | `iceberg.read_footer` / `iceberg.fetch_bytes` / `iceberg.decode` | existed |

The two new spans matter because (3) and (4b) were the only unmeasured slices of the shared `load_table_context` and the COUNT path — exactly the layers §2.1/§2.2 persist. On the first table of a cold process, `iceberg.oauth_token` nests inside that table's `r2rml.load_table`, so **subtract oauth from the first `load_table` to get the pure GET**; later tables' `load_table` is pure GET.

**Client-side answer to old open (iii):** the sub-spans confirm the client makes three *sequential, independent* round-trips per table — `r2rml.load_table` (REST GET → metadata_location + creds), then `r2rml.read_metadata` (S3 GET of the metadata JSON), then the manifest read (`scan_plan`/`count_manifest_read`, S3 GETs). Whatever Horizon does server-side, on the client the manifest read is **not** part of the `loadTable` GET, so §2.1 (persist metadata+scan_files) and §2.2 (persist the pointer) are independent client wins — 2.1 removes (3)+(4), 2.2 lets 2.1 apply without a fresh metadata read. Neither removes (2), the GET (ruling ii).

### 5.2 Serialization audit (ruling iii) — CONFIRMED SERIAL in both multi-table paths

The per-TM `loadTable` resolutions **run one-at-a-time** within a single multi-table query — audited at the call sites:
- **Generic multi-TriplesMap path** (`operator.rs:755`): `for triples_map in &triples_maps { … table_provider.scan_table(…).await? … }` — a plain `for`-loop awaiting each table's scan (which begins with its `load_table_context`) before the next. Independent tables, no data dependency, yet strictly serial.
- **Fused aggregate-over-join path** (`fused_aggregate.rs:1722` terminal-dim scan, then `1769` the `for h in (0..hops.len()-1).rev()` per-hop loop): each hop's `scan_table(…).await?` awaited in sequence; the fact scan adds one more. A k-hop rollup pays **k+1 serial `loadTable` GETs**.
  - *Subtlety:* the fused dim SCANS are genuinely order-dependent (composed terminal-back-to-fact, `map.get(&fk_next)`), so the *scan* order cannot be reordered — but the `loadTable` *resolution* (REST GET + metadata) is data-independent, so **resolution can be parallelized (buffered) up front and the row-consuming scans kept in dependency order.** The generic path has no such constraint; its whole `scan_table` calls parallelize.

**Evidence to confirm in the live run:** for a multi-table query (q008), `wall ≈ Σ load_table` (serial) vs `wall ≈ max load_table` (parallel). The measured `Σ r2rml.load_table` vs the query wall settles it and sizes the win. Precedent `0ade90c59` used `buffered(8)` (request-order-preserving, deterministic) to take the generate preview 47 s→10 s; the same shape applies here.

### 5.3 DoD gate — the cold subset, target RE-SET from the decomposition (ruling iv)

Cold subset (`q001, q008, q019, q027, q036, q046`, one at a time, 2 s paced, `--cold`, median of ≥3 reps). **The security ruling forecloses "sub-second" for a cold first-touch** (one `loadTable` GET is irreducible). The re-set targets:
- **q036** (bare `COUNT(*)`, 2.8 s today): decompose into `load_table` GET + `read_metadata` + `count_manifest_read`. Target = **OAuth-once + one `loadTable` GET + ~0 metadata/manifest** (2.1 persists the manifest record-counts). The residual IS the irreducible GET; report it, don't target sub-second unless the measurement shows the manifest read (not the GET) dominates — in which case q036 could approach the pointer-served floor.
- **q001** (dim star, 2.2 s cold): one `loadTable` GET + persisted metadata → target ≈ the single-GET floor.
- **q008** (rollup, 100 s cold pre-PR-6): the multi-table floor. Two levers stack — persistence removes metadata/manifest, **parallelization (§5.2) collapses k+1 serial GETs toward one GET's latency.** Target = `OAuth-once + ~1×GET (parallelized) + operator/scan residual`.
- **q019/q027/q046** (full-scan colds, 37–43 s): the §4 residual — PR-8 removes only the catalog slice; the data-fetch remainder is the PR-7 hand-off number, **reported not gated.**

**Discipline.** Cold numbers are Snowflake-live and variable — gate on the median of ≥3 paced cold reps; the persistent-cache correctness (pointer TTL, snapshot-pin invariance, immutable-key safety) is proven by hermetic tests, not the live run. **No creds/token expiry tests needed — nothing credential-bearing persists (ruling ii).**

---

## 6. Ranked implementation slices — to be finalized against the §5 decomposition numbers

Order is provisional; the live decomposition (running now) confirms which slice carries the most cold win before code is written (lead: "implement to the data").

1. **Measurement sub-spans** (§5.1) — **DONE** (this branch): `r2rml.read_metadata`, `r2rml.count_manifest_read` + allowlist. They gate the rest and are the DoD counters.
2. **Parallelize catalog resolution across TriplesMaps** (§5.2, ruling iii) — if the live wall confirms `Σ load_table` (serial), this is a **no-persistence cold win** and likely the largest single-query lever for multi-table queries (q008 and every rollup). Generic path: `buffered(N)` the `scan_table` calls (request-order-preserving, per `0ade90c59`). Fused path: buffer the `loadTable` *resolutions* up front, keep the composed dim *scans* in dependency order. **Sequence this early** — it needs no disk store and no security surface.
3. **Persist the immutable layer** (§2.1: `TableMetadata` + `scan_files` by `metadata_location`) — highest-ROI persistence, zero staleness, no expiry logic, **zero secrets.** Removes metadata (3) + manifest (4) reads on a cold hit. Likely the bulk of the q036/q001 cold win.
4. **Persist the non-secret `metadata_location` pointer** (§2.2, TTL-gated, **no creds**) — lets 2.1 apply on a cold hit without a fresh metadata read; still issues the `loadTable` GET for creds. Correctness behind the hermetic pointer-TTL / snapshot-pin tests.
5. **429 backoff + catalog concurrency cap** (§3) — de-risks the fan-out; compounds with the reduced QPS from 2–4.

**Explicitly OUT (ruling ii):** persisting the OAuth token or the vended creds. In-memory only, unchanged.

**Open questions — RESOLVED by the lead (2026-07-14):**
- (i) **Dedicated `DiskCatalogCache` dir** (not `DiskArtifactCache`'s) — separate semantics; the cold DoD protocol clears/keeps them independently.
- (ii) **No tokens or vended creds on disk.** Persist only immutable metadata+scan_files and the non-secret `metadata_location` pointer. The `loadTable` GET is the irreducible per-table floor.
- (iii) **Client-side, independent round-trips** (§5.1) — 2.1 and 2.2 are independent; neither removes the GET. The live sub-spans confirm the split and whether q036's manifest read can be fully pointer-served.

**Next:** report the §5 decomposition numbers to the lead, then implement slices 2–5 to the data.

---

## 7. Slice-1 (parallelize catalog resolution + session storage-cache) — implementation, gate, residuals

Shipped as ONE commit with one switch (`FLUREE_R2RML_PARALLEL_CATALOG`): a best-effort `prefetch_tables` that warms a query's per-table catalog contexts concurrently (`buffered(8)`, request-order-preserving per `0ade90c59`) before the serial scan loops, **plus** a session-scoped S3-client cache (see §7.3). Injected in the fused path (`fused_aggregate.rs`, before the terminal scan) and the generic multi-TriplesMap loop (`operator.rs`); it dedupes against tables already resolved in the session pin (`is_pinned`) so a warmed-then-scanned table pays one GET, not two. Best-effort: a warm error is swallowed and the real scan re-resolves and surfaces it, so the lever is purely additive and switch-gated.

### 7.0 The honest slice-1 win (expectation-set)

On a **fused** query the parallelizable catalog slice is **(tables − 1) × ~1.7 s** of first-touch `loadTable` GET latency — the first GET can't be overlapped, each additional one can. So q008 (fact + DIM_CUSTOMER + DIM_GEOGRAPHY, 3 tables) ≈ **~3.4 s**; q032 (fact + DIM_STORE, 2 tables) ≈ **~1.7 s**. On top of that, the folded session storage-cache is an **independent, always-on** saving: **one AWS SDK-client build per table instead of one per scan** — so it removes prefetch's warm+scan double-build (which is what made naive prefetch a net loss), and it also removes the per-re-scan rebuild in correlated joins (a pre-existing defect, §7.3), both with the switch OFF too for the storage-cache half. These are first-touch (cold-catalog) numbers; a warm-catalog process pays neither.

### 7.1 DoD gate = WARM-DISK A/B, NOT cold (cold's catalog slice is unmeasurable)

The original §5.3 cold gate is **wrong for slice 1**, and the underpowered q008 cold A/B proved it: q008's cold wall is ~550 s of `iceberg.fetch_bytes` flattened to ~34–40 s by 16-way S3 concurrency, and a ~3 s catalog-slice effect is **unresolvable against the ±4 s cold S3 noise** (measured cold walls: prefetch-ON 44.2–50.0 s, OFF 41.1–42.9 s — overlapping). The `Σ r2rml.load_table` span total does **not** shrink under concurrency either (each GET's own duration is unchanged; only their wall overlap changes), so the span sums can't show it. **Slice-1's DoD gate is therefore the WARM-DISK state** (the original protocol's third state): a *fresh process* (in-memory catalog caches empty → the loadTable GETs are paid) reading a *populated on-disk artifact cache* (data local → the scan is fast), so `wall ≈ catalog slice + fast scan ≈ 6–9 s` and a 3–4 s parallelization effect is decisive. Protocol: prime the `--cache-dir` once, then fresh-process `exec-one` reps ON vs OFF. The cold subset stays the gate for the persistence slices (2–4), whose win survives cold; only slice-1's is warm-disk. Parity is unchanged (result-invariant: warming caches and reusing an S3 client cannot change rows).

### 7.2 Engagement proof

`r2rml.prefetch` (allowlisted, callsite-verified) fires once per multi-table query with `requested`/`warmed` fields; its presence in the ON run proves the path executed and how many tables it overlapped, and OFF (switch-off, call site skipped) emits none. This is the direct engagement signal — necessary because ON/OFF `r2rml.load_table` counts are identical (n = #tables either way: prefetch warms the pin, the scan hits it; without prefetch the scan does the GET) and so cannot themselves distinguish "fired" from "no-op".

### 7.3 Residuals & findings

- **OAuth stays n=1 even under fully-concurrent prefetch (verified live).** `buffered` polls cooperatively on one task and the REST-client build is synchronous, so the first future builds + caches the process-wide client (and its OAuth token) before any other future resumes past its async nameservice lookup; every later table reuses them. No cold OAuth storm, and no serial-first warm is needed. **Fragility note:** if the client build ever becomes `async`, this dedup breaks and a serial first-table warm would be required — commented at the callsite.
- **Pre-existing defect, independently fixed by the fold-in:** the per-query `IcebergCatalogSession` pinned the loadTable *response* but not the `S3IcebergStorage` client built *from* it, so **every scan rebuilt the AWS SDK client** (`aws_config` load + S3 + HTTP client) — a correlated join re-scanning a dim paid it per re-scan, and naive prefetch paid it twice (warm + scan), which canceled the parallelization gain on q008. Slice 1 caches `Arc<S3IcebergStorage>` in the session keyed like the pin and reuses it; `store_load_table` invalidates it on any fresh loadTable so a **creds refresh rebuilds the client** (never serves a stale-creds client — unit-tested). This is the natural completion of the session-pin design, not scope creep.
- **Cross-pattern non-fused joins are a documented follow-up.** Slice-1's prefetch fires within a single operator (the fused chain, or one pattern's multi-TriplesMap set). A non-fused join across *separate* patterns resolves its tables in *separate* `R2rmlScanOperator`s that the join pulls serially, so an in-operator prefetch can't reach them; parallelizing that would need a query-plan-level prefetch (walk the plan for all R2RML tables, warm once before execution) — a bigger design for **un-measured** value (none of the cold subset hits that path). Deferred, noted here with the operator-boundary reason.

---

## 8. Slice-2 (DiskCatalogCache: persist metadata + scan_files + count-stats) — implementation

Persists the three SECRET-FREE, IMMUTABLE catalog layers to disk so a cold
process with a warm catalog dir skips the S3 metadata + manifest reads (it still
pays the one `loadTable` GET for fresh vended creds — ruling ii). New module
`fluree-db-api/src/graph_source/disk_catalog_cache.rs`; wired at the three read
sites in `r2rml.rs` (in-memory miss → disk → S3), switch
`FLUREE_ICEBERG_CATALOG_DISK_CACHE` (default on).

### 8.1 The pointer and the TTL are UNNECESSARY (§2.2 simplification)

§2.2 proposed persisting a `metadata_location` pointer under a drift-TTL. Under
the no-creds ruling the `loadTable` GET **always runs** (for creds) and returns
the CURRENT `metadata_location`, so the disk cache keyed by that content-addressed
location is trivially correct with **no pointer and no TTL**: a table commit
yields a new location = a new key = a clean miss; a given key's value is immutable
and can never go stale. So slice 2 is just three content-addressed stores keyed by
`metadata_location`:
- `TableMetadata` (serde-ready) — removes the `r2rml.read_metadata` S3 GET.
- `scan_files` (the unfiltered full file list; the in-memory cache is already
  bypassed when a pushdown filter prunes, so this is immutable per snapshot) —
  removes the scan path's `iceberg.scan_plan` manifest read.
- COUNT-path manifest stats (`data_files` + `has_delete_manifests`) — removes the
  `r2rml.count_manifest_read` manifest read (q036's ~450 ms slice).

`DataFile` (+ `FileFormat`, `PartitionData`) gained `Serialize/Deserialize` in
`fluree-db-iceberg` to persist the file lists (they derived only `Debug/Clone`);
`Arc`-wrapped fields are persisted as plain `Vec` (serde's `rc` feature is off).
Entries are serde_json (no bincode in the workspace).

### 8.2 Dedicated dir = the cold-data / warm-catalog gate, expressed

The cache lives in a dir **sibling** to the Parquet/binary `DiskArtifactCache`
(`<artifact_dir>-catalog`), never inside it. The vbench cold protocol clears the
pinned artifact dir (`clear_cold_cache` → `remove_dir_all`); the sibling survives.
So **two `--cold` runs against the same `--cache-dir`** are exactly the
cold-data/warm-catalog state: run 1 populates `<dir>-catalog`; run 2 clears the
data artifacts (cold data) but reads metadata/scan_files/count-stats from the warm
catalog sibling — `r2rml.read_metadata`, `iceberg.scan_plan`, and
`r2rml.count_manifest_read` drop to **n=0** on run 2. That span-count collapse (+
q036's wall dropping by the metadata+manifest slice, + run1==run2==oracle parity)
is slice-2's DoD gate. The cache is a pure optimization — every I/O or parse
failure degrades to a miss, never a query error — and hermetic round-trip tests
(`disk_catalog_cache::tests`) cover the serde path; the disabled switch and a
non-creatable dir both fall back to today's S3-every-cold behavior.

### 8.3 Robustness (lead cautions) — versioning, atomic writes, bounded dir

Content-addressing the KEY does not protect the VALUE layout across releases, so:
- **Versioned envelope.** Every entry is `{format_version, payload}`; `read`
  checks the version and treats a mismatch — or ANY deserialize failure — as a
  miss (and deletes the stale/corrupt file). `CACHE_FORMAT_VERSION` MUST be bumped
  whenever a persisted payload type changes, so an added `DataFile` field can't be
  silently misread from an old entry. Unit-tested (`version_mismatch_is_a_miss`,
  `corrupt_entry_is_a_miss`).
- **Atomic writes.** Entries are written to a `.tmp` sibling then `rename`d, so a
  crash mid-write can't leave a torn file a later read would trust (the torn temp
  is just orphaned).
- **Bounded dir.** At process startup (first `for_dir`, gated by a `OnceLock`) the
  dir is pruned oldest-first (by mtime) to `MAX_CACHE_BYTES` (512 MiB) — metadata
  entries are tiny but a ~7,670-file `scan_files` entry is not, and unbounded
  growth under `~/.fluree` would eventually be a support ticket.

The serde surface is narrow: `DataFile`/`FileFormat`/`PartitionData` hold only
primitives, enums with unit variants, and `HashMap<i32, …>` / `Vec<…>` of the
same — no borrowed or in-memory-only fields, so the derive is clean (no
purpose-built record needed). Persistence is serde_json (no bincode in-tree).
