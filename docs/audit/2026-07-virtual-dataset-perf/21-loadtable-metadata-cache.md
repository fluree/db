# loadTable-METADATA cache (creds-never-persisted) + F19 with_graph_ref ride-along — DESIGN SKETCH

**Branch:** to stack on `perf/r2rml-q031-refprune` (#1502) — or its own branch off it; lead's call.
**Status:** SKETCH — **STOP for lead review**. No engine code until approved.
**North-star slate item** (lead-approved next after F20). Closes the residual that F20 (q031) and PR-4d (#1501, q016) both bottom out on: the ~2–3 s per-process-per-table `loadTable` REST/OAuth cost, on top of the shared fact-scan/materialize floor. Substrate: PR-8 slice-2 disk cache (`disk_catalog_cache.rs`), the cross-query moka cache (`cache.rs`), `load_table_context` (`fluree-db-api/src/graph_source/r2rml.rs:1036`).
**AJ HARD CONSTRAINT (non-negotiable):** vended credentials are **NEVER persisted to disk**. Persist only the credential-free metadata response; creds are fetched in-memory, per-process, ONLY when an actual S3 byte-fetch is needed. A fully disk-cached query issues zero REST/OAuth/S3.

## (1) The seam (measured, code-anchored)

q031 in-protocol (corpus order) = 5.21 s with `load_table.n=0` (amortized); the **isolated first-ask** still pays `load_table.n=2` ≈ ~2–3 s (measured via q032 proxy in the F20 arithmetic-hole rider). That ~2–3 s is the `loadTable` REST/OAuth GET (`catalog.load_table`, `r2rml.rs:1158`, the `r2rml.load_table` span). **PR-8 slice-2 already removes the metadata.json + manifest S3 round-trips** (`disk_catalog_cache.rs` `get_metadata`/`get_scan_files`, content-addressed by `metadata_location`) — but its own doc-comment is explicit: *"a cold process still issues one loadTable GET for fresh vended credentials — this only removes the metadata + manifest S3 round-trips."* So the `loadTable` GET survives slice-2 for two reasons, only one of which is creds:

1. **The metadata_location pointer.** The disk cache is keyed BY `metadata_location`; you need the pointer to read it. Today the pointer comes only from the `loadTable` response (or the in-memory cross-query cache, `cache.rs:128`, 60 s TTL — cold-process-empty).
2. **The vended creds.** `load_table_context` builds `S3IcebergStorage` **eagerly** (`r2rml.rs:1221–1265`) from `load_response.credentials`, and returns it (`:1431`) BEFORE any disk-cache check. So even when metadata + scanfiles + parquet are all disk-resident, storage is constructed — which needs creds — which needs the GET.

**Prior art in the SAME cache (Direct mode already does half of this).** `CatalogConfig::Direct` resolves its pointer from `cache.get_direct_metadata_location` (`r2rml.rs:1292`) and synthesizes a `LoadTableResponse { credentials: None, .. }` (`:1300`) — a **credential-free metadata-location cache** (`cache.rs:110`, `direct_metadata_locations`, 2 s TTL, in-memory). REST mode has no equivalent; its cross-query cache holds the WHOLE response incl. creds and is 60 s-TTL'd *because* of them.

## (2) The fix — two coupled parts

**(a) Persist the credential-free metadata_location pointer for REST mode.** A new disk-cache entry `lt_key → metadata_location` (`lt_key` = `(graph_source_id, namespace, table)`, `IcebergCatalogSession::load_table_key`), written whenever a real `loadTable` returns, read before the GET on the resolution ladder (`r2rml.rs:1130` — insert a new rung between the cross-query moka hit and the real REST load). This is Direct mode's `direct_metadata_locations` pattern, made (i) persistent (survives a fresh process) and (ii) REST-keyed. **Contains ONLY the pointer string — no creds, no token, no config.**

**(b) Make storage / creds LAZY.** `load_table_context` today returns `(Arc<S3IcebergStorage>, Arc<TableMetadata>, String)` with storage already built. Change it to return a **lazy storage handle** — a `OnceCell`/closure that performs the `loadTable` GET (for creds + a pointer revalidation) and builds `S3IcebergStorage` **on first actual S3 need**. The three consumers each gate on a disk hit first:
- **metadata** — `disk_cache.get_metadata(&metadata_location)` (slice-2) using the persisted pointer → no GET, no storage.
- **scan planning** — `cache.get_scan_files(&metadata_location)` (`r2rml.rs:1505`, moka + slice-2) → the `SendScanPlanner` (`:1494`, which needs `storage`) is bypassed → no storage.
- **parquet bytes** — `DiskArtifactCache` (`:1439`, threaded into the readers) → a disk hit reads bytes from disk, not `storage` → no storage.

When ALL three hit (the warm-disk / priming steady state = AJ's bar), the lazy storage handle is **never forced** → **zero loadTable GET, zero OAuth, zero S3** → the ~2–3 s vanishes. A miss on ANY of them forces the handle: one `loadTable` GET (fresh creds + a current pointer), storage built, S3 read — exactly today's path, no regression.

## (3) SECURITY invariant (the load-bearing constraint)

- **Persisted payload = the pointer string ONLY.** A hermetic test asserts the on-disk `Envelope` for the new key deserializes to a bare `String` and that NO field of `LoadTableResponse.credentials` / config / token ever reaches `disk_cache.write`. (Mirror slice-2's version-tag + corrupt-entry-is-a-miss discipline, `disk_catalog_cache.rs:113–138`.)
- **Creds stay in-memory, per-process, per-actual-fetch.** The lazy handle builds storage from a FRESH `loadTable` GET each cold process; vended creds live only in the `S3IcebergStorage` Arc for the query's lifetime (today's model, `r2rml.rs:1226`). Nothing about creds changes except that they're fetched **later and only if needed**.
- **Disk-cache-served parquet ⇒ zero creds.** The whole point: no S3 fetch ⇒ the handle is never forced ⇒ no creds ever enter the process for that query.

## (4) Staleness (the one new persisted thing that CAN go stale)

Unlike slice-2's content-addressed entries (a new commit = new `metadata_location` = new key = clean miss, no invalidation), the `lt_key → metadata_location` POINTER can go stale (a table commit moves the pointer; the persisted one is now old). Handling (lead to choose):
- **A. Persist with a freshness TTL** (timestamp in the `Envelope`; older than TTL ⇒ ignore the persisted pointer, do the GET — which refreshes both pointer and creds). Mirrors the 60 s cross-query TTL, but persisted and creds-free. TTL tunable (`FLUREE_ICEBERG_LOADTABLE_META_TTL_SECS`), default generous (the demo warehouse is effectively immutable; a live table wants it short).
- **B. Revalidate cheaply** (a catalog HEAD / current-snapshot probe) — but that is ~the same round-trip we're removing, so only worth it if a HEAD is much cheaper than a full `loadTable`. Likely not; note and defer.
- **Recommend A.** Downstream soundness is automatic: IF the pointer is current, the content-addressed metadata/scanfiles/parquet are exactly right (immutable); the ONLY risk is serving a bounded-stale snapshot — the same tradeoff the existing 60 s cache already accepts, and consistent with AJ's ruling that the disk artifact cache is legitimate steady-state. A stale pointer never yields WRONG data for the snapshot it names; it can only lag a newer snapshot within the TTL.

## (5) Kill switch

`FLUREE_ICEBERG_LOADTABLE_META_CACHE` (default on). Off ⇒ the persisted-pointer rung is skipped and storage stays eager (today's behavior, byte-identical). The lazy-storage restructure is behaviorally transparent when the switch is off (the handle is forced immediately, reproducing the eager build).

## (6) DoD / gate

1. **Deterministic:** with a warm disk cache (priming done) + cold in-memory (fresh process / cleared moka), an isolated q031 (and q016) issues **`r2rml.load_table.n = 0` and `iceberg.oauth_token.n = 0`** — the crisp cache-independent proof the GET is gone. Switch off ⇒ back to `load_table.n = 2`.
2. **Wall:** isolated-first-ask q031 drops by the measured ~2–3 s (report; the remaining wall is the shared fact-scan/materialize floor = PR-2a territory, not this item).
3. **Security test (hermetic):** the persisted pointer payload contains no credential/token bytes; a forced-miss path still fetches fresh creds; a corrupt/oversized entry is a miss not an error (slice-2 parity).
4. **Staleness test (hermetic):** a moved pointer past TTL triggers a GET; within TTL serves the persisted pointer; a stale pointer never returns data for the wrong `metadata_location`.
5. **Full-corpus baseline at head:** oracle 0-mismatch; no query's wall/hash regresses; the isolated-first-ask improvement shows on the load-bearing tail (q031/q016/q038/q029 — all `load_table.n>0` cold).
6. Native untouched (no R2RML path change for native); kill-switch off = byte-identical.

## (7) Blast radius

Every REST-catalog virtual query benefits on a warm-disk first-ask (fewer/zero loadTable GETs); Direct mode already had the pointer cache (this brings REST to parity + persistence). No result can change — the pointer only selects WHICH immutable snapshot to read, and the (4) staleness bound is the same one the existing cache accepts. The lazy-storage restructure is the only broad-touch change (`load_table_context`'s return type + its ~3 consumers) — contained to `r2rml.rs`; the COUNT(*) shortcut (`:929`) shares `load_table_context`, so it inherits the laziness (a manifest-cached count also goes GET-free).

## (8) RIDE-ALONG — F19 residual: `with_graph_ref` parent-memo clone

Per the F20/F19 close-out, the one sound residual F19 fix rides the next PR touching this subsystem — this is it. **Change:** `context.rs:1222` (`with_graph_ref`) clones `self.r2rml_parent_memo` instead of `::default()` (keeping `const_sid_cache` fresh — the store-switch reason for the reset applies to the store-implicit const cache, NOT the parent-memo, whose key is store-disambiguated `(graph_source_id, parent_tm, cols, as_of_t)`, `operator.rs:75`). **Test (hermetic):** derive a ctx per rebuild via `with_graph_ref` (a minimal `GraphRef` over a genesis snapshot) and assert the DIM parent is scanned once across N rebuilds — the `with_graph_ref` analog of `pr8b::parent_lookup_survives_operator_rebuild`, which today only covers `with_active_graph`. Benefits only SERVICE / multi-source-default R2RML (no corpus query), so it ships gate-neutral; included here for subsystem locality, clearly separated as its own commit.

## (9) Open questions for the lead — STOP

(i) Staleness policy: TTL (A, recommended) vs a revalidation probe (B) vs pointer-cache-only-when-a-freshness-env-is-set? (ii) Is the lazy-storage restructure in-scope for THIS PR, or split — ship the persisted pointer first (still needs lazy storage to pay off, so probably atomic)? (iii) Confirm the switch name + the TTL env name. (iv) Should the persisted pointer live in `disk_catalog_cache.rs` (new `get/put_metadata_location`, same `Envelope`+version discipline) — I recommend yes, one cache home. (v) Ride-along `with_graph_ref` fix as a separate commit in this PR — confirm.
