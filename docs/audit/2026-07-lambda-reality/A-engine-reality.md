# Track A — Engine-side reality mapping for the Lambda runtime

**Scope.** Evidence only, no remedies. Every claim carries a `file:line` receipt against
the branch `assess/lambda-reality` (`210cf4833`, off `origin/feat/external-s3-iceberg` —
the BYO-IAM ambient-credential work stacked above the perf wave head `c948f0a2b`). No code
changes; no live Snowflake.

**The runtime we are mapping onto (given).** `fluree-db-api` runs embedded inside the
solo AWS Lambda: 10 GB ephemeral `/tmp` per container (a cold container starts empty),
900 s Lambda timeout but a ~55 s effective chat-path cap (CloudFront 60 s), X-Ray Active,
`FLUREE_DISK_CACHE_BUDGET_BYTES=8GiB` set (its CFN comment says "binary index artifacts"),
and **no** `FLUREE_ICEBERG_*` / `FLUREE_R2RML_*` env vars set. AJ's live chat tests still
time out despite the perf wave.

**The two facts that frame everything below:**

1. `FLUREE_DISK_CACHE_BUDGET_BYTES` governs **both** the native binary-index cache **and**
   the Iceberg Parquet data-file cache (they are the *same* budgeted cache instance in the
   *same* directory) — but it does **not** govern the Iceberg *catalog* cache (metadata /
   manifest / scan-files / loadTable pointer), which has a **separate, hardcoded 512 MiB
   cap**. The CFN comment is therefore incomplete.

2. The perf wave's "first-ask / cache-thrashed" bar was measured **cold-data / warm-catalog**
   by construction. A cold Lambda container is **cold-data / cold-catalog** — it additionally
   pays the loadTable GET + metadata + manifest round-trips that every north-star number got
   for free. The ≤3 s bar assumes a warm `-catalog` dir that a cold container does not have.

---

## (1) Env / config inventory — every knob the Iceberg/R2RML query path reads

### 1a. The budgeted disk cache — `DiskArtifactCache` (headline)

`fluree-db-core/src/disk_cache.rs` is the process-wide, content-addressed, byte-budgeted,
LRU-by-mtime disk cache.

- **Budget source.** `FLUREE_DISK_CACHE_BUDGET_BYTES` is read **per cache instance** in
  `DiskArtifactCache::new` (`disk_cache.rs:216`). `0` disables writes (`:218`); a valid
  integer is the byte budget (`:225`); unset falls to `configured_or_auto(available)`
  (`:243`), which is either the process-global configured default
  (`set_configured_budget_bytes`, `:35`) or **90 % of available disk**
  (`CACHE_BUDGET_NUMERATOR/DENOMINATOR = 9/10`, `:13-14`, applied `:40-47`).
- **Eviction.** Over budget → evict oldest-by-mtime down to the 80 % low-water mark
  (`CACHE_EVICT = 8/10`, `:15-16`; `low_water_mark` `:285`; `ensure_capacity` `:350`;
  `evict_until` `:313`). Eviction has **no in-use pinning** — a file can be evicted mid-read
  (the Parquet reader tolerates this by falling back to source, see
  `send_parquet.rs:222-226`).
- **Who routes through it.**
  - Native binary index: `fluree-db-binary-index/src/read/artifact_cache.rs:7-8`
    re-exports the core type; `binary_index_store.rs:366,3003` build it for the leaflet /
    forward-pack fetch path.
  - **Iceberg Parquet data files:** `fluree-db-iceberg/src/io/send_parquet.rs:23` imports it;
    `fluree-db-api/src/graph_source/r2rml.rs:1780-1781` builds it via
    `DiskArtifactCache::for_dir(&self.fluree.binary_store_cache_dir())` and hands it to the
    scan (`r2rml.rs:2008-2009`, `:2091-2092`, `:2152-2153`).
  - The `disk_cache_max_mb` builder that seeds the configured default is **`#[cfg(feature =
    "native")]`** (`fluree-db-api/src/lib.rs:1811-1817`) — a non-`native` build silently
    drops it — but the **env override is read unconditionally in core**, so it still applies.
- **Singleton per dir.** `for_dir` returns a shared `Arc` keyed by directory
  (`disk_cache.rs:180-190`, `CACHE_REGISTRY`). The native index and the Iceberg Parquet cache
  both target `binary_store_cache_dir()` (below), so they are **one instance sharing one
  budget in one directory** — the 8 GiB is split between leaflets and Parquet, not 8 GiB each.
- **Built-in Lambda tell.** `disk_cache.rs:17-18` names `DEFAULT_LAMBDA_TMP_BYTES = 512 MiB`
  and `:246-256` warns when available `/tmp` ≤ 512+64 MiB — the code already anticipates the
  Lambda `/tmp` shape.

> **Headline answer:** `FLUREE_DISK_CACHE_BUDGET_BYTES` is **not** binary-index-only. It caps
> the shared `/tmp/fluree_binary_cache` directory that holds **both** native leaflets **and**
> Iceberg Parquet files. It does **not** reach the catalog cache (§1b).

### 1b. The catalog disk cache — `DiskCatalogCache` (separate, hardcoded cap)

`fluree-db-api/src/graph_source/disk_catalog_cache.rs` — a **sibling** cache, explicitly "next
to `DiskArtifactCache`, never inside it" (`:13-16`).

- **Cap: hardcoded `MAX_CACHE_BYTES = 512 * 1024 * 1024`** (`:239`) — **not** env-configurable,
  **not** under `FLUREE_DISK_CACHE_BUDGET_BYTES`. Pruned oldest-first **once per process at
  startup** (`for_dir` + `PRUNED` OnceLock, `:302-308`; `prune_dir` `:107-136`).
- **Path: `{binary_store_cache_dir}-catalog`** (`catalog_cache_dir`, `:94-102`) →
  `/tmp/fluree_binary_cache-catalog` in Lambda. Sibling so the cold-benchmark protocol can
  clear data while keeping catalog (`:14-16`).
- **Holds** (all credential-free): parsed `TableMetadata` (`get/put_metadata` `:378-391`),
  manifest-derived scan-file list `CachedScanFiles` (`:393-417`), COUNT(\*) stats
  (`:419-441`), and the loadTable `metadata_location` pointer (`:451-492`). Keyed by the
  content-addressed `metadata_location` (except the pointer, keyed by `lt_key`), so a table
  commit yields a new key = clean miss, no invalidation (`:5-11`).
- **Switches:** master `FLUREE_ICEBERG_CATALOG_DISK_CACHE` (`:34`, default on); pointer
  `FLUREE_ICEBERG_LOADTABLE_PTR_CACHE` (`:52`, default on); pointer TTL
  `FLUREE_ICEBERG_LOADTABLE_PTR_TTL_SECS` (`:70-79`, **default 300 s**, `0` disables).
- `CACHE_FORMAT_VERSION = 2` (`:214`); on-disk names carry a stable xxh64 key hash + a
  `CACHE_SCOPE = "shared"` segment (`:254-286`).

### 1c. In-memory caches — `R2rmlCache` (per-Fluree-instance, container-only)

`fluree-db-api/src/graph_source/cache.rs`, all `moka::sync::Cache` (in-memory):

| Cache | Cap | TTL | Env |
|---|---|---|---|
| `compiled_mappings` | 64 | — | — |
| `table_metadata` | 128 | — | — |
| `scan_files` | 128 | — | — |
| `parquet_footers` | `max(128/2,32)=64` | — | — (PR-2 footer cache) |
| `direct_metadata_locations` | 128 | **2 s** (`DIRECT_METADATA_LOCATION_TTL`, `:32`) | — |
| `rest_clients` | 64 | **900 s** (`DEFAULT_REST_CLIENT_TTL_SECS`, `:62`) | `FLUREE_ICEBERG_REST_CLIENT_TTL_SECS` |
| `rest_load_tables` | 128 | **60 s** (`DEFAULT_REST_LOADTABLE_TTL_SECS`, `:42`) | `FLUREE_ICEBERG_LOADTABLE_TTL_SECS` |

Caps/TTLs wired at `cache.rs:150-190`. Also `FLUREE_ICEBERG_LOADTABLE_CACHE` toggles the
in-memory loadTable cache (per `SWITCHES.md`). **All per-container: empty on a cold container,
and gone the moment the `Fluree` instance is dropped** (the cache hangs off `Fluree`, reached
via `self.fluree.r2rml_cache()`, e.g. `r2rml.rs:1261,1370,1472`). Whether solo keeps one
`Fluree` alive across invocations is a cross-track question for lambda-audit; if it rebuilds
per invocation, even a *warm* container loses these.

### 1d. Concurrency, retry, and memory knobs

- **Scan concurrency** `FLUREE_ICEBERG_SCAN_CONCURRENCY` (`r2rml.rs:72-84`): default
  `min(available_parallelism, num_files).clamp(1, 32)`; override is uncapped. **Unset in the
  stack** → runs at the clamped `available_parallelism`. See §4.
- **Catalog concurrency** `FLUREE_ICEBERG_CATALOG_CONCURRENCY` (`catalog/rest.rs:65`):
  **default 8** (`DEFAULT_CATALOG_CONCURRENCY`, `:52`) — a process-wide semaphore bounding
  concurrent catalog ops (loadTable / manifest GETs).
- **Catalog retry/backoff** `FLUREE_ICEBERG_CATALOG_MAX_RETRIES` (default 4, `:54`),
  `FLUREE_ICEBERG_CATALOG_BACKOFF_BASE_MS` (default 250, cap 8000, `:56-57`).
- **Materialize window** `FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS` (`operator.rs:209-217`):
  default **512 K rows** resident.
- **Native in-memory cache budget** `cache_max_mb` (`lib.rs:1799`): default = a tiered
  fraction of **cgroup-clamped** RAM (`fluree-db-connection/src/cache.rs:40-68` — 30 %/40 %/35 %
  by size tier; clamp via `fluree_db_core::sysmem::effective_memory_limit_bytes`, which reads
  `/sys/fs/cgroup/memory.max` — `sysmem.rs:14-34,54`). **cgroup-aware** → a 10 GB Lambda sizes
  this to ~35 % ≈ 3.5 GB (native leaflet/object cache; largely idle for a pure virtual query,
  but it does claim that share of the memory limit). Contrast: `available_parallelism` (§4) is
  `std`, **not** routed through this cgroup helper.
- **Parquet read policy** (`send_parquet.rs`): whole-file admission ≤ 32 MiB
  (`WHOLE_FILE_MAX_BYTES`, `:103`) or ≥ 50 % projection (`:107`, `admit_whole_file` `:150`);
  footer-from-cache `FLUREE_ICEBERG_FOOTER_FROM_CACHE` (`:177`, default on); row-group/row
  predicate pushdown `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` (`:51`, default on); numeric-stats
  pruning `FLUREE_ICEBERG_NUMERIC_STATS` (`r2rml.rs:115`, default on). At SF01 the fact table
  is ~51 MB / 7 670 files ≈ **6.6 KB/file**, so **every fact file is cached whole** to
  `/tmp/fluree_binary_cache`.

### 1e. Cache root derivation — where a Lambda-embedded Fluree lands its caches

`Fluree::binary_store_cache_dir()` (`lib.rs:3466-3471`) returns the ledger manager's
configured `cache_dir`, else **`LedgerManagerConfig::default().cache_dir`**, which is
**`std::env::temp_dir().join("fluree_binary_cache")`** (`ledger_manager.rs:733`; doc `:709`
says "Default: `$TMPDIR/fluree_binary_cache`"). In Lambda `std::env::temp_dir()` is `/tmp`
(TMPDIR unset), so:

- Parquet + binary-index budgeted cache → **`/tmp/fluree_binary_cache`** (8 GiB budget).
- Catalog cache → **`/tmp/fluree_binary_cache-catalog`** (512 MiB hardcoded cap).

Both live in the **10 GB per-container ephemeral `/tmp`**: **per-container-persistent** (survive
warm invocations of the same container), **empty on a cold container**. Worst-case disk residency
≈ 8 GiB + 512 MiB = 8.5 GiB, under 10 GB but not by much once the deployment's own `/tmp`
scratch is counted. If solo sets `LedgerManagerConfig.cache_dir` to somewhere else, that
overrides the default — **flag for lambda-audit: confirm solo's configured `cache_dir` (or that
it is unset ⇒ `/tmp/fluree_binary_cache`).**

---

## (2) Cold-container first-ask arithmetic

### 2a. The protocol gap (why the north-star numbers don't transfer)

The epic's cold protocol clears the **data** artifact dir but keeps the **`-catalog`** dir warm
(`disk_catalog_cache.rs:13-16`: "clear the data artifact cache while KEEPING catalog
persistence — that cold-data / warm-catalog state is slice 2's DoD gate"). So every north-star
"first-ask" number was measured with the loadTable pointer + metadata + scan-files already on
disk. A **cold Lambda container has neither**.

**Cold-container per-table sequence** (from `load_table_context`, `r2rml.rs:1322-1355`, +
`metadata_from_caches` `:1260-1272`, + `disk_catalog_cache`):

| Step | Cold container (empty caches) | Warm-catalog first-ask (dev) |
|---|---|---|
| 1. pointer resolve | session pin MISS + disk pointer MISS (`get_metadata_location` `:1354`) | **HIT** (disk pointer, within 300 s) |
| 2. loadTable GET | **REST GET** — OAuth token exchange + GET, vended creds + `metadata_location` (~1–3 s/table) | skipped (`LazyS3Storage`, never forced) |
| 3. metadata.json | **S3 fetch + parse** (`get_metadata` MISS) | **HIT** (disk catalog) |
| 4. manifest → scan files | **manifest-list + manifest reads → derive scan-file list** (`get_scan_files` MISS) | **HIT** (disk catalog) |
| 5. data sweep | **fetch selected Parquet files at concurrency C** | fetch (cold-data) / HIT (pure-warm) |

Warm-catalog first-ask = **step 5 only**. Cold container = **steps 1–5, every table**. Steps
1–4 are the epic's "~7 s fixed" per fact table (q029 model: "~7 s fixed (loadTable+oauth+
scan_plan)", F17 memory); catalog fan-out across N tables is bounded at
`CATALOG_CONCURRENCY = 8` (`rest.rs:65`).

### 2b. Per-query cold-container estimates (SF01)

Anchored to the epic's measured components (dev machine, dev concurrency ≈ 10–16) with the two
Lambda multipliers applied: **(A) cold catalog is fully paid** (not amortized by a warm
`-catalog` dir), and **(B) the data sweep runs at Lambda concurrency ≈ 6** (§4), ~2× the dev
sweep because the per-file cost is S3-round-trip latency, not CPU (`send_parquet.rs` policy;
doc 06). These are order-of-magnitude, clearly-labelled estimates, not measurements.

| Query | Shape (post-wave plan) | Warm-catalog in-protocol (measured) | Cold-container Lambda estimate | Over 55 s cap? |
|---|---|---|---|---|
| **q029** | UNION of 2×FACT_WEB_EVENT, F17 → **1 sweep** | 2.61 s | ~7 s catalog + 1 sweep. Dev 1-sweep isolated ≈ 57 s (F17) → Lambda ≈ **~110–120 s** | **Yes** |
| **q031** | InventorySnapshot star, F20 → **2 loadTables** (fact + DIM_PRODUCT) | 5.22 s | 2× (loadTable+meta+manifest ≈ 3–7 s) + 1 fact sweep (7 670 files @ C≈6) → **~60–90 s** | **Yes** |
| **q016** | batched OPTIONAL, PR-4d → 3 scans; residual = 1 FACT_SHIPMENT sweep + ~11 s hash-join/materialize | 20.2 s | ~7 s catalog + fact sweep (Lambda ≈ 2×) + ~11 s join → **~60–100 s** | **Yes** |
| **q055** | `?s ?p ?o LIMIT 5` — all 16 TMs, all-table loadTable wave | 2.44 s | **16×** cold loadTable+meta+manifest (bounded at CATALOG_CONCURRENCY 8) + small data → **~25–45 s** | Borderline |
| **q056** | predicate-less `COUNT(*)` — un-fused, full object fetch over ~69 046 files (all 16 TMs) | **168 s** | already 168 s **with warm catalog**; cold adds the 16-table catalog cost and runs the whole fetch at C≈6 → **well past any cap** (minutes) | **Yes (hard)** |

The pattern: every fact-scanning or wide-fan-out query that is ≤ low-seconds **warm-catalog** on
dev crosses the 55 s chat cap **cold-container** in Lambda, because the two multipliers stack —
the cold catalog cost that the bar was measured to exclude, times a data sweep at half the
concurrency.

### 2c. SF20 scaling (~20× data)

The dominant cold cost is **file count**, not bytes (39 rows/file; fetch-bound). SF20 ≈ 20× the
files. loadTable/metadata/manifest per table grows sub-linearly (one GET, a larger manifest
tree), but **the data sweep grows ~linearly in file count**: an SF20 fact ≈ 150 K files → at
C≈6 the sweep alone is ~20× the SF01 sweep. Plan-level pruning that cuts *files fetched*
(PR-5 top-k 99.87 %, PR-7 numeric row-group prune, PR-3 star prune) is what keeps SF20 tractable
cold; anything that still reads the whole fact (q016/q029/q031/q056 tails) scales linearly and is
hopeless within 55 s at SF20 on a cold container. Also note the **512 MiB catalog cap**
(`disk_catalog_cache.rs:239`): an SF20 scan-files entry for a ~150 K-file table is far larger
than SF01's, so the catalog dir can start evicting entries it will want again — a warm-catalog
container can partially cold-miss at SF20. And the shared 8 GiB Parquet budget begins to matter:
SF20 across 16 TMs can approach it, triggering mtime eviction mid-workload.

### 2d. Which shipped wins survive a cold container

| **Survive cold** (plan-level — cut *planned* work regardless of cache state) | **Warm-container-only** (evaporate when `/tmp` is empty) |
|---|---|
| **PR-3** star_tm_prune — 16→3 tables (`FLUREE_R2RML_STAR_TM_PRUNE`): fewer loadTables + scans | **#1503** loadTable pointer cache (disk `-catalog`): cold ⇒ pays the GET; even warm, TTL-window-bounded 300 s (F21) |
| **PR-5** scan top-k — q046 99.87 % files pruned (`FLUREE_R2RML_TOPK_PUSHDOWN`): cuts the data sweep itself — **biggest cold win** | **PR-8 slice-2** disk catalog cache (metadata/manifest/scanfiles): cold ⇒ full metadata+manifest S3 |
| **PR-7** numeric row-group prune (`FLUREE_ICEBERG_NUMERIC_STATS`): fewer files fetched | **PR-2** footer cache (in-memory, 64 entries): cold ⇒ empty |
| **PR-6** fused aggregate — Σ from manifest record counts, no data sweep (`FLUREE_FUSED_R2RML_AGG`) | **Parquet artifact cache** (`/tmp/fluree_binary_cache`): cold ⇒ full data sweep from S3 |
| **F20** ref-target-prune — q031 7→2 loadTables (`FLUREE_R2RML_REF_TARGET_PRUNE`) | **In-memory moka** (loadTable 60 s / rest_client 900 s / table_metadata / scan_files): cold ⇒ empty |
| **F17** UNION/BIND budget — q029 2 sweeps→1 (`FLUREE_R2RML_UNION_BUDGET`) | |
| **PR-4d** OPTIONAL seed-coalesce — q016 182→3 scans (`FLUREE_R2RML_OPTIONAL_SEED_COALESCE`) | |
| **PR-4b/4c** batched OPTIONAL; **PR-8b** parent memo / **F19** (fewer intra-query re-scans) | |

Worked example — **q031's 72 s→5.2 s** is F20 (plan prune, *survives*) **plus** the loadTable
cache + disk-catalog + data cache (*all warm-only*). On a cold container only the F20 part
remains: 2 loadTables instead of 7, but each of those 2 pays the full cold GET + metadata +
manifest + data sweep. The headline "≤3 s first-ask" wins that came from caching, not planning,
**do not exist on a cold Lambda container.**

---

## (3) Timeout / abort wiring (engine side)

### 3a. Cancellation is a cooperative atomic flag with no built-in timer

`fluree-db-core/src/cancellation.rs`: `QueryCancellation` is a single `AtomicU8` reason
(`:60-63`), cooperative, **no timer, no tokio integration** — the module doc is explicit:
"Timeout and disconnect detection are external concerns: callers decide when to signal this
handle" (`:66-69`). Reasons: `Cancelled`/`Timeout`/`ClientDisconnected` (`:17-24`). Observed via
`reason()` (a relaxed atomic load, `:113-123`); `check_cancelled` (`context.rs:685-690`) is a
**pure atomic read**, no deadline comparison.

### 3b. The embedded single-query path installs no deadline of its own

Traced in full (Explore sub-agent, receipts verified):

- **No `QueryCancellation` is created inside `fluree-db-api`.** Repo-wide `QueryCancellation::new()`
  exists only in `fluree-db-server/src/query_control.rs:58`,
  `fluree-db-server/src/routes/stream_query.rs:693`, the bench (`fluree-bench-virtual/src/exec.rs:265`),
  and core tests. On the embedded path the handle is a plumbed `Option` defaulting to `None`
  (`fluree-db-api/src/query/mod.rs:29`, `QueryExecutionOptions.cancellation`), copied into the
  context (`view/mod.rs:97`) and installed only if `Some`
  (`fluree-db-query/src/execute/runner.rs:828-829`; else `QueryCancellation::disabled()`,
  `context.rs:318`). The single-query entry passes `QueryExecutionOptions::default()` ⇒ `None`
  (`view/query.rs:76`); nothing in the crate calls the `with_cancellation` setter.
- **No timer, no timeout field.** `fluree-db-api/src` has **zero** `cancel_with(...)` calls, so
  nothing ever flips the flag to `Timeout`. `QueryExecutionOptions` has **no** `timeout`/`timeout_ms`
  field (`query/mod.rs:27-43`), and there is **no** `x-query-timeout-secs` handling in
  `fluree-db-api` (that header lives in `fluree-db-server`). `timeoutMs` is honored **only** on
  the multi-query envelope path (below).

> **The embedded engine does not time itself out.** A per-query deadline exists **only** if the
> embedder (solo's Lambda handler) constructs a `QueryCancellation`, arms its own timer to
> `cancel_with(Timeout)`, and passes it in via `with_cancellation`. **Flag for lambda-audit:
> does solo do this?** (The `#[doc(hidden)] with_lifecycle_guard`, `query/mod.rs:80-86`, exists
> precisely so an embedder can keep its own timeout task alive — but nothing in-crate uses it.)

### 3c. The multi-query path has a hard wall-clock timeout (but it still leaks)

`fluree-db-api/src/query/multi/dispatch.rs:542` wraps each sub-query in
`tokio::time::timeout(effective, exec)`, `effective = min(opts.timeoutMs, remaining envelope
budget)` (`:522-529`; envelope `timeoutMs` clamped `:207-211`). On elapse it drops the future
and returns `AliasOutcomeKind::Timeout` (`:571`). This drops the cooperative future — but see 3e.

### 3d. The Iceberg scan never polls cancellation mid-sweep

`check_cancelled()` is called only by operators — `join.rs` (~20 sites), `sort.rs`
(`:653-748`), and the **R2RML operator** at `operator.rs:909, 1090, 1332, 2377` (per-batch /
per-POM, e.g. "cancellation checkpoint before each FK-parent scan", `:1086-1090`). **There is no
`check_cancelled` anywhere in `fluree-db-api/src/graph_source` or `fluree-db-iceberg/src`** —
the Parquet fan-out (the S3-fetch-bound phase that dominates the cold wall) does **not** poll
the flag. Cancellation is observed only *between* scan-stream pulls by the consuming operator.

### 3e. tokio-spawned Parquet reads outlive a cancelled query

The parallel scan spawns each file read via **`tokio::spawn`** (`r2rml.rs:2148`, top-k tail
`:2087`) and awaits the `JoinHandle`, `buffer_unordered(concurrency)` (`:2173`, `:2112`). When
the query future is dropped (timeout or unwind), `buffer_unordered` drops its in-flight inner
futures — each was awaiting a `JoinHandle`, and **dropping a `JoinHandle` detaches the task, it
does not abort it**. So up to `concurrency` (~6) Parquet reads **run to completion** after the
query is gone — each finishing its S3 fetch + decode + best-effort disk-cache write
(`disk_cache.rs:530`). It is **bounded** to the in-flight batch (unstarted files are never
spawned because the stream stops being polled), not the whole table — but it is real burn after
abandonment, and it warms the disk cache for a query nobody is waiting on.

### 3f. HTTP disconnect is invisible during the scan phase

Engine-side, the embedded path detects a dropped client only when a row batch is pushed to the
response channel and the send fails: `view/stream_query.rs:534-538` maps a closed `mpsc` to
`QueryError::Cancelled { ClientDisconnected }`. That fires **only at row-emission time**. For a
query whose whole wall is spent *before the first output row* — the cold fact-scan / full
materialization (q056, big fact sweeps) — the sink's `push` is never called, so **a disconnect
is never observed** and the scan runs to completion. (The standalone `fluree-db-server` has an
independent disconnect guard, `query_control.rs:47` — but solo embeds the api, not the server.)

> **Product answer — "chat abandons at ~55 s, does the engine keep burning?"** With the current
> engine wiring: **yes.** The embedded single-query path arms no deadline itself (3b); a
> CloudFront/chat disconnect is invisible until a row is emitted (3f), which for the timing-out
> fact-scan queries never happens before the scan finishes; the fetch-bound scan never polls the
> flag mid-sweep (3d); and even if solo *does* arm a timer (3c-style) or pass a handle, the
> in-flight ~6 Parquet reads finish anyway (3e) and cancellation only takes effect at the next
> operator checkpoint. Whether solo arms *any* of this is lambda-audit's track — but nothing in
> the engine makes it automatic.

---

## (4) Concurrency shape

`iceberg_scan_concurrency` (`r2rml.rs:72-84`): with `FLUREE_ICEBERG_SCAN_CONCURRENCY` unset (the
stack's state), the default is `min(available_parallelism, num_files).clamp(1, 32)`. The doc
(`:52-71`) notes the ceiling was raised 8→32 (PR-2 Lever B) and that **per-file cost is fixed S3
round-trip latency, not CPU** — so concurrency is a direct multiplier on the fetch-bound wall.

- **Lambda vs dev.** A 10 GB Lambda ≈ 6 vCPUs; the dev machine where the wins were measured has
  ≈ 10–16. So the default scan concurrency in Lambda is **≈ 6, roughly half the dev value**, and
  the 32-ceiling-reaching override is **unset** → **data sweeps run ~2× longer in Lambda** than
  in every north-star measurement. This is the single most under-appreciated multiplier in the
  cold arithmetic (§2b) and it compounds at SF20.
- **`available_parallelism` caveat (cross-check for lambda-audit).** Unlike the memory budget
  (which *is* cgroup-aware via `sysmem.rs`), `iceberg_scan_concurrency` uses
  `std::thread::available_parallelism()` (`r2rml.rs:80-82`), **not** the Fluree cgroup helper.
  Whether that reflects the Lambda vCPU allotment (cgroup `cpu.max`) or the host CPU count
  (`sched_getaffinity`) is genuinely uncertain in Lambda and Rust-version-dependent. **Confirm
  the real value via X-Ray / a probe** — if it reads the *host* count it could be *higher* than 6
  (helping), or if the cgroup throttles CPU the effective throughput is still ~6 vCPU regardless
  of the count `available_parallelism` returns. Setting `FLUREE_ICEBERG_SCAN_CONCURRENCY`
  explicitly removes the guesswork; it is currently unset.
- **Catalog concurrency** is a fixed **8** (`rest.rs:65`), so a wide first-ask (q055's 16-table
  loadTable wave) fans its cold catalog GETs out 8-at-a-time.
- **Runtime shape (cross-ref lambda-audit).** The Parquet reads `tokio::spawn` onto whatever
  runtime the Lambda handler built — that runtime construction is in **solo**, not this repo. If
  solo builds a **single-thread** runtime (or a multi-thread one with `worker_threads` < vCPUs),
  the "parallel" `buffer_unordered` fan-out is serialized on the executor regardless of the
  concurrency number, and scan concurrency is moot. **Flag for lambda-audit: confirm solo's
  tokio runtime flavor + worker_threads for the chat handler.**

---

## Cross-track flags for lambda-audit (deployed-stack forensics)

1. Solo's configured `LedgerManagerConfig.cache_dir` — is it unset (⇒ `/tmp/fluree_binary_cache`)
   or overridden? (§1e)
2. Does one `Fluree` instance persist across warm invocations, or is it rebuilt per request?
   (governs whether §1c in-memory caches survive warm-container reuse) (§1c)
3. Does the chat handler construct a `QueryCancellation` + arm a ~55 s `cancel_with(Timeout)`
   timer + pass it via `with_cancellation`? (§3b) If not, the engine never self-cancels.
4. Tokio runtime flavor + `worker_threads` for the Lambda handler. (§4)
5. Real `available_parallelism()` value inside the 10 GB Lambda (X-Ray / probe). (§4)
6. Is `FLUREE_DISK_CACHE_BUDGET_BYTES=8GiB` right for a 10 GB `/tmp` given it is **shared** by
   Parquet + binary index, with the catalog cache's 512 MiB **on top**? (§1a/§1b)

---

# Cross-track reconciliation — does the query Lambda have an Iceberg disk cache? (Track A × Track B)

Track A (§1a) said the Iceberg parquet cache is the shared budgeted `DiskArtifactCache` at
`/tmp/fluree_binary_cache`, env-governed. Track B (§2) said "there is no Iceberg/parquet disk
cache configured at all in the query Lambda … `FLUREE_DISK_CACHE_BUDGET_BYTES` … is read *nowhere*
by the query Lambda … irrelevant to Iceberg query perf." Discriminated at the **wiring level**,
both observations are correct about their own layer, but **Track B's conclusion is not**.

**The wiring chain (deployed query Lambda, both repos at the shipped pin):**

1. **Solo configures no cache — TRUE.** `build_connection_config` (solo
   `fluree-lambda-common/src/connection.rs:46-103`) emits JSON-LD with only `commitStorage` +
   `indexStorage` + `dynamodbNs` + indexing defaults — **no `cache_dir`, no disk budget, no Iceberg
   cache config.** Built via `FlureeBuilder::from_json_ld(&conn_config).build_client()`
   (`connection.rs:127-131`). Track B is right that *solo* wires no Iceberg cache.
2. **The engine wires it anyway.** With no `cache_dir` configured, `binary_store_cache_dir()`
   returns `LedgerManagerConfig::default().cache_dir = std::env::temp_dir()/"fluree_binary_cache"`
   = **`/tmp/fluree_binary_cache`** (db `lib.rs:3466-3471`, `ledger_manager.rs:728-736`). Every
   Iceberg scan constructs `DiskArtifactCache::for_dir(&binary_store_cache_dir())`
   (db `r2rml.rs:1780-1781`) and the sibling `DiskCatalogCache` at `{..}-catalog`
   (`r2rml.rs:1238-1243`, `disk_catalog_cache.rs:94-102`).
3. **The env var DOES reach the parquet cache — via the engine, not solo.** `DiskArtifactCache::new`
   reads `FLUREE_DISK_CACHE_BUDGET_BYTES` **unconditionally** (db `disk_cache.rs:216`), and the
   deployed query Lambda env **has `FLUREE_DISK_CACHE_BUDGET_BYTES=8589934592`** (Track B §3 table).
   So the deployed query Lambda's Iceberg parquet cache is constructed at `/tmp/fluree_binary_cache`
   with an **8 GiB budget**, and the catalog cache at `/tmp/fluree_binary_cache-catalog` (512 MiB).
4. **Track B's own logs prove the scan path runs it:** "Starting Iceberg table scan …" +
   "Scan planning complete files_selected=… files_pruned=…" (Track B §4) are emitted by the *same*
   `scan_table_inner` that builds the cache one line earlier.

> **Track B's specific claim is wrong; its instinct is right.** "The env var is read nowhere by the
> query Lambda / irrelevant to Iceberg" was grepped over the **solo source tree only**, missing
> `fluree-db-api` (a git dependency compiled *into* the query binary), which reads it and applies it
> to the Iceberg parquet cache. The knob is **not** irrelevant — it sets the parquet cache budget.
> **But** Track B's *effective* conclusion — "the cache buys nothing here" — is correct, for reasons
> other than "not configured":
> 1. Per-container `/tmp` is **cold on every cold container**; the conversation used **16 distinct
>    containers** (Track B §5) → 16 cold caches.
> 2. Heavy scans are **cancelled at 55 s** (deadline guard) before a full warm file set is written;
>    a cancelled scan leaves at most a partial set.
> 3. **Cross-region** (us-east-1 Lambda → us-east-2 parquet) dominates the *first* (cold) read —
>    the only read most cancelled queries reach.
> 4. `DW_SVL.DIM_PRODUCT` is `files_selected=1` (a single large file); the second scan's narrow
>    `[PRODUCT_KEY]` projection of a >32 MB file is **not** whole-file-admitted
>    (`send_parquet.rs:103,150`) → range-reads from source, uncached even warm.

**Verdict for the slate** (the team-lead's exact fork — config-only / solo-wiring / engine change):
**"enable the cache" is NOT a fix — it is already enabled** (engine default + env budget). The
cache's problem is not existence but **persistence and warming**: per-container ephemeral `/tmp` is
cold on every cold container, scans don't survive 55 s to warm it, and cross-region makes the cold
read the expensive one. The fix space is therefore **architecture-level** (a shared/persistent
catalog+data cache across containers — EFS or S3-backed), **region colocation**, and the
**deadline/async** fixes — *not* a config-flag flip and *not* a solo one-line cache wire.

---

# Appendix — Lever arithmetic (predictions, not remedies)

Everything below is **arithmetic prediction**, not measurement and **not a remedy commitment**.
It exists to make Track C's slate quantitative and to give the adversarial pass concrete
parameters to attack. Each lever recomputes the same decomposed wall model with one term changed;
the "does NOT fix" line under each is as load-bearing as the headline number.

## A.1 The parametric wall model

For one query on a **cold container**, wall ≈ Σ over touched tables of:

```
  T_catalog(table)              cold catalog chain: loadTable GET + metadata + manifest→scanfiles
+ T_sweep(F, C) = F / C × L     fetch F selected data files, C-at-a-time, per-file cold S3 GET = L
+ T_materialize                 row-count-bound work above the scan (joins, un-fused COUNT, sort)
```

Catalog fan-out across tables is bounded at `CATALOG_CONCURRENCY = 8` (`rest.rs:65`); the sweep
concurrency is `C` (§4).

**Parameters (with their anchors — these are what the adversarial pass should challenge):**

| Param | Value used | Range | Basis |
|---|---|---|---|
| `L` — per-file cold S3 GET latency (tiny ~6.6 KB file) | **30 ms** | 20–40 ms | Back-out below; load-bearing. Assumes reqwest keep-alive connection reuse ⇒ `L` is the round-trip, not TLS setup. If GETs pay fresh TLS, `L` balloons; if HTTP/2 multiplexing helps, lower. |
| `C` cold Lambda (10 GB ≈ 6 vCPU) | **6** | — | §4; `available_parallelism` caveat still open (could differ). Override unset. |
| `C` dev (where wins were measured) | **12** | 10–16 | §4. |
| `T_catalog` per **fact** table (cold) | **~7 s** | — | q029 model "~7 s fixed (loadTable+oauth+scan_plan)" (F17). Decomposes ~2 s loadTable GET + ~5 s metadata+manifest→scanfiles for a 7.7 K-file table. |
| `T_catalog` per small **dim** (cold) | **~2 s** | — | ~1 loadTable GET + tiny metadata + 1 manifest. |
| `T_materialize` q016 hash-join (SF01) | **~11 s** | — | Measured decomposition (#1501 body: ~11 s hash-left-join partition/materialize). |

**Anchor cross-checks (why `L≈30 ms`, `C_dev≈12` are defensible):**
- q029 cold-isolated one FACT_WEB_EVENT sweep = **57 s**, pure fetch, warm-catalog (F17). Solving
  `F/C_dev × L = 57 s` at `C_dev=12, L=30 ms` ⇒ **F_WEB_EVENT ≈ 22,800 files** — a plausible size
  for a large event fact. (I use **≈23 K** below.)
- q056 warm-catalog = **168 s** over **69,046 files** (all 16 TMs), fetch-dominated (un-fused COUNT
  reads every object column). Predicted `69046/12 × 30 ms = 172.6 s ≈ 168 s` — **independent
  second anchor**, same `L` and `C_dev`, different file set. Two anchors agreeing on `L≈30 ms` is
  the strongest evidence in this appendix; if either is wrong the whole table moves.

**Authoritative file counts:** FACT_INVENTORY_SNAPSHOT = **7,670** (metadata.json, zero-cred);
FACT_SHIPMENT ≈ **7,670** (q016 decomposition `n=23010 = 3×7670`); FACT_WEB_EVENT ≈ **23 K**
(back-out above); q056 whole-dataset = **69,046** (span-attributed). SF20 = **20×** files/rows/bytes.

`T_sweep(F, C) = F/C × L` (seconds), the term every lever moves:

| F (files) | C=6 (Lambda) | C=12 (dev) | C=16 | C=32 |
|---|---|---|---|---|
| 7,670 (FACT_INVENTORY / FACT_SHIPMENT) | **38.4** | 19.2 | 14.4 | 7.2 |
| 23,000 (FACT_WEB_EVENT) | **115.0** | 57.5 | 43.1 | 21.6 |
| 69,046 (q056 whole dataset) | **345.2** | 172.6 | 129.5 | 64.7 |

## A.2 Baseline cold-container walls (NO lever), SF01 and SF20, at C=6

| Query | Terms (SF01, C=6, L=30 ms) | **SF01 cold** | SF20 cold (files ×20; materialize ×20; T_catalog grows w/ manifest size) | Fits 55 s? |
|---|---|---|---|---|
| q029 | 7 (cat) + 115 (sweep 23K) + 0.6 (decode) | **~123 s** | ~2,300 (sweep 460K) + ~22 ≈ **~2,320 s** | No / No |
| q031 | 7 (cat, fact‖dim) + 38.4 (sweep 7.7K) + ~2 (materialize) | **~47 s** (L=40 ⇒ ~60 s) | ~767 (sweep 153K) + ~22 + ~2 ≈ **~790 s** | Borderline / No |
| q016 | 7 (cat) + 38.4 (residual sweep) + 11 (join) | **~56 s** (residual up to ~3× ⇒ ~130 s) | ~767 + ~22 + ~220 (join ×20) ≈ **~1,010 s** | No / No |
| q055 | ~14–20 (16-table catalog, 2 waves of 8) + small LIMIT-5 sweep | **~25–40 s** | ~44 (2 waves, bigger manifests) + small ≈ **~45–60 s** | Yes / Borderline |
| q056 | ~20 (16-table catalog) + 345 (full fetch 69K) | **~365 s** | ~6,900 (fetch 1.38 M) + ~40 ≈ **~6,940 s** | No / No (hard) |

The pattern from §2 in numbers: everything except the loadTable-dominated shapes (q055) is
**sweep-bound**, so the two multipliers — cold catalog + half concurrency — are what push
SF01-warm ≤3 s queries over the 55 s cap, and SF20 is categorically out of reach cold for any
sweep-bound shape. (These refine §2b's rougher bands with the decomposed model; where they differ,
the parametric figure here shows its work.)

## A.3 Per-lever predictions

### (a) Catalog-warm (a primed / persistent catalog cache) — removes `T_catalog`

Exactly the epic's cold-data/warm-catalog protocol: subtract the `T_catalog` term (the loadTable
GET + metadata + manifest→scanfiles). **Sweep and materialize are untouched.**

| Query | Cold baseline | Catalog-warm (C=6) | Δ | Verdict |
|---|---|---|---|---|
| q029 | ~123 s | ~116 s | −7 s | negligible — sweep-bound |
| q031 | ~47 s | ~40 s | −7 s | small |
| q016 | ~56 s | ~49 s | −7 s | small |
| q055 | ~30 s | **~5 s** | −25 s | **decisive** — q055 IS its catalog |
| q056 | ~365 s | ~345 s | −20 s | negligible — sweep-bound |

**Does NOT fix:** any sweep or materialize term. Helps only loadTable-dominated shapes (q055,
the `?s ?p ?o LIMIT k` first-touch). This is what #1503 + PR-8-slice-2 already buy **within a warm
container**; the lever question is making it survive a **cold** one (persistence beyond the
container), which is architecture-level (EFS/S3-backed catalog), not a config flip.

### (b) Scan concurrency 6 → 16 / 32 — scales the sweep term ~1/C

`T_sweep ∝ 1/C` until S3 request-rate throttling (~5,500 GET/s per prefix — not reached at these
values) **or** executor saturation. **Stated assumption:** the work is S3-latency-**wait**-bound,
so concurrency above the ~6 vCPU still helps (threads park on I/O), but gains taper past C≈16 once
per-request CPU (TLS, decode) competes for ~6 cores. Linear-until-taper.

| Query | C=6 | C=16 | C=32 | Note |
|---|---|---|---|---|
| q029 | ~123 s | ~50 s | ~29 s | still over 55 s until ~C≈14 |
| q031 | ~47 s | ~23 s | ~16 s | under 55 s at all C |
| q016 | ~56 s | ~32 s | ~25 s | join floor (11 s) unmovable by C |
| q056 | ~365 s | ~150 s | ~85 s | never under 55 s by concurrency alone |

**Does NOT fix:** `T_catalog` (fixed), `T_materialize` (q016's 11 s join, q056's count decode —
CPU-bound, competes with the very concurrency you raise). Pure win on sweep-bound tails, capped by
the vCPU allotment; a bigger Lambda (more memory ⇒ more vCPU) is the same lever by another name.

### (c) DATA COMPACTION — 39-rows/file → 64/128 MB target files (the lever to fight over)

Compaction collapses **file count**, so it attacks the **per-file request floor** that dominates
every sweep-bound wall — the single biggest term at SF01 and the *only* thing that makes SF20
tractable cold. FACT_INVENTORY 51 MB / 7,670 files → **1 file @128 MB** (or ~1 @64 MB);
7,670 → ~1–4 (row-group/split boundaries). The sweep term changes character: from `F/C × L` (tiny
GETs, latency-bound) to `F' /C × L_big`, `F'` = a handful of files, `L_big ≈ file_MB / ~100 MB·s⁻¹
+ latency ≈ 1.3 s` for a 128 MB file.

| Table / query | Files → compacted | Sweep SF01 C=6: before → after | Sweep SF20 C=6: before → after |
|---|---|---|---|
| FACT_INVENTORY (q031) | 7,670 → ~1–4 | 38.4 s → **~1–3 s** | 767 s → **~2 s** (1 GB → ~8 files) |
| FACT_WEB_EVENT (q029) | ~23 K → ~4 | 115 s → **~2 s** | 2,300 s → **~35 s** (~20 GB → ~160 files) |
| q056 whole dataset | 69,046 → ~70 | 345 s → **~5–15 s** (fetch overhead gone) | 6,900 s → **~20–40 s** |

**Predicted post-compaction cold walls (C=6, SF01):** q029 ~7 (cat) + ~2 ≈ **~9 s**; q031 ~7 + ~2
+ ~2 ≈ **~11 s**; q016 ~7 + ~3 + **11 (join stays)** ≈ **~21 s**; q056 ~20 (cat) + ~10 (fetch) +
**count-decode of ~456 MB of objects (stays)** ≈ **~40–60 s**.

**What compaction does NOT fix — read this before betting on it:**
- **loadTable GET** (~2 s/table) — O(1) in file count, untouched. A many-table first-ask (q055,
  16 loadTables) is *unaffected* by compaction; its bottleneck is catalog round-trips, lever (a)/(d).
- **metadata fetch** — untouched (small); **manifest derivation** — actually *helped* (fewer files
  ⇒ smaller manifest tree), a secondary bonus folded into a lower `T_catalog`.
- **`T_materialize`** — untouched. q016's ~11 s hash-join is **row-count**-bound; compaction moves
  zero rows. q016 lands ~21 s cold even fully compacted — still needs a join/probe lever for ≤3 s.
- **q056's un-fused COUNT** — compaction removes the 69 K-request floor but **the same object bytes
  are still fetched and every row still decoded to count** (Σ rows × POM). Compaction takes q056
  345 s → ~40–60 s; only a **fused predicate-less COUNT (F22, PR-6 sibling)** that reads manifest
  `record_count`s takes it sub-second with **no data read at all**. Compaction and F22 are
  orthogonal; the adversarial pass should not let one masquerade as the other.
- **Write-side cost & freshness** (out of engine scope, but the honest counterweight): compaction is
  a Snowflake/Iceberg maintenance job on the *source* — it has a cost, a cadence, and a
  freshness/latency tradeoff (newly written 39-row files exist until compaction runs). This is why
  it is a data-level lever, not an engine flip, and why it deserves the fight.

### (d) Pointer / catalog-TTL widening — spares part of `T_catalog`

Two sub-levers with very different reach (§1b):
- **Pointer only** (#1503, `FLUREE_ICEBERG_LOADTABLE_PTR_TTL_SECS`, currently 300 s): spares only
  the **loadTable GET** (~2 s of the ~7 s). Metadata + manifest still run *unless their
  content-addressed entries are also on disk* — and you need the pointer/GET to learn the
  `metadata_location` that keys them, so past-TTL you pay ~2 s (GET) and then metadata/scanfiles hit
  by content address. Net past-TTL ≈ ~2 s/table saved; within-TTL ≈ full ~7 s (pointer + catalog
  both hit).
- **Whole-catalog persistence** (the -catalog dir surviving cold containers): spares the full
  ~7 s/table.

| Query | Cold baseline | Pointer-only (−~2 s/table) | Whole-catalog (−~7 s/fact, −~2 s/dim) |
|---|---|---|---|
| q029 | ~123 s | ~121 s | ~116 s |
| q031 | ~47 s | ~45 s | ~40 s |
| q055 (16 tables) | ~30 s | ~18–22 s | **~5 s** |
| q056 | ~365 s | ~345 s | ~345 s |

**Does NOT fix:** any sweep/materialize. Same reach as (a); the distinction (a) vs (d) is *how* the
catalog is kept warm — TTL-widening keeps a **within-container** win alive longer (F21), whereas
true cold-container survival needs **shared/persistent** catalog storage (architecture-level). The
pointer-vs-whole-chain split matters: widening the pointer TTL alone leaves ~5 s/fact-table of
metadata+manifest on the table unless the content-addressed layers persist too.

### (e) Null lever — no engine change, chat cap raised or made async

Which shapes already fit under a higher sync cap or an async (job/poll) envelope, cold, at C=6:

| Query | SF01 cold | < 120 s? | < 300 s? | SF20 cold | < 300 s? |
|---|---|---|---|---|---|
| q029 | ~123 s | ✗ (just over) | ✓ | ~2,320 s | ✗ |
| q031 | ~47 s | ✓ | ✓ | ~790 s | ✗ |
| q016 | ~56 s | ✓ | ✓ | ~1,010 s | ✗ |
| q055 | ~30 s | ✓ | ✓ | ~50 s | ✓ |
| q056 | ~365 s | ✗ | ✗ | ~6,940 s | ✗ |

**Reach:** raising the sync cap to 120 s rescues q031/q016/q055 at **SF01 only**; an async envelope
(≤300 s) additionally covers q029 at SF01. **q056 misses even 300 s at SF01, and every sweep-bound
shape misses 300 s at SF20** — so the null lever is an SF01-scale band-aid that a real dataset
(SF20) defeats. It also does nothing about §3's burn-after-abandon (a raised cap means the engine
burns *longer* per timed-out ask unless cancellation is also wired). Cheapest to ship, smallest
ceiling.

## A.4 What the arithmetic says for the adversarial pass

- **Sweep-bound vs catalog-bound is the primary split.** q055 is catalog-bound (levers a/d win);
  q029/q031/q016/q056 are sweep-bound (levers b/c win). No single lever covers both families.
- **Compaction (c) is the only lever that scales to SF20** and the only one that touches the
  per-file request floor — but it stops at the loadTable GET, the hash-join, and the un-fused-COUNT
  decode. Pair it with a catalog lever (a/d) for q055 and a join/COUNT lever for q016/q056, or the
  tail re-emerges at scale.
- **Concurrency (b) is a free ~2× that never reaches the bar alone** and competes with materialize
  for the same ~6 vCPU.
- **The null lever (e) is SF01-only.** Any plan that leans on "just raise the cap" should be made to
  show its SF20 row.
- **Every number here rides on `L≈30 ms` and `C_dev≈12`** (two independent anchors, q029 + q056).
  The highest-value thing Track B/lambda-audit can return is a **measured** cold in-Lambda `L` and a
  **measured** `available_parallelism` — those two collapse the ranges above into commitments.

## A.5 Track B corrections to the model (deployed `DW_SVL`, cross-region)

Track B's forensics change three things in the model. Folded here; the cross-flag answers from
lambda-audit fold in when they land.

### (i) Cross-region multiplies `L`

The deployed stack is **cross-region**: Lambda us-east-1 → parquet `s3://…-use2` (us-east-2) +
Snowflake Polaris REST catalog (Track B §5). The per-file term `F/C × L` is RTT-dominated for tiny
files, so cross-region inflates `L`: a same-region S3 GET first-byte is ~10–20 ms (network RTT
~1–5 ms); cross-region us-east-1↔us-east-2 adds ~12–15 ms RTT, so with connection-pool reuse `L`
runs **~1.5–3× higher** cross-region (the "~3–10×" framing holds at the RTT-only layer; on the full
GET incl. S3 processing it compresses to ~1.5–3×). **Caveat on the anchors:** q029's 57 s and
q056's 168 s were measured **dev-Mac → S3** — *neither* Lambda-same-region *nor* cross-region — so
they validate the model **form** `F/C×L`, not the absolute Lambda `L`. A same-region Lambda would be
**faster** than the dev anchor; this cross-region stack is **comparable-to-worse**.

| Sweep (7,670-file fact) | L=15 ms (same-region) | L=30 ms (dev anchor / mild cross) | L=45 ms (cross-region) |
|---|---|---|---|
| C=6 | 19.2 s | 38.4 s | 57.5 s |
| C=16 | 7.2 s | 14.4 s | 21.6 s |

**Region colocation** (Lambda + parquet in one region, or replicate parquet to us-east-1) removes
the cross-region term outright — a pure multiplier win, orthogonal to compaction and concurrency.

### (ii) The failing dataset is a DIFFERENT file shape than A.1–A.4 assume — the corpus has THREE shapes

A.1–A.4 model the `DW_SF01` tiny-file fact (7,670 files, 39 rows/file) → a per-file **request**
floor. `DW_SVL.DIM_PRODUCT` is the **opposite**: `files_selected=1`, **167,705 rows in ONE file**
(Track B §4). For that shape:
- **`files_pruned=0` is EXPECTED, not a missed prune.** One file, no selective `FILTER`, no
  `ORDER BY DESC LIMIT` ⇒ PR-5 top-k, PR-7 numeric, and PR-3 star prune all correctly **decline**
  (nothing to prune). `files_pruned=0` here is the sound answer.
- **The wall is single-file cross-region TRANSFER + 167K-row MATERIALIZATION,** not request-count
  fan-out. Compaction and file-count levers (PR-5, PR-3, PR-2a) are **MOOT** on an already-single-
  file table.

So the corpus splits into **three** shapes, and a lever helps at most one or two:
- **(a) tiny-file fact** (DW_SF01) — request-floor bound → compaction / concurrency.
- **(b) single-large-file dim** (DW_SVL `DIM_PRODUCT`) — transfer+materialize bound → region
  colocation + a materialization/join-key lever; compaction irrelevant.
- **(c) catalog-bound many-table first-touch** (q055 / `get_data_model`) — catalog persistence.

**The deployed timeout is shape (b); the epic optimized shape (a).** That mismatch — not a broken
win — is why `DW_SVL` chat times out despite the wave. (Unverified whether `DW_SVL`'s FACT tables
are shape (a) or (b); Track B only profiled `DIM_PRODUCT`.)

### (iii) The two `DIM_PRODUCT` scans

Consistent with the FK/crawl pattern: one scan materializes the `DIM_PRODUCT` subject star, a second
projects only `[PRODUCT_KEY]` as a join/parent key (the engine scans one parent per `RefObjectMap`
POM, db `operator.rs:1085-1099`). On the shipped pin, F20 ref-target-prune cuts *loadTables* and
PR-8b parent memo caches *within-query* parent lookups, but a distinct **subject-scan + FK-key-scan
of the same table** remains two scans by design unless the key column is obtained without a rescan.
Two caveats: (1) the narrow `[PRODUCT_KEY]` range-read of a >32 MB file is exactly the case the
whole-file disk cache does **not** admit — so **even a warm cache would not have collapsed the
second scan**; (2) whether the second scan *should* have been served from the first scan's output
needs the exact SPARQL (Track B open item). This is a candidate **engine** lever (avoid the second
same-table scan / retain the key column), not a data or config fix.

### (iv) `get_data_model` — the worst real offender — added to the predicted-walls table

Shape: a single JSON `@context` query that **serially** `loadTable`s the entire star schema (all
~10 tables: `DIM_*` + `FACT_*`; Track B §4), with **no server-side deadline** — solo's `/data-model`
path sends no `x-query-timeout-secs` and carries no `opts.timeout`, so the engine's deadline guard
is inert. This is the concrete manifestation of §3b (the embedded single-query path arms no deadline
of its own): with neither solo nor engine arming one, the query runs to the 900 s Lambda ceiling and
pins **both** the query Lambda **and** the router container (Track B X-Ray, §4). It is a **runaway**,
not merely a >55 s scan.

| Query/op | Shape class | SF01 cold prediction (C=6) | Deployed reality (`DW_SVL`, cross-region) |
|---|---|---|---|
| `get_data_model` | (c) catalog-bound, **serial**, **un-deadlined** | Σ ~10 tables × T_catalog (serial): ~40–90 s catalog alone, + per-table first scan | **900 s runaway** (no server deadline); pins query **and** router 15 min |

The engine-side arithmetic: `get_data_model` ≈ Σ over ~10 tables of `(T_catalog + ≥1 scan)`,
**serial** (not the fan-out `buffer_unordered` of a single scan), cross-region. At ~7 s/fact +
~2 s/dim cold cross-region, the catalog chain alone is ~40–90 s before any data — already past 55 s —
and because it is un-deadlined it does not stop. This is the single shape where the **engine finding**
(§3b, no self-timeout) and the **solo finding** (no `/data-model` server deadline) compound into a
capacity-risk runaway, and it is the most fixable (wire a deadline on that path).

## A.6 Measured-in-Lambda correction — the dominant term is ROW MATERIALIZATION, not fetch

lambda-audit's cross-answers supply the **measured in-Lambda anchor** that supersedes the model's
weakest assumption (`L≈30 ms`, dev-Mac→S3). It changes what the primary lever is.

**Measured anchor (a scan that COMPLETED under budget):** `DW_SVL.DIM_CUSTOMER`, one file,
1,744,133 rows, projection of 4 columns, plan→complete = 30.95 s at `C = min(6, 1 file) = 1`
⇒ **≈56,000 rows/s on one core** (Track B §4/§"Lever arithmetic"). The file is small (4 columns),
so this is **not** fetch-bound — it is single-threaded R2RML row materialization.

**The dominant term is scan VOLUME (rows materialized), not cross-region latency and not file
count.** `files_pruned=0` on every table (no selective predicate is pushed down), so **every row is
materialized**. Cross-region metadata RTT is a measured ~0.9–1.3 s/table (≈30× same-region) — a real
multi-second cold floor for a ~19-table star, but dwarfed by materialization once FACT tables are in
play. So the model needs a materialization term, and it becomes the leading one:

```
  wall ≈ Σ_tables [ T_catalog(~1.2 s/table cross-region metadata RTT, cold)
                    + max( request-floor  F/C × L        (DW_SF01 tiny-file regime),
                           materialize     rows/(C × 56k) (DW_SVL volume regime) ) ]
```

For `DW_SVL` the materialize term dominates. FACT tables are 36 M–200 M rows across **64–129 files**
each ⇒ `C = min(6, files) = 6` ⇒ ~6×56k ≈ 336k rows/s ⇒ a **36 M-row FACT ≈ 107 s**, a **200 M-row
FACT ≈ ~590 s**. That one arithmetic explains BOTH the nine 55 s cancels (any query touching a FACT
blows the budget) AND the 900 s `get_data_model` runaway (info-stats touches every FACT, ~0.5 B rows,
un-pruned). **Correction to §A.5/(iii):** Track B's "a single 167K-row `DIM_PRODUCT` scan alone blew
55 s" is not consistent with 56k rows/s (167K rows ≈ 3 s); that 52 s query must also have touched a
FACT/join partner — the exact SPARQL is the open item, but the wall is a FACT materialization, not
the dim.

### The single-file regime is single-threaded by construction (engine receipt)

A single-file table pins scan concurrency to `min(6, 1) = 1` (§4), and the large-file decode runs
the whole Arrow decode on **one** `spawn_blocking` thread — `decode_large_file` (db
`send_parquet.rs:678-705`, one `tokio::task::spawn_blocking` at `:691`) reads that file's row groups
**sequentially**. So a single 1.7 M-row file uses **one of six cores**. **Intra-file (row-group)
parallelism is a real, currently-absent engine lever:** Parquet row groups are independently
decodable and are already enumerated (`surviving_row_groups`, `send_parquet.rs:68`); splitting them
across `spawn_blocking` tasks would cut the single-file / few-file materialization wall ~×(cores).
It is **plan-level and survives cold** — it needs no cache. This is the *only* engine lever for the
single-large-file regime besides re-layout.

### Compaction guidance changes — and is NOT the dominant fix here

Because a single file → `C=1`, the compaction target is **not** "few files" but **N×128 MB where
N ≥ available parallelism** — compacting `DW_SF01` to ONE file would drop it into `DW_SVL`'s
single-thread pathology. And critically: **compaction moves zero rows**, so it does **not** reduce
the dominant materialization term. The dominant lever is **pruning / predicate + row pushdown**
(`files_pruned=0` is the smoking gun) and **intra-file parallelism**; compaction and same-region
placement are secondary cold-floor trims.

### The info-stats runaway is a ROUTING gap, not a missing capability (FACT 2 answer)

**A metadata-only virtual info path already exists and already serves row counts from Iceberg with
zero data reads.** `build_graph_source_info` → `build_iceberg_virtual_info` →
`fetch_virtual_table_row_counts` reads the **loadTable snapshot summary only** — "no manifest-list,
manifest, or Parquet/data file is read" (db `ledger_info.rs:1524-1526`, fan-out `:1607-1648`);
classes/properties are derived from the R2RML mapping and NDV is set to `None`
(`build_virtual_ledger_info`, `:1193`, `:1344-1345`); the count fetch is even time-boxed
(`info_count_budget_ms`, `:1715-1735`).

**But that path is reached only on the `is_not_found()` fallback.** `LedgerInfoBuilder::execute`
(db `ledger_info.rs:2165-2191`) first resolves the id as a **committed** ledger; only if that returns
not-found does it fall back to `lookup_graph_source` → the metadata-only path (`:2172-2188`). A
virtual dataset that is **also a committed ledger** — which `full-enterprise-byo-1:main` is (a named
dataset composed of 16 R2RML triples-maps) — resolves as a ledger and takes the **native**
`build_ledger_info_with_options` → `assemble_full_stats` (`:487`, `:527`), which computes full stats
by **materializing the federated R2RML tables** = the 900 s runaway.

**Feasibility: HIGH — it is a routing + skip-scan fix, not new infrastructure.** Route a
graph-source-federated dataset's stats to the metadata path (row counts from snapshot summaries,
schema from the mapping) instead of `assemble_full_stats`. **What consumers need:** the info-stats
`flakes`/`count`/`ndv-*` fields are already `Option` documented "null when unknown (virtual dataset,
no scan)" (`ledger_info.rs:147,195,211-222,249,274`), so the schema already tolerates
estimate/absent. The `get_data_model` consumer needs the **shape** (classes, properties, table
structure) + **approximate counts** for LLM context — not exact flake totals or exact NDV. Iceberg
snapshot summaries give **exact** row counts cheaply; column NDV is optional in Iceberg manifests
(`distinct_counts`) — serve it if present, else `null`. **No full-table scan is required for the
data-model use case.** This kills the runaway independent of any deadline fix (the deadline fix is
the orthogonal solo-side belt-and-suspenders).
