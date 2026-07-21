# C2 — Empirical bench, wave 1: native vs virtual over the R2RML/Iceberg/Snowflake path

Scope: the live-measured half of the perf audit of fluree/db's virtual-dataset path, at audit tip `10e073fe9` (origin/feat/lambda-usability, MEASURED unmoved this session). Companion to A1/A2/A3 (architecture/strategy/coverage), B1 (SotA), C1 (constraint envelope), C3 (materialization facts). Labels: MEASURED = a wall/counter I recorded this session; INFERRED = arithmetic/reasoning over measured values; every paragraph is one line for clean copy/paste.

## 0. Methodology (MEASURED unless noted)

Binary: `<audit-checkout>/target/release/vbench`, a release build pinned at `10e073fe9` (meta `git_commit` on every run confirms it; `git_dirty=false`). Host <bench-host>, tokio multi-thread worker_threads=16. The engine under test is `fluree-db-api` with the `iceberg` feature (the R2RML/Iceberg read path + `with_r2rml()`). NOTE: origin/feat/lambda-usability == `10e073fe9` at session start (fetched, MEASURED) — the bench tip did not drift during the audit.

Protocol (mirrors the established `pf_wave_final_baseline.sh`): per target, one PRIMING rep (discarded) then N MEASURED reps, median wall reported; per-query deadline from the corpus manifest `timeout_s`. Native reps=5, virtual reps=3. Corpus = `fluree-bench-virtual/corpus` v2, 68 queries q001–q068 (23 dims-only, 39 fact-touching, 6 exploration); q060–q068 hardcode `FROM <enterprise-sf01-v:main>` (dataset-path shape) and therefore always target the sf01 virtual source. The virtual target `virtual-sf01` is the scale-matched twin of `native-sf01` (byte-identical source parquet, SF0.1), so result hashes are directly comparable; `virtual-sf20` is the 200×-scale live stress target (hashes NOT comparable to native).

POST-REBOOT COLD NOTE (important for reading cold numbers): the host had restarted before this session; all OS page caches were cold at start. HOWEVER the on-disk artifact caches survive a reboot: the home-scoped Iceberg cache `<fluree-home>/.vbench-iceberg-cache` held 7,672 entries (dated pre-reboot) and `$TMPDIR/fluree_binary_cache` (the engine default) may also persist. So "post-reboot first touch" = OS-page-cache-cold + network-catalog-cold, but NOT a fully-cold parquet download unless the artifact cache is explicitly cleared. The Step-4 cold protocol below clears the artifact cache explicitly (`exec-one --cold` → `clear_cold_cache`), so it is a TRUE cold (forced re-download); post-reboot first-touch of a warm-on-disk cache is a distinct, milder regime. The stale `crash/` dir in the fluree home is EMPTY and dated 2026-07-10 (pre-reboot) — not from this restart; reported, not touched.

Native sanity re-check after the reboot (MEASURED): q001 against native-sf01 returned status=ok, rows=500, result_hash `b63934693917fee0…` — byte-identical to the committed native baseline — at wall 104 ms. The 104 ms (vs the committed hot median of 1 ms) is exactly the post-reboot cold page-cache first-touch of the ledger index; a warm re-run returns to ~1 ms. The native ledger survived the reboot intact.

CLIENT-SIDE MEMORY SHAPE (MEASURED datum, single-machine): the `vbench run --targets virtual-sf01` process — the VIRTUAL scan client alone (one hot handle reused across the whole 68-query corpus; the native ledger is NOT loaded by this process, it targets only virtual-sf01) — peaked at RSS ≈ 20.5 GB on a 48 GB machine (~43% of RAM) during the priming pass, with macOS compressor active (~8 GB) but swap used = 0. This is the R2RML/Iceberg read-path working set: per-file parquet decode buffers + leaflet/ledger caches + transient per-query binding materialization, accumulated behind a warm handle. It did not OOM, but the figure is consistent with the earlier machine-wide memory-pressure incident and corroborates V2's memory-budget finding: a single virtual scan client can hold ~20 GB client-side, so the ~78%-of-budget-per-query accounting (no `set_memory_limit(budget/N)`) is load-bearing under any concurrency. A memory watchdog (alert at RSS≥34 GB or swap>1 GB) guarded the run.

## 1. HEADLINE — the committed perf baseline is STALE and the gate is blind on 62% of the corpus (MEASURED)

The committed virtual-sf01 perf baseline (`baselines/perf/virtual-sf01.json`) was blessed from commit `7d77218e2` (`blessed_from.commit`, MEASURED) — an OLD tip, before the #1450→#1514 single-scan-crawl / spans / perf line. It has 54/68 entries. Of those 54, exactly 28 are pinned at `hot_wall_ms_median = 180000` — i.e. they were DNF (deadline-capped at the 180 s manifest timeout) at bless time and blessed AS the timeout wall: q008, q009, q010, q012, q014, q015, q016, q017, q018, q019, q021, q025, q026, q027, q028, q029, q031, q032, q036, q037, q039, q040, q041, q046, q047, q048, q052, q053 (MEASURED, exact list). Two more (q044, q050) are pinned at 120000 (the 120 s dim timeout). And 14 queries are MISSING entirely: q055–q068 (the exploration family q055–q059/q068 + the dataset-path family q060–q067). Net: the gate has no honest perf reference on 28 + 14 = 42 of 68 queries (62%).

Why this is dangerous (INFERRED from the budget model, MEASURED constants): `compare --gate` fires a virtual-hot violation only when observed wall > baseline_median × 1.20 AND observed − baseline ≥ 50 ms (`budgets.json`: virtual_hot=20%, min_delta_ms=50; cold is advisory-only, never gates; no per-query overrides). For a query blessed at 180000, the gate does not trip until 216000 ms. Example: q010 is now ~124 ms native (MEASURED, native-full) and fast virtual (§2) — yet its blessed budget is 180000, so it could regress by ~1,740× (to 216 s) before the gate notices. A now-fast member silently loses all perf protection. Only the correctness/hash arm of `compare` is currently trustworthy on this corpus.

Re-bless protocol (recommendation, to include in the audit): (1) bless perf from a FRESH full-corpus run at the CURRENT tip, N≥3 reps, ALL 68 queries (this wave produces exactly that run); (2) quarantine the known loadTable-GET drift set {q002, q004, q022, q024, q030, q043} into per-query WIDER budgets (network-flappy, not engine — see §2/§4) rather than letting it flap the gate; (3) NEVER pin a timeout wall as a baseline — a DNF must bless as "no baseline / must-fix", not as 180000 ms (which reads as a 180 s "budget"); (4) re-bless on every base-commit change (single-machine medians; the budgets.json comment already says re-bless locally before gating on a new machine).

## 2. Native vs virtual, full corpus — the run (MEASURED)

Run: virtual-sf01, 1 priming rep discarded + 3 measured reps (median wall), full 68-query corpus, at tip `10e073fe9`, 2026-07-18 15:18–16:01. Outcome: 65 ok + 3 expected_error (q013/q034/q051 — the lang-tag/custom-datatype error-boundary queries that correctly error on a virtual R2RML target) + 0 unexpected failures. 57 of the 65 ok queries land ≤3 s (88%). Deliverable JSONL: `audit-2026-07/data/virtual-full-10e073fe9.jsonl` (copied, PAT-scrubbed). Native oracle: `data/native-full-10e073fe9.jsonl`.

Per-query table (native median wall / virtual hot median / ratio / rows / scan_table n / files_selected / files_pruned / est_row_count / hash_gate). `cm=1` = the COUNT(*) manifest shortcut fired (no scan). Ratios flagged where virtual/native > 10×:

```
q     cls        st   nat_ms  virt_ms   ratio    rows scan fs   fp    erc  gate       note
q001  dims       ok        1        4    4.0x     500    1  0    0      0  full
q002  dims       ok       ~0     1041     n/a       8    3  1    0    500  full       drift-set
q004  dims       ok       15     1384   92.3x    2789    1  1    0  25000  full       hi-ratio/lo-abs
q005  dims       ok        3       43   14.3x      20    4  0    0      0  full       hi-ratio/lo-abs (43ms)
q006  dims       ok        7     1364  194.9x    3593    1  1    0  15000  full       hi-ratio/lo-abs
q008  fact       ok      768      550    0.7x       9    3  0    0      0  full       fact→dim→dim rollup, FASTER than native
q010  fact       ok      900      432    0.5x      84    2  0    0      0  full       was 180000ms DNF at bless
q016  fact       ok      454    20934   46.1x    5000    3  0    0      0  rows_only  TAIL: OPTIONAL crawl, LIMIT5000
q017  fact       ok      150     2859   19.1x       0    5  0    0      0  full       anti-join (NOT EXISTS)
q019  fact       ok      234     1820    7.8x       0    1  0 7670      0  full       GL_JOURNAL filter_range, prunes 7670
q022  dims       ok      345     1627    4.7x       3    1  1    0 390000  full       FUSED groupby-count, drift-set
q024  dims       ok      240     1485    6.2x   44142    1  1    0 390000  full       materializes 44142 rows, drift-set
q028  fact       ok       65     1783   27.4x    5000    7  0    0 1e6    rows_only  hi-ratio/lo-abs
q031  fact       ok      206     4486   21.8x    5000  243  0    0      0  rows_only  TAIL: scan_table n=243
q036  fact       ok       ~0       66     n/a       1    0  0    0      0  full  cm=1 COUNT shortcut
q037  fact       ok       ~0       71     n/a       1    0  0    0      0  full  cm=1 COUNT shortcut
q038  dims       ok      191    57625  301.7x       1    3  2    0 427500  full       TAIL/HEADLINE: un-fused filtered COUNT
q039  fact       ok       ~0       63     n/a       1    0  0    0      0  full  cm=1 COUNT shortcut
q043  dims       ok       78     7824  100.3x       0    6  0    6      0  full       TAIL: drift-set, prunes 6 files
q044  fact       ok       51     1577   30.9x       0    1  0 7670 250000  full       hi-ratio/lo-abs, prunes 7670
q046  fact       ok      209       23    0.1x      10    1  0   10      0  full       scan-side top-k, 10 files (99.9% pruned)
q055  expl       ok        1      513  513.0x       5   25  0    0      0  rows_only  hi-ratio/lo-abs (513ms), 25 tables
q056  expl       ok       ~0    19789     n/a       1   25  0    0      0  full       TAIL: un-fused exploration COUNT, 25 tables
q057  expl       ok      919    24093   26.2x     100   25  0    0      0  rows_only  TAIL: exploration DISTINCT, 25 tables
q058  expl       ok      149     5298   35.6x      16   16  0    0      0  full       TAIL: exploration COUNT, 16 tables
q059  expl       ok     2594    23609    9.1x      10   25  0    0      0  full       TAIL: exploration COUNT, 25 tables
(full 68-row table in the JSONL; rows above = the notable/tail/highlight members)
```

The >3 s tail is 8 queries: q038 (57.6 s), q057 (24.1 s), q059 (23.6 s), q016 (20.9 s), q056 (19.8 s), q043 (7.8 s), q058 (5.3 s), q031 (4.5 s). It is dominated by two families — (a) the exploration/profile family q056/q057/q058/q059 (wildcard `?s ?p ?o` scans that fan out across 16–25 tables; inherently a whole-warehouse read, MEASURED scan_table n=25), and (b) the un-fused COUNT q038 (§below). q016 (OPTIONAL crawl) and q031 (243-scan inventory-below-reorder) round it out; q043 is a drift-set member (§4).

The 15 ratio>10× queries split into two categories that must not be conflated: (1) a REAL high-absolute tail — q038 (57.6 s), q016 (20.9 s), q057 (24.1 s), q058 (5.3 s), q031 (4.5 s), q043 (7.8 s); and (2) HIGH-RATIO / LOW-ABSOLUTE — q055 (513 ms), q006 (1.36 s), q004 (1.38 s), q005 (43 ms), q044 (1.58 s), q028 (1.78 s), q049 (1.68 s), q029 (1.36 s), q017 (2.86 s) — all ≤3 s virtual, flagged only because native is sub-100 ms (an index point-read the virtual path answers with a catalog+parquet round-trip). Category (2) is NOT a north-star concern; only category (1) is real tail.

Highlights (MEASURED): q001 = 4 ms virtual vs 1 ms native (index-fast both sides). q010 = 432 ms virtual — this was a 180000 ms DNF at bless time (7d77218e2); it is now 0.43 s, a ~415× improvement the stale gate cannot see (§1). q008 (fact→dim→dim revenue rollup) = 550 ms virtual, actually FASTER than its 768 ms native (fused join+aggregate beats the native materialize). The bare-COUNT trio q036/q037/q039 fire the `count_manifest` shortcut (cm=1, scan=0, files=0) and answer in ~65 ms from manifest metadata — no scan at all. rep-spread was tight for most queries (e.g. q031 4483–4495 ms, q038 57253–58245 ms) except the drift set (q022 1211–3158 ms, q024 939–2321 ms) — network variance isolated to §4's known set.

THE UN-FUSED COUNT (q038), headline gap (MEASURED): `SELECT (COUNT(*)) WHERE { ?s a edw:Customer ; edw:isCurrent true }` = 57.6 s virtual vs 191 ms native (301×). Because the COUNT carries a predicate filter (`isCurrent = true`), the manifest `count_manifest` shortcut does NOT apply (cm=0) — the engine falls to a full scan that materializes ~427,500 rows (erc) just to count the matching ones. This is the F-AUD-8 / probe-05-class decline made concrete on a corpus member: a COUNT that any column-stats or predicate-index path would answer in ms instead reads and materializes the whole class. It is the single largest native/virtual gap in the corpus and the clearest "coverage cliff" datum.

## 3. Parity / correctness verdict (MEASURED) — 0 mismatches, fully explained

`vbench compare` against the blessed native oracles: **68 records, 0 hash mismatch(es), 3 perf violation(s)** (COMPARE_EXIT=0). Correctness PASSES end-to-end. The 3 perf "violations" (q022 1.85×, q024 2.94×, q043 3.84×, all "confirmed on rerun") are ALL members of the known loadTable-GET drift set {q002,q004,q022,q024,q030,q043} — network-flappy catalog latency, not engine (§4). `compare --gate` exited 1 solely on these 3 advisory-noise perf flags; the correctness arm is clean.

Independent cross-check (STRICTER than the gate): I directly compared virtual vs native `result_hash` — an order-INDEPENDENT multiset hash (canon.rs: rows canonicalized cell-by-cell, sorted, then hashed). Result: of the 59 queries with a native twin, 45 are BYTE-IDENTICAL native==virtual (same multiset hash) and 11 differ; the other 9 (q060–q068) are virtual-only (they hardcode `FROM <enterprise-sf01-v:main>`, no native twin). The 11 differences are EXACTLY the 11 corpus-designated `hash_gate: rows_only` queries that have a native twin (q015/q016/q028/q029/q031/q045/q048/q049/q053/q055/q057; the 12th rows_only member q068 is virtual-only). Every one is a `LIMIT N` query with NO `ORDER BY` (LIMIT 5000/100/10/5) — so it returns an arbitrary VALID N-row subset, and native and virtual legitimately pick different subsets (identical row COUNTS in all 11). This is defined-nondeterministic subset selection, NOT a correctness bug: the harness deliberately gates these on row count (`HashGate::RowsOnly` in baseline.rs — compares `rows`, not `result_hash`), which is why `compare` passes them. My cross-check is stricter (compares content) and therefore surfaces the nondeterminism the gate is designed to tolerate — and it independently rediscovered the exact rows_only set from first principles. Net: the engine produces byte-identical results for every deterministic query; there is NO correctness finding, and the "rows_only" confound (memory: prior waves' rows_only nondeterminism) is fully characterized here.

Coverage nuance (not a bug): because those 11 members are unordered-LIMIT, they gate on count only — they cannot catch a hypothetical bug that returns the right COUNT but wrong rows within the LIMIT window. Adding an `ORDER BY` (making them `Full`-gated) would close that, at the cost of exercising a sort. Recommendation for the corpus, not the engine.

## 4. SF20 live targeted probes (MEASURED) — the two coverage cliffs become DNFs at 200×

exec-one, 1 rep each, fresh cold process, 2 s paced, against the live `virtual-sf20` source (200× scale). AUTH: no 401 — the `enterprise-sf20-v` registered source's client_secret is VALID (a prior 401 risk did not materialize; SF20 catalog + Parquet reads succeeded). Every SF20 exec-one is a full cold catalog open (fresh process), so loadTable + oauth fire each time (2.9–4.8 s of catalog per probe, MEASURED below).

```
q     probe            status   wall_ms  rows  scan_n  files_sel  files_pruned  parquet_n  loadtbl_us  oauth_us
q002  point-lookup     ok         7,678     8       3          3            0          3   4,768,448    455,390
q016  detail-crawl     DNF      180,000     0       5     15,347            0     23,275   3,554,954    250,952
q031  ref-prune        ok        24,136  5,000     240      7,675            0        322   2,952,802    392,246
q038  un-fused COUNT   DNF      120,000     0     160          3            0        159   3,072,325    413,791
q008  GROUP-BY rollup  ok        18,391     9       3          0            0      7,675   3,462,131    849,955
```

Reading it (MEASURED): the two DNFs are EXACTLY the two coverage cliffs. q016 (OPTIONAL detail-crawl) selects 15,347 files and issues 23,275 Parquet reads with ZERO pruning (fp=0) — it scans the whole fact linearly and times out at 180 s. q038 (filtered COUNT, no manifest shortcut) times out at the 120 s dim deadline. The three that COMPLETE are instructive: q002 (point-lookup) is catalog-bound — 5.2 s of its 7.7 s is loadTable+oauth, only 3 Parquet reads for 8 rows; q031 (ref-prune arc) selects 7,675 files but the LIMIT 5000 budget cuts actual reads to 322 (the scan-side budget forwards through the ref-chain — 240 scan_table ops, 24 s); q008 (fact→dim→dim revenue rollup) streams 7,675 FACT_ORDER Parquet files into a 9-row fused aggregate in 18.4 s (fused path completes where the crawl/COUNT cliffs DNF). INFERRED: at 200× scale the corpus splits cleanly — fused-aggregate and budgeted-LIMIT shapes complete (seconds-to-tens-of-seconds), while un-pruned crawls and un-fused filtered COUNTs cross into DNF. The cliffs are the same ones the sf01 tail flagged (§2), amplified by scale.

## 5. Cold-vs-warm subset with per-phase span attribution (MEASURED)

Per query: COLD = `exec-one --cold` (fresh process, dedicated scratch cache CLEARED → full download); WARM = `exec-one` (fresh process, same scratch cache now populated → cached data, but re-mints oauth + re-reads catalog = the Lambda-reuse cost); HOT = the reused-handle median from the §2 full run (warmest, steady-state server). Spans are total_us summed across concurrent tasks, so they exceed the wall where reads parallelize; the WALL deltas are the load-bearing numbers.

```
q     shape           COLD_ms   WARM_ms  HOT_ms   cold load_table  cold parquet(sum,n)   warm parquet(sum,n)
q002  point-lookup      5,476     1,257   1,041    3,788,956 us     494,171 (3)             1,089 (3)
q045  LIMIT browse      2,207        46      21    1,465,079 us      79,867 (1)               281 (1)
q014  FUSED rollup     40,978       407     119    1,496,050 us    653,605,360 (7670)     5,666,843 (7670)
q038  un-fused COUNT   58,311    58,935  57,625    2,736,227 us     937,452 (3)            11,983 (3)
q046  top-k            3,230        45      23    2,098,633 us    3,180,409 (10)              807 (10)
q016  detail-crawl    99,613    21,847  20,934    2,975,890 us  1,243,018,053 (23010)  14,355,117 (23010)
```

Attribution (MEASURED): (a) CATALOG COST = oauth + load_table, fires on every cold open at 1.5–3.8 s of load_table alone; on a warm cache-hit it either vanishes entirely (q045/q046 warm show oauth/load_table spans ABSENT → 45–46 ms total) or shrinks (q002 warm load_table 0.89 s). This is the serverless cold-start tax: ~2–4 s per cold Lambda invocation before any data moves. (b) DATA-GET+DECODE = the parquet_read delta: q014 cold sums 653 s across 7,670 concurrent file reads (wall 41 s) vs warm 5.7 s (wall 407 ms) — downloading 7,670 FACT_ORDER files is the entire cold cost; q016 cold sums 1,243 s across 23,010 reads (wall 99.6 s). (c) The browse/top-k queries (q045, q046) collapse to 45–46 ms warm — once the catalog and the handful of files they touch are cached, they are effectively free.

THE DECISIVE PAIR (MEASURED) — q014 vs q038, both COUNT-shaped: q014 (FUSED) is 40,978 ms cold → 407 ms warm — a 100× cache speedup, because its cost is I/O (downloading 7,670 files) and the fused aggregate compute is trivial. q038 (UN-FUSED filtered COUNT) is 58,311 ms cold → 58,935 ms warm — NO cache speedup (warm ≈ cold), because its cost is not I/O (only 3 Parquet reads) but per-row materialization of ~427,500 rows to evaluate the filter and count. Caching cannot help a compute-bound query. This single pair localizes the entire performance story: the bottleneck is the execution shape, not the transport.

## 6. Fused-aggregate effective rows/s — the materialization-ceiling contradiction, SETTLED (MEASURED)

**ERRATUM (RT2, 2026-07-18): the "8-91×" multiples in this section divide whole-process throughput by the per-core ceiling — a basis mismatch (16-core host, decode-parallel fused path). Corrected per-core figures: q014 hot 1.69×, q027 hot 5.67×, marginal slope 11.7×, q014-vs-q038 same-shape warm pair 144.8× wall / 61× per-row. Native q014 comparator is 199ms/905k rows/s (not 293ms/614k); fused-beats-native stands (119ms < 199ms). Qualitative conclusions unchanged. See RT2-redteam-empirics.md; 00-MASTER-AUDIT.md carries the corrected numbers.**

Fact-table sizes at SF0.1 (native COUNT, MEASURED): FACT_ORDER 180,000; FACT_WEB_EVENT 1,000,000; FACT_GL_JOURNAL 250,000 rows. The open question was whether the fused-aggregate path escapes the ~56,000 rows/s row-at-a-time RDF-binding materialization ceiling. It does — by 8–27×:

```
query (shape)                                       rows processed  wall     effective rows/s
q014 FUSED COUNT+SUM by channel / FACT_ORDER  hot        180,000    119 ms      1,512,605   (27x ceiling)
q014  "                                       warm(proc) 180,000    407 ms        442,260   (7.9x)
q014  "                                       native     180,000    293 ms        614,334   (11x)
q027 FUSED grouped COUNT / FACT_WEB_EVENT     hot      1,000,000    197 ms      5,076,142   (91x)
q038 UN-FUSED filtered COUNT / DIM_CUSTOMER   warm      ~427,500  58,935 ms          7,254   (0.13x — BELOW ceiling)
q038  "                                       cold      ~427,500  58,311 ms          7,331   (0.13x)
```

VERDICT (MEASURED, settles the contradiction): the fused-aggregate path processes 180,000–1,000,000 source rows into a handful of grouped outputs at 442,000–5,076,000 rows/s — 8× to 91× ABOVE the ~56k ceiling. The un-fused filtered COUNT processes rows at ~7,300 rows/s — 7.7× BELOW it. The mechanism (MEASURED, not asserted): the ~56k ceiling is the rate of materializing rows AS RDF bindings (per-row term construction); a fused aggregate never builds per-row bindings — it streams scanned rows straight through the aggregate accumulator (`fused_aggregate.rs`), so its throughput is bounded by scan/decode, not by binding materialization. The cold/warm cache test (§5) is the proof: q014 (fused) is cache-sensitive (I/O-bound, warm 407 ms) while q038 (un-fused, same COUNT shape) is cache-insensitive (compute-bound, warm ≈ cold ≈ 58 s). Two COUNT-shaped queries over comparable row magnitudes differ by ~145× warm, entirely on whether the aggregate fuses into the scan. So: fused shapes DO escape the materialization ceiling; the ceiling is real and binds only the shapes that fall back to per-row materialization (un-fused/filtered aggregates, wide crawls, large row-returning SELECTs like q024 at ~30–44k output-rows/s near the ceiling).

## 7. What the numbers say (MEASURED + INFERRED)

Tip status vs the ≤3 s north-star (MEASURED): 57 of 65 ok queries land ≤3 s (88%); 45/45 deterministic queries are byte-identical native==virtual; 0 correctness mismatches. The virtual path at `10e073fe9` is, for the large majority of the corpus, a low-seconds-or-better graph over live Snowflake Iceberg — a genuine, large improvement over the bless-time state where 28 of these were DNFs (§1). Several members are FASTER than native (q008 550 ms vs 768 ms; q009, q010, q012, q027, q032, q052 all sub-native) because the fused join+aggregate beats a native materialize.

The remaining >3 s tail is 8 queries and it is a COVERAGE tail, not a fundamental one (INFERRED from the counters): q056/q057/q058/q059 (exploration wildcard scans over 16–25 tables — a whole-warehouse read by construction), q038 (un-fused filtered COUNT — the F-AUD-8 aggregate-admission gap), q016 (un-pruned OPTIONAL crawl — F-AUD-9 join admission), q031 (243-scan ref-chain), and q043 (drift-set network flap, not engine). Every tail member maps to a named coverage gap in A2/A3; none is a representation or wire-format problem. INFERRED: fusing the currently-declining shapes (filtered/DISTINCT/MIN-MAX COUNT, un-pruned crawl budgeting) would pull the tail below 3 s — the engine already proves it can stream a fact table at ~450k–5M rows/s when the aggregate fuses (§6); the tail is where it fails to fuse and falls back to ~7k rows/s per-row materialization.

The serverless cold-start tax is real and quantified (MEASURED §5): 2–4 s of catalog (loadTable+oauth) per cold Lambda open, plus the full Parquet download for whatever the query touches (41 s to pull 7,670 FACT_ORDER files cold). The on-disk artifact cache makes warm invocations cheap for I/O-bound shapes (q045/q046 → 45 ms) but does nothing for compute-bound ones (q038 → 59 s warm). INFERRED: a warm-kept execution environment + the persistent artifact cache is load-bearing for the browse/point-lookup/fused-rollup families; the un-fused-COUNT and wide-crawl families need engine coverage work, not more caching. Client-side memory (§0): a single virtual scan client held ~20.5 GB RSS across the warm-handle corpus — corroborating V2's memory-budget finding that per-query accounting (no `set_memory_limit(budget/N)`) is load-bearing under any concurrency.
