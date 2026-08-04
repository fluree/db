# Multi-fact fused-join generality (db #1589) — A/B yardstick

Substrate B (local MinIO + iceberg-rest), SF0.1, EC2 c7g.4xlarge (aarch64, 16c/30GB), quiet box (loadavg ~0.3). N=5 cold, fresh process per rep, `query_exec_serialize` boundary. duckdb v1.5.5 (sha256 `02163197…`). fluree-1589 = `683bc893b` (S1+S2+pins); fluree-before = `99e9656d9` (#1582 head, declines S1/S2 → generic path). Raw: `multifact_gen_sf01.jsonl` (+ `_warm`, `_floor`, `_cq027_baseline`).

## Correctness (gate — real DuckDB answers, PASS required before timing)
Both pairs, both fluree binaries, exact gate: **PASS**. s1 = 10 rows, s2 = 28105 rows.

## Cold timing (median of 5)

| pair | duckdb-parquet | duckdb-iceberg | fluree-1589 | fluree-before | 1589 RSS | no DNF |
|---|--:|--:|--:|--:|--:|:--:|
| s1_multifilter_tickets_by_category (K=2 semi-joins) | 11 ms | 64 ms | **832 ms** | 3000 ms | 177 MB | 5/5 |
| s2_tickets_by_customer_product (2 ref-IRI keys) | 32 ms | 49 ms | **978 ms** | 965 ms | 187 MB | 5/5 |

fluree-1589 / duckdb-iceberg (cold median): **s1 = 13.0×, s2 = 20.0×**.

## Acceptance bar (≤~5× DuckDB cold · no DNF · RSS <2 GB)
- **no DNF: MET** (5/5 both shapes, both binaries).
- **RSS <2 GB: MET** (177 / 187 MB — ~11× under budget).
- **≤5× DuckDB cold: NOT met at SF0.1** (13× / 20×). See mechanism below — this is the shared virtual-scan floor, not the fold.

## Mechanism (why the ratio, and why it isn't the fold)
- **Fixed overhead is negligible.** fluree-1589 cold floor (p1, manifest-count, no scan) = **22 ms**. So the ~800–980 ms is query execution, not CLI/catalog startup.
- **Compute-bound, not fetch-bound.** cold ≈ warm (s1 832→786 ms, s2 978→922 ms) ⇒ the cost is the R2RML virtual scan + per-row term/IRI materialization, not cold catalog I/O.
- **It's the shared scan floor, not multi-fact generality.** A *single-table* fused pair (cq027, 100k rows) is already **6.8×** DuckDB cold here. The multi-fact shapes add the expected extra dim-scan + membership/resolver-build cost on top of that shared floor (s1 scans fact + 3 dims; s2 builds 2 FK→IRI resolvers over 30k+3k dim rows). The fold itself is vectorized (N1, already in #1582); the residual is the Arrow→ColumnValue decode-seam + per-row IRI minting (SYNTHESIS N2), a separate no-regret unit — orthogonal to this PR's admission widening.

## What this unit delivered (the charter goal: stop DNF-ing one step beyond P3)
- Both shapes now **fuse and complete cold, un-materialized, sub-second-to-~1 s, RSS <200 MB, correct** — where the pre-#1589 path declined to the materialize twin.
- **S1 is 3.6× faster than the materialize twin** (832 ms vs 3000 ms) — a real fusion win.
- **S2 is at wall-clock parity with the twin at SF0.1** (978 vs 965 ms) but slightly leaner (187 vs 202 MB); its win is structural (bounded single-pass vs a ≥2-ref materialize that grows with scale), not an SF0.1 wall-clock delta.

## Note on scale
SF0.1 puts DuckDB at its floor (40–64 ms), below the charter's own ~200–400 ms DuckDB reference. Per the established trend (RESULTS.md: fluree/duckdb ratio *widens* with scale, per-row materialization linear vs DuckDB sub-linear), an SF1 run would not bring the ratio under 5× — the ≤5× target requires the N2 scan/decode work, not more scale. SF1 was therefore not run.
