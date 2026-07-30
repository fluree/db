# bench-duckdb-ab — DuckDB-vs-Fluree A/B benchmark program

A reproducible A/B that uses DuckDB as a single-node yardstick for Fluree's virtual (R2RML-over-Iceberg) query path over the SF0.1 `ENTERPRISE_DEMO` BI star schema. The thesis: Fluree should at least MATCH DuckDB per comparable query on the same physical Iceberg files; every DuckDB win beyond noise is a named engine gap with a query shape attached.

## Layout

- `PROTOCOL.md` — the binding rules (substrate split, memo-COLD/data-WARM fresh-process primary, RSS disclosed not caps-equal, full-result-consumption timing boundary, substrate field discipline, correctness-first, SQL↔SPARQL equivalence rules, DNF caps, machine discipline). Read this first.
- `RESULTS.md` — the wave tables (A: clean p1/p2; B: partitioned-copy probe; C: pair-set widening + predictions-vs-outcomes + named gaps; D: scale-up).
- `harness/` — `run_pair.py` (paired runner, binding-protocol compliant), `summarize_ab.py`, `targets.json` (SANITIZED template — replace `<PLACEHOLDER>` tokens), `pairs/` (the SQL↔SPARQL pair corpus + `manifest.json` with per-pair equivalence rules).
- `substrate/` — `docker-compose.yml` (MinIO + Iceberg REST fixture), `load_tables.py` (unpartitioned), `load_tables_partitioned.py` (Wave-B probe), `README.md` (the runbook).

## Engines

- DuckDB v1.5.5 (Iceberg predicate pushdown; pinned >= 1.5.3). Invoked `arch -arm64` on Apple Silicon.
- Fluree virtual path, two provenance-stamped binaries: shipped-main, and a build carrying the #1528 filter-over-join fusion. `engine_version` on every row records the binary's git commit so a fusion-lacking build is never mistaken for a fixed one.

## Quick start

```sh
cd substrate && docker compose up -d
SF_PARQUET_SRC=/path/to/output-sf01 python load_tables.py     # see substrate/README.md
cd ../harness
# fill targets.json <PLACEHOLDER>s first, then:
python run_pair.py --pairs p1_count_fact --engines duckdb,fluree \
  --duckdb-target duckdb-iceberg-minio-sf01 --fluree-target fluree-minio-sf01-main \
  --modes cold --runs 5 --out out/run.jsonl
python summarize_ab.py out/*.jsonl
```

Scope: append-only SF01 (+ larger locally-generated SF for scale). MoR/equality-delete tables are OUT (see `PROTOCOL.md` §10).
