# bench-external-ab — external-engine A/B benchmark program

> **Notice**: This benchmark harness invokes an independently installed DuckDB CLI binary as an external process, solely for comparative performance measurement. No DuckDB source code, binaries, extensions, or platform components are included in, linked into, distributed with, or incorporated into this repository or any Fluree product. DuckDB® is a trademark of its owner; references here are nominative — identifying the third-party product being measured — and imply no affiliation or endorsement. Versions, configuration, and the full protocol are disclosed for reproducibility.

A reproducible A/B benchmark that characterizes Fluree's virtual (R2RML-over-Iceberg) query path against external single-node engines over the SF0.1 `ENTERPRISE_DEMO` BI star schema. The harness is engine-agnostic; external engines are adapters in `harness/engines/`. The first external engine measured is DuckDB, used as a single-node yardstick. Reference point: Fluree should be at least on par with the external engine per comparable query on the same physical Iceberg files; each measured shortfall beyond noise is reported as a named engine gap with the query shape attached, and where Fluree lags it is stated with equal plainness.

## Layout

- `PROTOCOL.md` — the binding rules (substrate split, memo-COLD/data-WARM fresh-process primary, RSS disclosed not caps-equal, full-result-consumption timing boundary, substrate field discipline, correctness-first, SQL↔SPARQL equivalence rules, DNF caps, machine discipline). Read this first.
- `RESULTS.md` — the wave tables (A: clean p1/p2; B: partitioned-copy probe; C: pair-set widening + predictions-vs-outcomes + named gaps; D: scale-up).
- `harness/` — `run_pair.py` (engine-agnostic paired runner), `check_pair.py` (correctness cross-check), `summarize_ab.py`, `engines/` (external-engine adapters: `fluree.py`, `duckdb.py`, `common.py`), `targets.json` (SANITIZED template — replace `<PLACEHOLDER>` tokens), `pairs/` (the SQL↔SPARQL pair corpus + `manifest.json` with per-pair equivalence rules).
- `substrate/` — `docker-compose.yml` (MinIO + Iceberg REST fixture), `load_tables.py` (unpartitioned), `load_tables_partitioned.py` (Wave-B probe), `README.md` (the runbook).

## Engines

- Fluree virtual path (the engine under characterization), two provenance-stamped binaries: shipped-main, and a build carrying the #1528 filter-over-join fusion. `engine_version` on every row records the binary's git commit so a fusion-lacking build is never mistaken for a fixed one.
- DuckDB v1.5.5 (the first external yardstick engine; Iceberg predicate pushdown, pinned >= 1.5.3). Installed independently by the user (Homebrew or the official binary — a documented user step, never auto-fetched or committed here) and invoked as an external CLI, `arch -arm64` on Apple Silicon. See the NOTICE above.

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
