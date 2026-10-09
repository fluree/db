# results — committed raw JSONL behind every RESULTS.md number

Every headline number in `../RESULTS.md` is a median (or DNF verdict) computed from the raw per-run rows in this directory, so any cell can be spot-checked from the repo. These are the actual harness outputs (`run_pair.py` emit rows, `PROTOCOL.md` schema: one JSON object per timed run — `pair_id`, `engine`, `substrate`, `engine_version` with the binary's git commit, `mode`, `run_idx`, `wall_ms`, `rows`, `ok`, `peak_rss_bytes`, `extra`). Scratch/regenerated runs still land in the gitignored `harness/out/`; only the files that back a published table are copied here and tracked.

Sanitization: these rows carry only engine/substrate/timing fields — no hostnames, account identifiers, secrets, or absolute paths. `engine_version` is a version string plus a short git commit; `target`/`substrate` are logical labels.

## Which file backs which RESULTS.md section

| file | rows | backs (RESULTS.md section) | engines / modes |
|---|---|---|---|
| `waveA.jsonl` | 50 | Wave A — clean p1/p2 (2026-07-30), the rows that replace the load-contaminated baseline | duckdb + fluree (main & #1528), cold + warm |
| `substrate_b_cold.jsonl` | 30 | Wave A — baseline four-way TRUE-COLD table (2026-07-29), incl. the standing p3 cold numbers | duckdb + fluree, cold |
| `substrate_b_warm.jsonl` | 15 | Wave A — baseline four-way WARM table, incl. the standing p3 warm numbers | fluree, warm |
| `main_ab.jsonl` | 30 | Wave A — baseline four-way, the shipped-main Fluree leg (p1/p2/p3) | fluree-main, cold + warm |
| `1528_ab.jsonl` | 30 | Wave A — baseline four-way, the #1528-fusion Fluree leg (p1/p2/p3) | fluree-#1528, cold + warm |
| `waveC_ec2.jsonl` | 250 | Wave C — the full 10-pair table on the uncontested `c7g.4xlarge` (authoritative) | duckdb + fluree (main & #1528), cold + warm |
| `waveC_main.jsonl` | 94 | Wave C — SUPERSEDED contended-local (macOS) provenance, shipped-main leg | fluree-main, cold + warm |
| `waveC_1528.jsonl` | 49 | Wave C — SUPERSEDED contended-local (macOS) provenance, #1528 leg | fluree-#1528, cold |
| `waveD_ec2.jsonl` | 153 | Wave D — scale-up SF=1 (~27.5M fact rows) table | duckdb + fluree (main & #1528), cold + warm |

Wave B (`../RESULTS.md` "partitioned-copy probe") is a set of one-off DuckDB probe walls posted to issue #1568, not a `run_pair.py` sweep, so it has no median-backed JSONL here; the probe table in RESULTS.md stands as the record. See `../substrate/load_tables_partitioned.py` for the loader that produces both partitioned layouts it discusses.

## Re-deriving a cell

`summarize_ab.py` reads any of these and prints per-pair medians (steady-state reps 2..N, rep1 shown separately):

```sh
cd ../harness
python summarize_ab.py ../results/waveC_ec2.jsonl        # Wave C table
python summarize_ab.py ../results/waveD_ec2.jsonl        # Wave D table
python summarize_ab.py ../results/waveA.jsonl ../results/substrate_b_cold.jsonl ../results/substrate_b_warm.jsonl
```
