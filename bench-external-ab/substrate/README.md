# Substrate B — local engine-pure Iceberg fixture

A locally-controlled, catalog-fronted Iceberg substrate both engines read, so the data-scan numbers are engine-pure (no Snowflake network variance, no vended-credential path, no many-small-files fan-out). This is where the headline A/B numbers come from. See `../PROTOCOL.md` §1.

## Stack

`docker-compose.yml` brings up:
- `minio` — S3 storage at `http://localhost:9000` (console `:9001`), creds `minioadmin/minioadmin` (the documented local MinIO default — not a real secret).
- `mc-init` — creates the `warehouse` bucket, then exits.
- `rest` — `apache/iceberg-rest-fixture` REST catalog at `http://localhost:8181`, S3-backed by the MinIO above.

A REST catalog (not per-table direct mode) is required because the R2RML mapping resolves 16 tables by `rr:tableName` (`DW_SF01.<TABLE>`), which needs catalog namespace resolution.

## Bring it up

```sh
cd substrate
docker compose up -d
docker compose ps            # wait for minio healthy + rest up
```

## Load the tables

Needs a python with `pyiceberg[s3fs]` + `pyarrow` (the Wave-B `transform` spec also wants `pyiceberg_core`). `SF_PARQUET_SRC` points at a directory of `<table>/data_0.parquet` — the SF0.1 generator output (16 tables). Reproduce it with the committed, deterministic generator in `datagen/` (see `datagen/README.md` for the row-count + sha256 manifest):

```sh
# Reproduce the SF0.1 substrate parquet:
cd datagen && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python generate.py --scale-factor 0.1 --out ./output-sf01 --threads 1
cd ..

# Unpartitioned wave-1 layout (one data file per table), namespace DW_SF01:
SF_PARQUET_SRC=./datagen/output-sf01 python load_tables.py

# Wave-B probe, identity-bucket layout (namespace DW_SF01_PART; no pyiceberg_core needed):
SF_PARQUET_SRC=./datagen/output-sf01 PART_TABLE=fact_web_event PART_COL=EVENT_DATE \
  PART_GRAIN=month python load_tables_partitioned.py

# Wave-B probe, GENUINE month-transform layout (namespace DW_SF01_PART_T; spec month(EVENT_DATE)):
SF_PARQUET_SRC=./datagen/output-sf01 PART_TABLE=fact_web_event PART_COL=EVENT_DATE \
  PART_GRAIN=month PART_SPEC=transform python load_tables_partitioned.py
```

`PART_GRAIN` (year|month|day) is the time grain; `PART_SPEC` selects an identity partition on a derived integer bucket (`derived`, default) or a genuine iceberg transform on the raw date column (`transform`).

Tables are written with UPPERCASE identifiers (Snowflake folds identifiers uppercase; the shared R2RML mapping references `ORDER_KEY` etc.), so the SAME uppercase mapping works on substrate A and B. DuckDB is case-insensitive, so its SQL is unaffected.

## DuckDB side

Target `duckdb-iceberg-minio-sf01` in `../harness/targets.json`. It attaches the local REST catalog with `AUTHORIZATION_TYPE none` + `ACCESS_DELEGATION_MODE none` and a BYO S3 secret (our MinIO creds), which bypasses the vended-cred path entirely.

## Fluree side

The R2RML mapping both engines' Fluree leg reads is committed here as `enterprise-sf01-mapping.ttl` (the substrate contract: 16 TriplesMaps, `rr:tableName "DW_SF01.<TABLE>"`, surrogate-`*_KEY` subjects under `http://data.fluree.dev/edw/...`, the same UPPERCASE mapping working on substrate A and B). Register the virtual graph source ONCE against the local catalog, then point a vbench/CLI target at it:

```sh
fluree iceberg map enterprise-sf01-b --mode rest \
  --catalog-uri http://127.0.0.1:8181 \
  --r2rml ./enterprise-sf01-mapping.ttl \
  --s3-endpoint http://127.0.0.1:9000 --s3-path-style --no-vended-credentials
# AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin in env.
```

GOTCHAS (all solved): use `127.0.0.1`, NOT `localhost` (localhost resolves to IPv6, the fixture listens on IPv4); OMIT `--warehouse` (passing `s3://warehouse/` gets injected as a URL path prefix — leave the REST prefix empty); Fluree reads column names from the Iceberg schema (uppercase), so the same uppercase mapping works on both substrates.

## Run a pair

Correctness FIRST (PROTOCOL §6): a pair's timings only count after its results MATCH across engines. Run the gate before timing and require a PASS:

```sh
cd ../harness
python check_pair.py \
  --pairs p1_count_fact,p2_category_rollup,p3_open_tickets_by_segment \
  --duckdb-target duckdb-iceberg-minio-sf01 --fluree-target fluree-minio-sf01-main
# only on all-PASS, time them:
python run_pair.py \
  --pairs p1_count_fact,p2_category_rollup,p3_open_tickets_by_segment \
  --engines duckdb,fluree \
  --duckdb-target duckdb-iceberg-minio-sf01 \
  --fluree-target fluree-minio-sf01-main \
  --modes cold --runs 5 --out out/run.jsonl
```

`config.toml` under `fluree-home/.fluree/` is intentionally NOT committed (it is a local, generated home). Point the target's `config` at your own registered fluree home.
