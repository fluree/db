# datagen — the synthetic ENTERPRISE_DEMO generator (substrate source of truth)

This is the deterministic generator that produces the SF0.1 / SF1 Parquet the substrate loads
(`../load_tables.py` reads `<table>/data_0.parquet` from here; `../load_tables_partitioned.py`
reads the same for the Wave-B probe). It is committed so the whole Fluree-and-DuckDB A/B is
reproducible end to end, not just from `<placeholder>` paths.

DuckDB-driven, single `--scale-factor` knob (facts scale linearly, dims scale `sqrt(SF)`),
one `string.Template` per table in `queries.py`. Referential integrity is by construction
(FKs are random keys inside the parent surrogate range); SCD-2 history on `dim_customer` /
`dim_product`; big-fact dates are monotonic in the row index so Parquet parts are time-sorted
for Iceberg min/max pruning. `queries.py` uses fictional placeholder brand/company strings
(Acme, Globex, …) as synthetic column values — they are random data, not third-party
references. `duckdb` (the Python package, `requirements.txt`) is a user-installed build
dependency for generation only; nothing is auto-fetched or committed as a binary.

## Reproduce the substrates

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# SF0.1 substrate (Waves A–C). Single 128MB part per table => <table>/data_0.parquet,
# which is what ../load_tables.py expects. --seed is fixed by default (0.42).
.venv/bin/python generate.py --scale-factor 0.1 --out ./output-sf01 --threads 1

# SF1 substrate (Wave D, ~27.5M fact rows).
.venv/bin/python generate.py --scale-factor 1 --out ./output-sf1 --threads 1
```

Then load into substrate B (see `../README.md`):

```sh
SF_PARQUET_SRC=./output-sf01 python ../load_tables.py
```

Determinism: row counts per table are fixed by `--scale-factor` alone (see the manifest
below) and reproduce exactly. Cell VALUES are `setseed(0.42)`-deterministic when generated
single-threaded (`--threads 1`); the aggregate answers the A/B correctness gate checks are
stable regardless, because the gate compares DuckDB vs Fluree over the *same* generated files.
Byte-identical Parquet is not guaranteed across DuckDB versions / thread counts, so the
manifest below pins the exact SF0.1 files used for the committed `../../results/` numbers by
sha256 — regenerate and compare row counts to verify equivalence, or match sha256 to confirm
the identical substrate.

## SF0.1 manifest (the files behind the committed results)

`output-sf01/<table>/data_0.parquet`, DuckDB v1.5.x generator, `--scale-factor 0.1 --seed 0.42`:

| table | rows | sha256(data_0.parquet) |
|---|---|---|
| dim_date | 7670 | `4eea00dcc65d26db3efda8ab601b92fbaa54402a3ab1fa4ae25763073b9d64d9` |
| dim_geography | 25000 | `4de01fb1e5edd4aa7ef0f231bf4eb8795f421b2a5c37740fe61835174f89427b` |
| dim_supplier | 2000 | `44bf0fdb064dbc1ced455006f3992fdefdd288070c772073dc1d763fb1481991` |
| dim_account | 15000 | `c303b3825a925e32cbc7f3db8aaf3aab56ed429dd35f3101b75b02cef4cf67dc` |
| dim_store | 500 | `a92e22349813d257bc50f2a165f06a3f5dfdb76616003cf62cdee20d6e319602` |
| dim_employee | 5000 | `6f9ba00213524e009d3aa8f0153e908633c4eeef3c2439992724b4e832186eed` |
| dim_customer | 390000 | `15a72544c743b5771386c4f9ed5fce358501d8fa5c0f57693210ee44d432cf91` |
| dim_product | 37500 | `bc506bff3d92ef39cf556f5bc4fa8f3df1dbb29dbf7bb8bd0f006b69d39583b7` |
| fact_order | 180000 | `d573102a953cdfa68e36d2c00af3323e2150e0a97fc00e08db691129d286f092` |
| fact_order_line | 600000 | `f8ffbfeb510976f8c2250c1661334d05e8e832bf6950c2798dcdee784cbf121c` |
| fact_inventory_snapshot | 300000 | `a513e0c1350a4a885ddcbcdf80eb6b3baf3a3488cf2c6e0ae69659169f18d7b5` |
| fact_shipment | 180000 | `607d145dd79ddb8123b4e900f1f2ab85226c3b08c96906926d56fd46a09ca671` |
| fact_payment | 200000 | `506afc108db8d86096c3c330ee0c9bfcfee6d1488989d1bcd59a8187cdb4955e` |
| fact_gl_journal | 250000 | `b66bbfe90b60f37ca5b480182b5c370f3c623668356616e3b6bbb0c6c398cd75` |
| fact_web_event | 1000000 | `3f629e19e1af3260cac0b695c4fbc6f218123455c8af970a47daddb261344ee8` |
| fact_support_ticket | 40000 | `c99d930c516a7d86a39b9d640f0fa2c98c9cb1bd6c0ec7d748376b7781a21750` |

Total 3,232,670 rows. These row counts match the magnitudes cited in `../../RESULTS.md`
(e.g. `fact_order` 180000 = p1 `COUNT`; `dim_customer` 390000 = the cq038 filtered-COUNT
scan; `fact_web_event` 1000000 = the Wave-B/C 1M-row scans). Verify with:

```sh
for t in output-sf01/*/; do
  python3 -c "import sys,hashlib,pyarrow.parquet as pq;f=sys.argv[1]+'data_0.parquet';\
print(sys.argv[1], pq.read_metadata(f).num_rows, hashlib.sha256(open(f,'rb').read()).hexdigest())" "$t"
done
```

The `--external-volume` name and Snowflake `CREATE ICEBERG TABLE` DDL emitted by `ddl.py`
are only for the deployment-real substrate-A path; substrate B (the headline numbers) needs
only the Parquet output.
