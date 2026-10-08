# glue-local: Iceberg tables catalogued in AWS Glue, without AWS

A local, credential-free, Docker-free harness for the `glue` catalog mode. **moto** mocks S3 and
the AWS Glue Data Catalog in one Python process. **pyiceberg's `GlueCatalog`** writes real Iceberg
tables through it, laid out the way Glue-integrated writers produce them:

- the current metadata file is recorded only in the Glue table's `metadata_location` parameter;
- there is **no `metadata/version-hint.text`**.

It does two jobs:

- `check.sh` grades a `fluree` binary end to end through the CLI (`fluree iceberg map` + `fluree query`).
- `write_fixture.sh` regenerates the committed fixture behind the CI test
  `fluree-db-api/tests/it_iceberg_glue_moto.rs`, which replays it into a moto server and runs the
  same checks through the Rust API (see [CI](#ci)).

## Setup (once)

```bash
H=scripts/glue-local
RUN_DIR="${TMPDIR:-/tmp}/fluree-glue-local"     # the default; holds the venv, moto's log, run dirs
uv venv "$RUN_DIR/.venv" --python 3.12
uv pip install --python "$RUN_DIR/.venv/bin/python" -r "$H/requirements.txt"
```

Export `RUN_DIR` if you use another location.

## Run it

```bash
$H/up.sh                                        # start moto from empty and seed the tables
FLUREE_BIN=/path/to/fluree $H/selftest.sh       # harness self-check: expect 4/4 pass
FLUREE_BIN=/path/to/fluree $H/check.sh target   # expect 0 failures
$H/down.sh
```

`check.sh` exits with the number of failures. Each run uses a fresh Fluree project and a fresh
`TMPDIR`, because Fluree's Iceberg disk caches outlive the process and would otherwise let a run
against a reseeded moto answer from an earlier run's cache.

The `baseline` profile (`check.sh baseline`) grades a release without a Glue mode: it expects the gap
to reproduce (no `--mode glue`, and direct mode failing on the missing `version-hint.text`).

## Files

| File | What |
|---|---|
| `env.sh` | Shared settings: port, dummy credentials, `AWS_ENDPOINT_URL_GLUE` → moto. Isolates the run from `~/.aws`. Sets **no** S3 endpoint override, so Fluree must reach moto's S3 through `--s3-endpoint` |
| `up.sh` / `down.sh` | Start moto from empty and seed / stop it (all state is in moto's memory) |
| `seed.py` | Writes the tables below through pyiceberg's `GlueCatalog`; writes `manifest.json` (expected counts, locations); `--export-fixture DIR` also exports the CI fixture |
| `mappings/*.ttl` | R2RML mappings, one per check (typed `rr:TriplesMap`, explicit `rr:termType rr:IRI` on template object maps) |
| `check.sh` | The check matrix, graded against the `baseline` or `target` profile |
| `selftest.sh` | Proves the mappings and the target counts with any binary, independently of the Glue mode: it writes `version-hint.text` for every table (pointing at the file Glue marks current) and reads them in direct mode |
| `write_fixture.sh` | Regenerates `fluree-db-api/tests/fixtures/iceberg/glue/` |
| `requirements.txt` | Pinned Python dependencies |

## Seeded tables (Glue database `demo`, bucket `lake`)

| Table | Rows | Shape | Tests |
|---|---|---|---|
| `customers` | 50 | 2 metadata files, no hint | the case real Glue produces |
| `orders` | 200 | 3 metadata files (2 appends), no hint | the current metadata wins; join partner |
| `customers_hinted` | 50 | + `version-hint.text` | direct-mode control: isolates "missing hint" as the only variable |
| `orders_orphan` | 100 | + an **uncommitted** `00099-*.metadata.json` planted in `metadata/` | the Glue pointer is authoritative; "highest-numbered file" would return 0 |
| `hive_table` | — | a Glue table that is not Iceberg (no `metadata_location`) | must fail clearly |

## Check matrix

| ID | Check | baseline (no Glue mode) | target |
|---|---|---|---|
| C1 | direct + hint (control) | 50 | 50 |
| C2 | direct, Glue-written, no hint | error: version-hint | info only |
| C3 | glue: one table | no glue mode | **50** |
| C4 | glue: 3 metadata files | no glue mode | **200** |
| C5 | glue: two-table R2RML join | no glue mode | **200** |
| C6 | glue: orphan metadata file present | no glue mode | **100** (the catalog pointer, not the orphan) |
| C7 | glue: table not in Glue | no glue mode | clear error (not found) |
| C8 | glue: non-Iceberg Glue table | no glue mode | clear error (not an Iceberg table) |
| C9 | direct, no hint, orphan present | error: version-hint | **never a silent 0** |

Never change an expected count to make a run pass: `selftest.sh` proves them without the Glue mode.

## CI

`it_iceberg_glue_moto` replays `fluree-db-api/tests/fixtures/iceberg/glue/` (this harness's tables,
in bucket `fluree-glue-it` and Glue database `glue_it`, exported by `write_fixture.sh`) into a moto
server through the AWS SDK, so CI needs no Python. It runs C3–C8 plus catalog browse and table
preview through the Rust API. The CI `test` job provides moto as a service container and sets
`FLUREE_GLUE_LOCAL_ENDPOINT`; the test skips when that is unset, and `glue_moto_is_configured_in_ci`
fails if the job's `FLUREE_GLUE_MOTO` marker is set without it. To run it locally against this
harness's moto:

```bash
$H/up.sh
FLUREE_GLUE_LOCAL_ENDPOINT=http://127.0.0.1:5055 \
  cargo test -p fluree-db-api --features glue-moto --test it_iceberg_glue_moto
```

After `write_fixture.sh`, run the test before committing the new fixture.
