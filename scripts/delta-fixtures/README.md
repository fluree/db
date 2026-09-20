# Delta test-fixture generators

The tables under `fluree-db-delta/tests/fixtures` are committed copies of what
these two scripts write. Each builds its tables with a reference writer, checks
that writer's own reads against independently constructed expected rows, and
records them in a `manifest.json`. The reader's tests restate the expected rows
from the scripts' inputs; they never read them back through the reader.

Run from the repository root. Both refuse to overwrite an existing directory.
Requirements: `uv` and Python 3.12 (uv can provide it); the Spark script also
needs Java 17 or 21, and resolves the pinned Delta JVM package from Maven on
first use.

```sh
uv run --locked --python 3.12 scripts/delta-fixtures/fixture.py /tmp/delta-basic-001
uv run --locked --python 3.12 scripts/delta-fixtures/spark_fixture.py /tmp/delta-spark-001
```

- `fixture.py` (delta-rs): `dim_store`; `flags` (a nullable boolean); and
  `fact_order` — five versions covering initial rows, an append, an update, a
  delete and an added nullable column, with nullable amounts, a null partition
  value, and a checkpoint on the latest version.
- `spark_fixture.py` (Delta Spark 4.0): `deletion_vectors` (two files of five
  row groups, nine rows deleted without rewriting either), `column_mapping`
  (rename, drop, re-add over one Parquet file), `in_commit_time`,
  `history_loss` (a version whose data file is gone), `checkpointed` and
  `log_cleaned` (the same 13-commit table with and without the commits below
  its checkpoint), `types` (every scalar type plus a row of nulls) and
  `partitioned`. Small Parquet pages and row groups are deliberate: they are
  what lets the pruning tests see inside a file.

Data and expected rows are repeatable; commit timestamps, table ids and file
names differ between runs, so regenerating a fixture changes its file names.
When copying a run into `tests/fixtures`, leave out `manifest.json` and
`spark-warehouse`.
