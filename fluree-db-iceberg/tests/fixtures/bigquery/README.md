# BigQuery-written Iceberg table (`big`)

Copied read-only on 2026-10-27 from a BigQuery-managed Iceberg table's exported
metadata. The data is synthetic: 2,000,000 rows in 4 Parquet files, 24,526,336
bytes. The source bucket's name is replaced by `example-bucket` in every path
(`gs://example-bucket/iceberg/big/...`), in the metadata JSON and inside both
Avro files, whose blocks were re-encoded for it; the manifest list's
`manifest_length` is updated to the rewritten manifest's size. Nothing else is
changed.

- `big.metadata.json`: the table metadata. Its only snapshot's summary is
  `{"operation": "overwrite"}`, with no `total-records`, `total-data-files` or
  `total-files-size` (all optional in the Iceberg spec).
- `big.manifest-list.avro`: that snapshot's manifest list. It names its file
  counters `added_files_count` / `existing_files_count` / `deleted_files_count`,
  the spec spelling.
- `big.manifest.avro`: the one data manifest it lists.

The `manifest-list` path inside the metadata points at that placeholder bucket;
tests serve the bytes under that path from memory.
