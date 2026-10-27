# BigQuery-written Iceberg table (`big`)

Copied read-only on 2026-10-27 from a BigQuery-managed Iceberg table's exported
metadata (`gs://fluree-gcs-iceberg-1027-4b73/iceberg/big/metadata/`). The data
is synthetic: 2,000,000 rows in 4 Parquet files, 24,526,336 bytes.

- `big.metadata.json`: the table metadata. Its only snapshot's summary is
  `{"operation": "overwrite"}`, with no `total-records`, `total-data-files` or
  `total-files-size` (all optional in the Iceberg spec).
- `big.manifest-list.avro`: that snapshot's manifest list. It names its file
  counters `added_files_count` / `existing_files_count` / `deleted_files_count`,
  the spec spelling.
- `big.manifest.avro`: the one data manifest it lists.

The `manifest-list` path inside the metadata still points at the bucket; tests
serve the bytes under that path from memory.
